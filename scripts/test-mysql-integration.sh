#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

password="${BINFLUX_MYSQL_PASSWORD:-binflux-test-only}"
compose=(docker compose -f docker-compose.yml)
pid=""
cleanup() {
  if [[ -n "$pid" ]]; then kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true; fi
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  rm -f data/checkpoint.json
}
trap cleanup EXIT

"${compose[@]}" up -d --build --wait
cargo build --quiet
export BINFLUX_MYSQL_PASSWORD="$password"
mkdir -p data

mysql_exec() {
  "${compose[@]}" exec -T mysql sh -c 'MYSQL_PWD="$MYSQL_PASSWORD" mysql -h127.0.0.1 -ubinflux shop -N -B' <<SQL
$1
SQL
}

mysql_exec 'CREATE TABLE cdc_smoke (id INT PRIMARY KEY, note VARCHAR(40) NOT NULL) ENGINE=InnoDB;'
rm -f data/checkpoint.json /tmp/binflux-first.jsonl /tmp/binflux-second.jsonl
target/debug/binflux run --config binflux.example.yaml >/tmp/binflux-first.jsonl 2>/tmp/binflux-first.log &
pid=$!
for _ in $(seq 1 60); do [[ -s data/checkpoint.json ]] && break; sleep 1; done
[[ -s data/checkpoint.json ]] || { echo "Binflux did not establish a starting checkpoint" >&2; exit 1; }

mysql_exec 'START TRANSACTION; INSERT INTO cdc_smoke VALUES (1, "before"); UPDATE cdc_smoke SET note="after" WHERE id=1; DELETE FROM cdc_smoke WHERE id=1; COMMIT;'
for _ in $(seq 1 60); do [[ "$(wc -l </tmp/binflux-first.jsonl)" -ge 3 ]] && break; sleep 1; done
[[ "$(wc -l </tmp/binflux-first.jsonl)" -eq 3 ]] || { echo "Expected three committed row events" >&2; cat /tmp/binflux-first.log >&2; exit 1; }
grep -q '"operation":"INSERT"' /tmp/binflux-first.jsonl
grep -q '"operation":"UPDATE"' /tmp/binflux-first.jsonl
grep -q '"operation":"DELETE"' /tmp/binflux-first.jsonl

kill "$pid"
wait "$pid" 2>/dev/null || true
pid=""
mysql_exec 'INSERT INTO cdc_smoke VALUES (2, "restart-replay");'
target/debug/binflux run --config binflux.example.yaml >/tmp/binflux-second.jsonl 2>/tmp/binflux-second.log &
pid=$!
for _ in $(seq 1 60); do [[ -s /tmp/binflux-second.jsonl ]] && break; sleep 1; done
[[ "$(wc -l </tmp/binflux-second.jsonl)" -eq 1 ]] || { echo "Restart did not deliver exactly the post-checkpoint event" >&2; cat /tmp/binflux-second.log >&2; exit 1; }
grep -q 'restart-replay' /tmp/binflux-second.jsonl
echo "MySQL CDC, transaction delivery, and file/position restart smoke test passed"
