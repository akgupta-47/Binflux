#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

password="${BINFLUX_MYSQL_PASSWORD:-binflux-test-only}"
project="binflux-reliability-$$"
port="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
metrics_port="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
export BINFLUX_MYSQL_PASSWORD="$password" BINFLUX_MYSQL_PORT="$port"
compose=(docker compose -p "$project" -f docker-compose.yml)
tmpdir="$(mktemp -d /tmp/binflux-reliability.XXXXXX)"
config="$tmpdir/binflux.yaml"
checkpoint="$tmpdir/checkpoint.json"
binflux_pid=""
sql_pid=""
head_pid=""
success=0

cleanup() {
  if [[ -n "$binflux_pid" ]]; then
    kill -INT "$binflux_pid" 2>/dev/null || true
    wait "$binflux_pid" 2>/dev/null || true
  fi
  if [[ -n "$sql_pid" ]]; then
    kill "$sql_pid" 2>/dev/null || true
    wait "$sql_pid" 2>/dev/null || true
  fi
  if [[ -n "$head_pid" ]]; then
    kill "$head_pid" 2>/dev/null || true
    wait "$head_pid" 2>/dev/null || true
  fi
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
  if [[ "$success" == 1 ]]; then
    rm -rf "$tmpdir"
  else
    echo "Reliability test artifacts preserved at $tmpdir" >&2
  fi
}
trap cleanup EXIT

sed \
  -e "s/^  port: 3306$/  port: $port/" \
  -e 's/^  gtid_restart: false$/  gtid_restart: true/' \
  -e "s#listen_address: 127.0.0.1:9090#listen_address: 127.0.0.1:$metrics_port#" \
  -e "s#tls_ca_cert: ./tests/certs/ca.pem#tls_ca_cert: $PWD/tests/certs/ca.pem#" \
  -e "s#path: ./data/checkpoint.json#path: $checkpoint#" \
  binflux.example.yaml > "$config"

"${compose[@]}" up -d --build --wait
cargo build --quiet --bin binflux

mysql_exec() {
  "${compose[@]}" exec -T mysql sh -c \
    'MYSQL_PWD="$MYSQL_PASSWORD" mysql -h127.0.0.1 -ubinflux shop -N -B' <<<"$1"
}
mysql_root() {
  "${compose[@]}" exec -T mysql sh -c \
    'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -h127.0.0.1 -uroot -N -B' <<<"$1"
}
wait_for_mysql() {
  for _ in $(seq 1 60); do
    if mysql_root 'SELECT 1;' >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "MySQL did not become ready" >&2
  return 1
}
line_count() { wc -l < "$1" | tr -d ' '; }
wait_for_lines() {
  local file="$1" expected="$2" actual
  for _ in $(seq 1 90); do
    actual="$(line_count "$file")"
    if (( actual >= expected )); then return 0; fi
    if [[ -n "$binflux_pid" ]] && ! kill -0 "$binflux_pid" 2>/dev/null; then
      echo "Binflux exited before writing $expected events" >&2
      cat "${file%.jsonl}.log" >&2 2>/dev/null || true
      return 1
    fi
    sleep 1
  done
  echo "Timed out waiting for $expected events in $file (got $actual)" >&2
  return 1
}
start_binflux() {
  local output="$1" log="${1%.jsonl}.log"
  : > "$output"; : > "$log"
  RUST_LOG='info,mysql_binlog_connector_rust=error' target/debug/binflux run --config "$config" --sink stdout >"$output" 2>"$log" &
  binflux_pid=$!
  for _ in $(seq 1 60); do
    if [[ -s "$checkpoint" ]] && grep -q 'connected to MySQL binlog stream' "$log"; then return 0; fi
    if ! kill -0 "$binflux_pid" 2>/dev/null; then cat "$log" >&2; return 1; fi
    sleep 1
  done
  echo "Binflux did not connect" >&2; cat "$log" >&2; return 1
}
stop_binflux() {
  kill -INT "$binflux_pid"
  if ! wait "$binflux_pid"; then echo "Binflux did not stop cleanly" >&2; return 1; fi
  binflux_pid=""
}
crash_binflux() {
  kill -KILL "$binflux_pid"
  wait "$binflux_pid" 2>/dev/null || true
  binflux_pid=""
}
assert_checkpoint_position_unchanged() {
  local expected="$1"
  python3 - "$checkpoint" "$expected" <<'PY'
import json, sys
path, expected_text = sys.argv[1:]
expected = json.loads(expected_text)
actual = json.load(open(path))
if (actual["binlog_file"], actual["binlog_position"]) != (expected["binlog_file"], expected["binlog_position"]):
    print("Checkpoint moved across a transaction that was not fully delivered", file=sys.stderr)
    print("before:", expected)
    print("after: ", actual)
    sys.exit(1)
PY
}

mysql_exec '
CREATE TABLE cdc_reliability (id INT PRIMARY KEY, note VARCHAR(255) NOT NULL) ENGINE=InnoDB;
CREATE TABLE cdc_types (
  id INT PRIMARY KEY,
  nullable_text VARCHAR(40) NULL,
  amount DECIMAL(10,2) NOT NULL,
  payload_json JSON NOT NULL,
  data_blob BLOB NOT NULL
) ENGINE=InnoDB;
'

start_binflux "$tmpdir/smoke.jsonl"
baseline="$(cat "$checkpoint")"
python3 - "$checkpoint" "$metrics_port" <<'PY'
import json, sys, time, urllib.request
checkpoint, port = sys.argv[1:]
assert json.load(open(checkpoint)).get("gtid_set") is not None
for _ in range(30):
    try:
        body = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=1).read().decode()
        assert "binflux_transactions_delivered_total" in body
        break
    except Exception:
        time.sleep(0.2)
else:
    raise AssertionError("Prometheus metrics endpoint did not become ready")
PY

# No binlog event or checkpoint movement is allowed before COMMIT.
mysql_exec 'START TRANSACTION; INSERT INTO cdc_reliability VALUES (1, "uncommitted"); DO SLEEP(4); COMMIT;' &
sql_pid=$!
sleep 2
[[ "$(line_count "$tmpdir/smoke.jsonl")" == 0 ]]
assert_checkpoint_position_unchanged "$baseline"
wait "$sql_pid"; sql_pid=""
wait_for_lines "$tmpdir/smoke.jsonl" 1
[[ "$(line_count "$tmpdir/smoke.jsonl")" == 1 ]]

# Multiple row operations in one committed transaction retain source order.
mysql_exec 'START TRANSACTION; UPDATE cdc_reliability SET note="updated" WHERE id=1; INSERT INTO cdc_reliability VALUES (2, "second"); DELETE FROM cdc_reliability WHERE id=1; COMMIT;'
wait_for_lines "$tmpdir/smoke.jsonl" 4
python3 - "$tmpdir/smoke.jsonl" <<'PY'
import json, sys
events = [json.loads(line) for line in open(sys.argv[1])]
assert [e["operation"] for e in events] == ["INSERT", "UPDATE", "INSERT", "DELETE"]
assert len({e["transaction_id"] for e in events[1:]}) == 1
PY
after_multi="$(cat "$checkpoint")"
[[ "$after_multi" != "$baseline" ]]

# Real ROW events cover NULL, DECIMAL, JSON, and binary/blob values.
mysql_exec "INSERT INTO cdc_types VALUES (1, NULL, 12345.67, JSON_OBJECT('ok', TRUE, 'n', 7), X'00FF41');"
wait_for_lines "$tmpdir/smoke.jsonl" 5
python3 - "$tmpdir/smoke.jsonl" <<'PY'
import base64, json, sys
events = [json.loads(line) for line in open(sys.argv[1])]
row = next(e["after"] for e in events if e["source"]["table"] == "cdc_types")
assert row["nullable_text"] is None, row
assert str(row["amount"]) == "12345.67", row
assert base64.b64decode(row["data_blob"]["$base64"]) == bytes((0, 255, 65)), row
assert base64.b64decode(row["payload_json"]["$base64"]), row
PY

# Multiple row images in one transaction share a GTID and preserve ordering.
mysql_exec 'START TRANSACTION; INSERT INTO cdc_reliability VALUES (3, "multi-a"), (4, "multi-b"), (5, "multi-c"); COMMIT;'
wait_for_lines "$tmpdir/smoke.jsonl" 8
python3 - "$tmpdir/smoke.jsonl" <<'PY'
import json, sys
events = [json.loads(line) for line in open(sys.argv[1])][5:8]
assert all(e["operation"] == "INSERT" for e in events)
assert len({e["transaction_id"] for e in events}) == 1
assert [e["after"]["id"] for e in events] == [3, 4, 5]
PY

# A large committed transaction exercises bounded buffering and batch delivery.
python3 - "$tmpdir/large.sql" <<'PY'
import sys
values = ",".join(f"({i}, 'large-{i}')" for i in range(1000, 6000))
open(sys.argv[1], "w").write("START TRANSACTION;\nINSERT INTO cdc_reliability (id, note) VALUES " + values + ";\nCOMMIT;\n")
PY
"${compose[@]}" exec -T mysql sh -c \
  'MYSQL_PWD="$MYSQL_PASSWORD" mysql -h127.0.0.1 -ubinflux shop -N -B' < "$tmpdir/large.sql"
wait_for_lines "$tmpdir/smoke.jsonl" 5008
python3 - "$tmpdir/smoke.jsonl" <<'PY'
import json, sys
events = [json.loads(line) for line in open(sys.argv[1])]
large = [e for e in events if e["source"]["table"] == "cdc_reliability" and e["after"] is not None and 1000 <= e["after"]["id"] < 6000]
assert len(large) == 5000, len(large)
assert len({e["transaction_id"] for e in large}) == 1
assert [e["after"]["id"] for e in large] == list(range(1000, 6000))
PY

# Rotation must switch the durable filename and continue delivery.
old_binlog="$(mysql_root 'SHOW BINARY LOG STATUS;' | awk '{print $1}')"
mysql_root 'FLUSH BINARY LOGS;'
new_binlog="$(mysql_root 'SHOW BINARY LOG STATUS;' | awk '{print $1}')"
[[ "$new_binlog" != "$old_binlog" ]]
mysql_exec 'INSERT INTO cdc_reliability VALUES (6, "after-rotation");'
wait_for_lines "$tmpdir/smoke.jsonl" 5009
grep -q '"binlog_file":"'"$new_binlog"'"' "$tmpdir/smoke.jsonl"
[[ "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["binlog_file"])' "$checkpoint")" == "$new_binlog" ]]

# An interrupted MySQL connection must reconnect without moving the checkpoint.
before_network_lines="$(line_count "$tmpdir/smoke.jsonl")"
"${compose[@]}" stop mysql >/dev/null
sleep 2
[[ "$(line_count "$tmpdir/smoke.jsonl")" == "$before_network_lines" ]]
"${compose[@]}" start mysql >/dev/null
wait_for_mysql
mysql_exec 'INSERT INTO cdc_reliability VALUES (7, "after-network-reconnect");'
wait_for_lines "$tmpdir/smoke.jsonl" 5010
grep -q 'after-network-reconnect' "$tmpdir/smoke.jsonl"

# A hard process crash after commit must recover from the persisted checkpoint.
stop_binflux
start_binflux "$tmpdir/crash-restart.jsonl"
sleep 2
crash_binflux
[[ "$(line_count "$tmpdir/crash-restart.jsonl")" == 0 ]]
start_binflux "$tmpdir/crash-restart.jsonl"
sleep 2
[[ "$(line_count "$tmpdir/crash-restart.jsonl")" == 0 ]]
mysql_exec 'INSERT INTO cdc_reliability VALUES (8, "after-crash-restart");'
wait_for_lines "$tmpdir/crash-restart.jsonl" 1
grep -q 'after-crash-restart' "$tmpdir/crash-restart.jsonl"

# Graceful shutdown preserves the last committed checkpoint.
graceful_checkpoint="$(cat "$checkpoint")"
stop_binflux
grep -q 'shutdown requested' "$tmpdir/crash-restart.log"
[[ "$(cat "$checkpoint")" == "$graceful_checkpoint" ]]

# Rewinding to the pre-event checkpoint intentionally replays the same stable ID.
cp "$checkpoint" "$tmpdir/pre-duplicate.json"
mysql_exec 'INSERT INTO cdc_reliability VALUES (9, "duplicate-replay");'
start_binflux "$tmpdir/duplicate-first.jsonl"
wait_for_lines "$tmpdir/duplicate-first.jsonl" 1
first_event_id="$(python3 -c 'import json,sys; print(json.loads(open(sys.argv[1]).readline())["event_id"])' "$tmpdir/duplicate-first.jsonl")"
stop_binflux
cp "$tmpdir/pre-duplicate.json" "$checkpoint"
start_binflux "$tmpdir/duplicate-second.jsonl"
wait_for_lines "$tmpdir/duplicate-second.jsonl" 1
second_event_id="$(python3 -c 'import json,sys; print(json.loads(open(sys.argv[1]).readline())["event_id"])' "$tmpdir/duplicate-second.jsonl")"
[[ "$first_event_id" == "$second_event_id" ]]
stop_binflux

# Force a downstream write failure during a large transaction. The source
# checkpoint must remain before that transaction so a healthy restart replays it.
before_sink_failure="$(cat "$checkpoint")"
python3 - "$tmpdir/failing-large.sql" <<'PY'
import sys
values = ",".join(f"({i}, 'pipe-{i}')" for i in range(10000, 15000))
open(sys.argv[1], "w").write("START TRANSACTION;\nINSERT INTO cdc_reliability (id, note) VALUES " + values + ";\nCOMMIT;\n")
PY
mkfifo "$tmpdir/downstream.pipe"
head -n 1 "$tmpdir/downstream.pipe" > "$tmpdir/partial.jsonl" &
head_pid=$!
RUST_LOG='info,mysql_binlog_connector_rust=error' target/debug/binflux run --config "$config" --sink stdout \
  >"$tmpdir/downstream.pipe" 2>"$tmpdir/sink-failure.log" &
binflux_pid=$!
for _ in $(seq 1 60); do
  if grep -q 'connected to MySQL binlog stream' "$tmpdir/sink-failure.log"; then break; fi
  if ! kill -0 "$binflux_pid" 2>/dev/null; then cat "$tmpdir/sink-failure.log" >&2; exit 1; fi
  sleep 1
done
grep -q 'connected to MySQL binlog stream' "$tmpdir/sink-failure.log"
"${compose[@]}" exec -T mysql sh -c \
  'MYSQL_PWD="$MYSQL_PASSWORD" mysql -h127.0.0.1 -ubinflux shop -N -B' < "$tmpdir/failing-large.sql"
for _ in $(seq 1 90); do
  [[ -s "$tmpdir/partial.jsonl" ]] && ! kill -0 "$binflux_pid" 2>/dev/null && break
  sleep 1
done
[[ -s "$tmpdir/partial.jsonl" ]]
if kill -0 "$binflux_pid" 2>/dev/null; then
  echo "Binflux kept running after the downstream pipe closed" >&2
  kill -KILL "$binflux_pid" 2>/dev/null || true
  wait "$binflux_pid" 2>/dev/null || true
  binflux_pid=""
  exit 1
fi
wait "$binflux_pid" 2>/dev/null || true
binflux_pid=""
wait "$head_pid" 2>/dev/null || true
head_pid=""
assert_checkpoint_position_unchanged "$before_sink_failure"

start_binflux "$tmpdir/sink-replay.jsonl"
wait_for_lines "$tmpdir/sink-replay.jsonl" 5000
python3 - "$tmpdir/sink-replay.jsonl" <<'PY'
import json, sys
events = [json.loads(line) for line in open(sys.argv[1])]
assert len(events) == 5000, len(events)
assert all("pipe-" in event["after"]["note"] for event in events)
PY
stop_binflux

# A single supported column ALTER checkpoints cleanly and the next row carries
# the new schema fingerprint and column metadata.
start_binflux "$tmpdir/ddl.jsonl"
before_schema="$(python3 -c 'import json,sys; print(json.loads(open(sys.argv[1]).readline())["schema_version"])' "$tmpdir/sink-replay.jsonl")"
mysql_exec "ALTER TABLE cdc_reliability ADD COLUMN extra VARCHAR(40) DEFAULT 'v1'; INSERT INTO cdc_reliability VALUES (15000, 'ddl-added-column', 'after-ddl');"
wait_for_lines "$tmpdir/ddl.jsonl" 1
python3 - "$tmpdir/ddl.jsonl" "$before_schema" <<'PY'
import json, sys
event = json.loads(open(sys.argv[1]).readline())
assert event["schema_version"] != sys.argv[2]
assert event["after"]["extra"] == "after-ddl", event
assert any(column["name"] == "extra" for column in event["schema"])
PY
stop_binflux

echo "MySQL real-binlog reliability coverage passed (transactions, types, schema DDL, GTID restart, rotation, reconnect, crash, replay, sink failure, shutdown, metrics)"
success=1
