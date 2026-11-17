# Contributing to Binflux

Thanks for helping improve Binflux. This guide describes the current development workflow and the correctness constraints that changes should preserve.

## Development setup

Install a stable Rust toolchain with Rust 2024 edition support. Docker is needed for the real-MySQL integration suites.

Build the project and inspect the available commands:

```sh
cargo build
cargo run -- --help
```

For a local MySQL run, copy `binflux.example.yaml`, configure the source and checkpoint path, and provide the password through the environment variable named by `source.password_env` (the example uses `BINFLUX_MYSQL_PASSWORD`). Do not put credentials in configuration files or logs.

## Code and correctness guidelines

- Keep source reading, event decoding, transaction assembly, sinks, and checkpointing separated by their existing module boundaries.
- Preserve transaction boundaries. Do not emit or checkpoint a partial transaction.
- Preserve delivery ordering: write and flush a complete transaction to the sink before persisting its checkpoint. Stdout delivery is at-least-once; a crash between output and checkpoint replacement can replay rows.
- Keep queues and transaction buffers bounded. Unsupported or ambiguous binlog data should fail safely without advancing the checkpoint.
- Keep the single-process checkpoint lock in place. Two runs using the same checkpoint must not consume and update it concurrently.
- Keep logs on stderr so stdout remains a clean JSON Lines data stream. Never log source passwords or credential-bearing connection URLs.
- Update the README or the relevant document when changing behavior, configuration, delivery guarantees, supported MySQL features, or operational commands.
- The protocol adapter under `vendor/mysql-binlog-connector-rust` is locally patched for TLS certificate verification. Avoid unrelated vendor edits; document and isolate any necessary upstream patch.

## Checks

Run the local Rust checks before submitting a change:

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

For changes that affect binlog decoding, transactions, checkpointing, reconnects, or delivery, run the disposable real-MySQL reliability suite:

```sh
./scripts/test-mysql-reliability.sh
```

The focused end-to-end smoke test is also available:

```sh
./scripts/test-mysql-integration.sh
```

Both scripts require Docker and create disposable MySQL containers and volumes. The test certificates in `tests/certs` are for local testing only; do not reuse them elsewhere.

## Pull requests

Keep changes focused and explain the user-visible behavior or reliability effect. Include:

- A short description of the change and its motivation.
- The checks you ran and their results.
- Any configuration or delivery guarantee changes.
- Known limitations or unsupported binlog cases introduced by the change.

For bug fixes, include a regression test where practical. For new event or value support, cover both the decoded output and the checkpoint behavior on failure boundaries.
