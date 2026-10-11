# AGENTS.md — e4a-helix-store implementer rules

Single binary, one job: embed helix `db` (disk mode) behind the stdio protocol in README.md.

* Scope: this repo only. Protocol changes → propose to coordinator (engine depends on it).
* Build: ONLY `cargo check -p e4a-helix-store`... (single package: plain `cargo check`);
  NEVER `cargo build`, never `--release` (CI builds). `CARGO_TARGET_DIR=target/agent-<name>`.
  Tests: `cargo test` for this package only. `cargo fmt` before handoff.
* Deps: helix rev pin changes go in `Cargo.toml` with the reason in your report. No other new deps
  without justification. No `unsafe`, no `unwrap/expect` outside tests.
* Research: upstream API questions → extend the depth-1 sparse clone at `/tmp/opencode/helix-db`
  (`git sparse-checkout add crates/db/src/...`), read `open_with_config`, `HelixDbSource`,
  query-service entry points. Read, don't vendor.
* Disk-only is a hard rule (README). Single-writer assumption: engine leases; on `close`, flush.
* Do NOT commit/push. Report files, API deltas, tests, blockers. Coordinator commits.
