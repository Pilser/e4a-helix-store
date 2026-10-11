# e4a-helix-store — HelixDB stdio plugin (separate repo, file-backed only)

Engine (`e4a-engine`) never embeds Helix source: it spawns this binary and speaks the line protocol
below. Upstream `HelixDB/helix-db` is **Apache-2.0** — embedding here is license-clean.

## Protocol (JSON lines over stdio)

| op | request | response |
|---|---|---|
| `open` | `{id, op:"open", path}` — opens/creates a DISK-mode db at `path` (single writer; engine holds the lease, §03) | `{id, ok, data:{}}` |
| `query` | `{id, op:"query", route, params}` — `route` ∈ `docs/02-helix.md` routes (`concept_get`, `prereqs_of`, `concept_search_hybrid`, `chunk_search`, `concept_upsert`); `params` = SDK-style JSON batch | `{id, ok, data}` rows |
| `close` | `{id, op:"close"}` — flush + checkpoint, release handle | `{id, ok}` |

Errors: `{id, ok:false, error, code}` with engine codes (`invalid`, `not_found`, `conflict`, `unsupported`, `internal`).

## Build rules

* Disk mode ONLY (`HelixDbSource` disk / `open_with_config` file-backed). Memory and object-storage
  modes are forbidden in v1 — persistence must survive process restarts on Swarm volumes (§07).
* Helix rev pin lives in `Cargo.toml` here and nowhere else. Upstream API questions → depth-1 sparse
  clone to `/tmp/opencode/helix-db` (already seeded), read `crates/db/src/{lib.rs,config/}`.
* CI: `cargo check --locked` + cache only.
