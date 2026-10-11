//! Protocol + persistence tests (in-process, temp dirs).
//!
//! Batches are built with the upstream AST builders and serialized to JSON,
//! so `params` travel the exact path the stdio loop sends them.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use helix_ast::{batch, expr::Predicate, prelude::g, query::QueryRequest};
use serde_json::{Value, json};

use crate::store::Store;
use crate::{handle_request, routes};

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

/// Unique temp dir for one test db. Removed when the test finishes.
struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        let id = NEXT_DIR.fetch_add(1, Ordering::SeqCst);
        let path =
            std::env::temp_dir().join(format!("e4a-helix-store-test-{}-{id}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        Self { path }
    }

    fn as_str(&self) -> &str {
        self.path.to_str().expect("temp dir is utf-8")
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Write batch for the `concept_upsert` route: one Concept node.
fn upsert_params(concept_id: &str) -> Value {
    let request = QueryRequest::write(
        batch::write_batch()
            .var_as(
                "c",
                g().add_n(
                    "Concept",
                    vec![
                        ("concept_id", concept_id),
                        ("code", "M1.1"),
                        ("title", "Counting"),
                        ("subject", "math"),
                    ],
                ),
            )
            .returning(["c"]),
    );
    serde_json::to_value(&request).expect("batch serializes")
}

/// Read batch for the `concept_get` route: filter by `concept_id`, project
/// every prop except the embedding.
fn get_params(concept_id: &str) -> Value {
    let request = QueryRequest::read(
        batch::read_batch()
            .var_as(
                "c",
                g().n_where(Predicate::eq("concept_id", concept_id))
                    .value_map(Some(vec!["concept_id", "code", "title", "subject"])),
            )
            .returning(["c"]),
    );
    serde_json::to_value(&request).expect("batch serializes")
}

async fn open(store: &Store, dir: &TempDir) -> Value {
    handle_request(store, &json!({"id": 1, "op": "open", "path": dir.as_str()})).await
}

async fn query(store: &Store, id: i64, route: &str, params: &Value) -> Value {
    handle_request(
        store,
        &json!({"id": id, "op": "query", "route": route, "params": params}),
    )
    .await
}

#[tokio::test]
async fn open_upsert_get_close_reopen_persists() {
    let dir = TempDir::new();
    let store = Store::new();

    assert_eq!(
        open(&store, &dir).await,
        json!({"id": 1, "ok": true, "data": {}})
    );

    let upserted = query(&store, 2, "concept_upsert", &upsert_params("c1")).await;
    assert_eq!(upserted["ok"], json!(true), "upsert failed: {upserted}");
    assert!(
        upserted["data"]["c"].is_array(),
        "upsert returns rows: {upserted}"
    );

    let got = query(&store, 3, "concept_get", &get_params("c1")).await;
    assert_eq!(got["ok"], json!(true), "get failed: {got}");
    let rows = got["data"]["c"].as_array().expect("get returns rows");
    assert_eq!(rows.len(), 1, "one concept row: {got}");
    assert_eq!(rows[0]["concept_id"], json!("c1"));
    assert_eq!(rows[0]["title"], json!("Counting"));

    assert_eq!(
        handle_request(&store, &json!({"id": 4, "op": "close"})).await,
        json!({"id": 4, "ok": true})
    );

    // Reopen in a fresh store: disk mode must have persisted the write.
    let store2 = Store::new();
    assert_eq!(open(&store2, &dir).await["ok"], json!(true));
    let again = query(&store2, 5, "concept_get", &get_params("c1")).await;
    assert_eq!(again["ok"], json!(true), "get after reopen failed: {again}");
    let rows = again["data"]["c"].as_array().expect("rows after reopen");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["concept_id"], json!("c1"));
    assert_eq!(
        handle_request(&store2, &json!({"id": 6, "op": "close"})).await["ok"],
        json!(true)
    );
}

#[tokio::test]
async fn second_open_closes_first() {
    let dir_a = TempDir::new();
    let dir_b = TempDir::new();
    let store = Store::new();

    assert_eq!(open(&store, &dir_a).await["ok"], json!(true));
    assert_eq!(
        query(&store, 2, "concept_upsert", &upsert_params("c9")).await["ok"],
        json!(true)
    );

    // Second open must close dir_a's writer before opening dir_b.
    assert_eq!(
        handle_request(
            &store,
            &json!({"id": 3, "op": "open", "path": dir_b.as_str()})
        )
        .await,
        json!({"id": 3, "ok": true, "data": {}})
    );
    // dir_b is empty: the c9 row lives in dir_a, not here.
    let got = query(&store, 4, "concept_get", &get_params("c9")).await;
    assert_eq!(got["ok"], json!(true), "get failed: {got}");
    assert_eq!(got["data"]["c"].as_array().map(Vec::len), Some(0));

    // dir_a still holds c9: the first handle was flushed on replacement.
    let store_a = Store::new();
    assert_eq!(open(&store_a, &dir_a).await["ok"], json!(true));
    let again = query(&store_a, 5, "concept_get", &get_params("c9")).await;
    assert_eq!(again["data"]["c"].as_array().map(Vec::len), Some(1));
}

#[tokio::test]
async fn protocol_rejections() {
    let store = Store::new();

    // Query before any open.
    let rsp = query(&store, 1, "concept_get", &get_params("c1")).await;
    assert_eq!(
        rsp,
        json!({"id": 1, "ok": false, "error": "query before open: no open database", "code": "invalid"})
    );

    // Unknown route (all six known routes are enumerated in routes::ROUTES).
    let dir = TempDir::new();
    assert_eq!(open(&store, &dir).await["ok"], json!(true));
    for route in routes::ROUTES {
        assert!(routes::is_supported(route), "{route} must be supported");
    }
    let rsp = query(&store, 2, "nope", &json!({})).await;
    assert_eq!(
        rsp,
        json!({"id": 2, "ok": false, "error": "unsupported route: nope", "code": "unsupported"})
    );

    // Params that are not a query batch.
    let rsp = query(&store, 3, "concept_get", &json!({"bogus": 1})).await;
    assert_eq!(rsp["ok"], json!(false));
    assert_eq!(rsp["code"], json!("invalid"));

    // Missing route / params / path fields.
    assert_eq!(
        handle_request(&store, &json!({"id": 4, "op": "query", "params": {}})).await["code"],
        json!("invalid")
    );
    assert_eq!(
        handle_request(
            &store,
            &json!({"id": 5, "op": "query", "route": "concept_get"})
        )
        .await["code"],
        json!("invalid")
    );
    assert_eq!(
        handle_request(&store, &json!({"id": 6, "op": "open"})).await["code"],
        json!("invalid")
    );
    // Unknown op and missing op.
    assert_eq!(
        handle_request(&store, &json!({"id": 7, "op": "drop"})).await,
        json!({"id": 7, "ok": false, "error": "unsupported op: drop", "code": "unsupported"})
    );
    assert_eq!(
        handle_request(&store, &json!({"id": 8})).await["code"],
        json!("invalid")
    );

    // Close is idempotent, and closing a fresh store succeeds too.
    assert_eq!(
        handle_request(&store, &json!({"id": 9, "op": "close"})).await,
        json!({"id": 9, "ok": true})
    );
    assert_eq!(
        handle_request(&Store::new(), &json!({"id": 10, "op": "close"})).await,
        json!({"id": 10, "ok": true})
    );
}
