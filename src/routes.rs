//! Route dispatch for `query{route, params}`.
//!
//! The engine (`e4a-graph`) builds traversals; this plugin only executes the
//! SDK-style JSON batch in `params` via [`Store::query_json`]. Every route in
//! [`ROUTES`] is therefore served uniformly — there are no per-route stubs.
//! Semantic correctness of each route (traversal shape, `_with` param usage,
//! RRF merge) is owned by the batch the engine sends.

use serde_json::Value;

use crate::store::{Store, StoreError};

/// Helix routes from `docs/02-helix.md` this plugin executes.
pub const ROUTES: &[&str] = &[
    "concept_get",
    "prereqs_of",
    "concepts_by_subject",
    "concept_search_hybrid",
    "chunk_search",
    "concept_upsert",
];

/// Whether `route` is a known Helix route.
#[must_use]
pub fn is_supported(route: &str) -> bool {
    ROUTES.contains(&route)
}

/// Validate `route` and execute its `params` batch against the open store.
/// Returns the query `returns` map for the `{id, ok, data}` envelope.
pub async fn execute(store: &Store, route: &str, params: &Value) -> Result<Value, StoreError> {
    if !is_supported(route) {
        return Err(StoreError::Unsupported(format!(
            "unsupported route: {route}"
        )));
    }
    store.query_json(params).await
}
