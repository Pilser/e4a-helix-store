//! Disk-mode HelixDB handle behind the stdio plugin protocol.
//!
//! Single writer only: [`Store`] holds at most one open [`HelixDB`] handle.
//! Opening a second path closes the first. Only [`HelixDbSource::Disk`] is
//! ever constructed here — memory and object-storage modes are refused by
//! construction (there is no protocol field that could select them).

use std::path::PathBuf;

use helix_db_engine::{HelixDB, HelixDbSource, error::HelixDbError};
use serde_json::Value;
use thiserror::Error;
use tokio::sync::Mutex;

/// Logical database name inside the disk object-store root.
///
/// The protocol `open{path}` maps to `Disk { root: path, database: <this> }`,
/// so one filesystem path always owns exactly one logical database.
const DATABASE_NAME: &str = "e4a";

/// Plugin errors, already classified into the engine codes from the protocol
/// (`invalid`, `not_found`, `conflict`, `unsupported`, `internal`).
#[derive(Debug, Error)]
pub enum StoreError {
    /// Malformed request, bad batch JSON, or planner/validation failure.
    #[error("{0}")]
    Invalid(String),
    /// Referenced node/edge/index does not exist.
    #[error("{0}")]
    NotFound(String),
    /// Retryable transaction conflict or index backpressure.
    #[error("{0}")]
    Conflict(String),
    /// Route or operation the plugin does not implement.
    #[error("{0}")]
    Unsupported(String),
    /// Storage/config/operational failure (open, flush, checkpoint).
    #[error("{0}")]
    Internal(String),
}

impl StoreError {
    /// Engine error code for the response envelope.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Invalid(_) => "invalid",
            Self::NotFound(_) => "not_found",
            Self::Conflict(_) => "conflict",
            Self::Unsupported(_) => "unsupported",
            Self::Internal(_) => "internal",
        }
    }

    /// Byte-exact error envelope: `{id, ok:false, error, code}`.
    #[must_use]
    pub fn to_response(&self, id: &Value) -> Value {
        serde_json::json!({
            "id": id,
            "ok": false,
            "error": self.to_string(),
            "code": self.code(),
        })
    }

    /// Classify a HelixDB failure into an engine code.
    ///
    /// Anything without a precise code is an operational failure: `internal`.
    /// The message always carries the full upstream error text so the engine
    /// can log it.
    fn from_helix(error: HelixDbError) -> Self {
        if matches!(
            error,
            HelixDbError::TransactionConflict(_)
                | HelixDbError::RequestReadViewChanged
                | HelixDbError::IndexBackpressure { .. }
                | HelixDbError::UniqueConstraintViolation { .. }
        ) {
            return Self::Conflict(error.to_string());
        }
        if matches!(
            error,
            HelixDbError::NodeNotFound(_)
                | HelixDbError::EdgeNotFound { .. }
                | HelixDbError::IndexNotFound(_)
                | HelixDbError::IndexOperationNotFound { .. }
        ) {
            return Self::NotFound(error.to_string());
        }
        // Planner/query errors sometimes report explicitly unsupported
        // constructs; surface those as `unsupported` instead of `invalid`.
        let text = error.to_string();
        if text.to_lowercase().contains("unsupported") {
            return Self::Unsupported(text);
        }
        Self::Internal(text)
    }
}

/// Single-writer disk handle. `&self` methods so the stdio loop can share it.
pub struct Store {
    db: Mutex<Option<HelixDB>>,
}

impl Store {
    /// Closed store; the first `open` brings the handle up.
    #[must_use]
    pub fn new() -> Self {
        Self {
            db: Mutex::new(None),
        }
    }

    /// Open (creating) a DISK-mode db at `path`. A currently open handle is
    /// closed first, so at most one writer exists at any time.
    ///
    /// Uses [`HelixDB::open`] (embedded defaults for the disk profile), the
    /// same constructor the server-side open path builds on.
    pub async fn open(&self, path: &str) -> Result<(), StoreError> {
        if path.is_empty() {
            return Err(StoreError::Invalid(
                "open requires a non-empty \"path\"".to_string(),
            ));
        }
        // Second open closes first: drop the old handle outside the lock.
        let old = self.db.lock().await.take();
        if let Some(db) = old {
            // Release even if flush fails; report the first failure.
            let flush_err = db.flush_writer().await.err();
            let close_err = db.close().await.err();
            if let Some(e) = flush_err {
                return Err(StoreError::from_helix(e));
            }
            if let Some(e) = close_err {
                return Err(StoreError::from_helix(e));
            }
        }
        tokio::fs::create_dir_all(path)
            .await
            .map_err(|e| StoreError::Invalid(format!("cannot create db dir {path}: {e}")))?;
        let source = HelixDbSource::Disk {
            root: PathBuf::from(path),
            database: DATABASE_NAME.to_string(),
        };
        let db = HelixDB::open(source)
            .await
            .map_err(StoreError::from_helix)?;
        *self.db.lock().await = Some(db);
        Ok(())
    }

    /// Execute an SDK-style JSON batch (the `params` of a `query` request)
    /// through the same in-process service path the server's `POST /v2/query`
    /// uses ([`HelixDB::query_json`]), and return the `returns` map.
    pub async fn query_json(&self, params: &Value) -> Result<Value, StoreError> {
        let guard = self.db.lock().await;
        let Some(db) = guard.as_ref() else {
            return Err(StoreError::Invalid(
                "query before open: no open database".to_string(),
            ));
        };
        let bytes = serde_json::to_vec(params)
            .map_err(|e| StoreError::Invalid(format!("params are not valid JSON: {e}")))?;
        let out = db
            .query_json(&bytes)
            .await
            .map_err(StoreError::from_helix)?;
        serde_json::from_slice::<Value>(&out)
            .map_err(|e| StoreError::Invalid(format!("bad query response: {e}")))
    }

    /// Flush + close + release the handle. Idempotent: closing a closed or
    /// never-opened store succeeds.
    pub async fn close(&self) -> Result<(), StoreError> {
        let db = self.db.lock().await.take();
        let Some(db) = db else { return Ok(()) };
        let flush_err = db.flush_writer().await.err();
        let close_err = db.close().await.err();
        if let Some(e) = flush_err {
            return Err(StoreError::from_helix(e));
        }
        if let Some(e) = close_err {
            return Err(StoreError::from_helix(e));
        }
        Ok(())
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}
