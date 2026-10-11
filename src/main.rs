//! e4a-helix-store: stdio plugin embedding HelixDB (`db` crate) in DISK mode only.
//!
//! Protocol: one JSON object per line on stdin → one JSON object per line on stdout.
//!   {"id":1,"op":"open","path":"/mnt/e4a/helix/school-s3"} → {"id":1,"ok":true,"data":{}}
//!   {"id":2,"op":"query","route":"concept_get","params":{...}} → {"id":2,"ok":true,"data":{...}}.
//!   {"id":3,"op":"close"} → {"id":3,"ok":true}.
//! Errors: {"id":N,"ok":false,"error":"...","code":"..."}. Never memory/object modes in v1.

mod routes;
mod store;
#[cfg(test)]
mod tests;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::store::{Store, StoreError};

#[tokio::main]
async fn main() {
    let store = Store::new();
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            // EOF: engine hung up; shut down quietly.
            Ok(None) => break,
            Err(e) => {
                emit(
                    &StoreError::Invalid(format!("stdin read error: {e}"))
                        .to_response(&Value::Null),
                );
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let response = match serde_json::from_str::<Value>(&line) {
            Ok(req) => handle_request(&store, &req).await,
            Err(e) => StoreError::Invalid(format!("bad json: {e}")).to_response(&json!(0)),
        };
        emit(&response);
    }
}

/// Print one response line. Line-buffered stdout flushes on `\n`.
fn emit(response: &Value) {
    println!(
        "{}",
        serde_json::to_string(response).unwrap_or_else(|_| {
            r#"{"id":0,"ok":false,"error":"response encode failed","code":"invalid"}"#.to_string()
        })
    );
}

/// Dispatch one parsed envelope to its response object (also used by tests).
async fn handle_request(store: &Store, req: &Value) -> Value {
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let op = req.get("op").and_then(Value::as_str).unwrap_or("");
    match op {
        "open" => match req.get("path").and_then(Value::as_str) {
            Some(path) => match store.open(path).await {
                Ok(()) => json!({"id": id, "ok": true, "data": {}}),
                Err(e) => e.to_response(&id),
            },
            None => {
                StoreError::Invalid("open requires a string \"path\"".to_string()).to_response(&id)
            }
        },
        "query" => {
            let route = req.get("route").and_then(Value::as_str);
            let params = req.get("params");
            match (route, params) {
                (Some(route), Some(params)) => match routes::execute(store, route, params).await {
                    Ok(data) => json!({"id": id, "ok": true, "data": data}),
                    Err(e) => e.to_response(&id),
                },
                (None, _) => StoreError::Invalid("query requires a string \"route\"".to_string())
                    .to_response(&id),
                (_, None) => StoreError::Invalid(
                    "query requires \"params\" (SDK-style JSON batch)".to_string(),
                )
                .to_response(&id),
            }
        }
        "close" => match store.close().await {
            Ok(()) => json!({"id": id, "ok": true}),
            Err(e) => e.to_response(&id),
        },
        "" => StoreError::Invalid("missing \"op\"".to_string()).to_response(&id),
        other => StoreError::Unsupported(format!("unsupported op: {other}")).to_response(&id),
    }
}
