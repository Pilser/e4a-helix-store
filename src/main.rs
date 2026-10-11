//! e4a-helix-store: stdio plugin embedding HelixDB (`db` crate) in DISK mode only.
//! Protocol: one JSON object per line on stdin → one JSON object per line on stdout.
//!   {"id":1,"op":"open","path":"/mnt/e4a/helix/school-s3"} → {"id":1,"ok":true,"data":{}}
//!   {"id":2,"op":"query","route":"concept_get","params":{...}} → {"id":2,"ok":true,"data":[...]}.
//!   {"id":3,"op":"close"} → {"id":3,"ok":true}.
//! Errors: {"id":N,"ok":false,"error":"...","code":"..."}. Never memory/object modes in v1.
//! STUB: transport works; helix wiring is the implementer task (see README.md + AGENTS.md).

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() {
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let req: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(e) => {
                println!("{{\"id\":0,\"ok\":false,\"error\":\"bad json: {e}\",\"code\":\"invalid\"}}");
                continue;
            }
        };
        let id = req.get("id").cloned().unwrap_or(Value::Null);
        let op = req.get("op").and_then(Value::as_str).unwrap_or("");
        // TODO(implementer): dispatch open/query/close against helix `db` disk handle.
        println!("{{\"id\":{id},\"ok\":false,\"error\":\"unimplemented op: {op}\",\"code\":\"unsupported\"}}");
    }
}
