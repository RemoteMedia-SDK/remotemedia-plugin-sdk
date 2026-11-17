//! Opt-in, content-free stage tracing for test harnesses.
//!
//! On only when `REMOTEMEDIA_STAGE_TRACE=1`, or the older
//! `REMOTEMEDIA_RAG_TRACE=1`, is set. Each event is one line on stderr:
//! `RAG_STAGE ` and a JSON record (the prefix scaling's RAG evaluation
//! parser reads). Records carry ids, stage names, local elapsed times and the
//! details a caller passes; never what was said. Times are local elapsed
//! durations, never synchronized host timestamps.

use serde_json::{json, Value};
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

/// The variables that turn tracing on.
pub const ENV: &str = "REMOTEMEDIA_STAGE_TRACE";
pub const LEGACY_ENV: &str = "REMOTEMEDIA_RAG_TRACE";
/// Each record's line prefix.
pub const PREFIX: &str = "RAG_STAGE ";

static NEXT: AtomicU64 = AtomicU64::new(1);
static LOST: AtomicU64 = AtomicU64::new(0);
static RECORD: AtomicU64 = AtomicU64::new(1);
static RUN: OnceLock<String> = OnceLock::new();

/// Whether tracing is on.
pub fn enabled() -> bool {
    [ENV, LEGACY_ENV]
        .iter()
        .any(|name| std::env::var(name).as_deref() == Ok("1"))
}

/// One stage of a reply: a `start` record now, `end` when finished (or
/// `unfinished` when dropped first), and any marks in between.
pub struct Stage {
    identity: Option<Value>,
    started: Instant,
    finished: bool,
}

impl Stage {
    pub fn new(stage: &str, reply: &str, details: Value) -> Self {
        let identity = enabled().then(|| {
            json!({"version":1,"stage":stage,"reply_id":reply,"process_id":std::process::id(),
                   "process_instance":RUN.get_or_init(|| uuid::Uuid::new_v4().to_string()),
                   "span_id":NEXT.fetch_add(1, Ordering::Relaxed),"clock":"local_elapsed_ms"})
        });
        let span = Self {
            identity,
            started: Instant::now(),
            finished: false,
        };
        span.mark("start", details);
        span
    }

    pub fn mark(&self, event: &str, details: Value) {
        if let Some(record) = self.record(event, details) {
            // Value's Display writes JSON in fragments. A host logger in another
            // DLL (or stdout redirected to the same file) has a different lock
            // and can split those fragments. Serialize before the single write.
            let line = format!("{PREFIX}{record}\n");
            if std::io::stderr().lock().write_all(line.as_bytes()).is_err() {
                LOST.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn finish(mut self, details: Value) {
        self.finished = true;
        self.mark("end", details);
    }

    fn record(&self, event: &str, details: Value) -> Option<Value> {
        let mut record = self.identity.clone()?;
        record["event"] = json!(event);
        record["record_id"] = json!(RECORD.fetch_add(1, Ordering::Relaxed));
        record["elapsed_ms"] = json!(self.started.elapsed().as_secs_f64() * 1000.0);
        record["details"] = details;
        record["lost_records"] = json!(LOST.load(Ordering::Relaxed));
        Some(record)
    }
}

impl Drop for Stage {
    fn drop(&mut self) {
        if !self.finished {
            // Dropping can mean cancellation, error or a closed consumer. Do
            // not claim a more specific cause that this diagnostic cannot know.
            self.mark("end", json!({"outcome":"unfinished"}));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // One test touches the environment, so tests cannot race on it.
    #[test]
    fn switched_by_either_variable() {
        std::env::remove_var(ENV);
        std::env::remove_var(LEGACY_ENV);
        assert!(!enabled());
        let off = Stage::new("turn", "r-1", json!({}));
        assert!(off.record("mark", json!({})).is_none());
        drop(off);

        std::env::set_var(LEGACY_ENV, "1");
        assert!(enabled());
        let on = Stage::new("turn", "r-1", json!({}));
        let record = on.record("first_text", json!({"n": 1})).unwrap();
        assert_eq!(record["stage"], "turn");
        assert_eq!(record["reply_id"], "r-1");
        assert_eq!(record["event"], "first_text");
        assert_eq!(record["clock"], "local_elapsed_ms");
        on.finish(json!({"outcome": "answered"}));
        std::env::remove_var(LEGACY_ENV);

        std::env::set_var(ENV, "1");
        assert!(enabled());
        std::env::remove_var(ENV);
    }
}
