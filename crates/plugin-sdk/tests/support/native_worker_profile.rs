//! Opt-in, bounded fixture instrumentation. No media or protocol bytes are logged.
use remotemedia_plugin_sdk::types::Error;
use std::{fs::File, io::Write, path::PathBuf, sync::Mutex};

pub struct Profile {
    file: File,
    expected: usize,
    rows: Mutex<Vec<[u64; 3]>>,
}

impl Profile {
    pub fn from_env() -> Result<Option<Self>, Error> {
        let Some(directory) = std::env::var_os("RM_NATIVE_BENCH_PROFILE_DIR") else {
            return Ok(None);
        };
        let expected: usize = std::env::var("RM_NATIVE_BENCH_PROFILE_COUNT")
            .map_err(|e| Error::Execution(e.to_string()))?
            .parse()
            .map_err(|e| Error::Execution(format!("invalid profile count: {e}")))?;
        if !(1..=4096).contains(&expected) {
            return Err(Error::Execution("profile count must be 1..=4096".into()));
        }
        let path = PathBuf::from(directory)
            .join(format!("native-worker-stages-{}.json", std::process::id()));
        let file = File::create_new(path).map_err(|e| Error::Execution(e.to_string()))?;
        Ok(Some(Self {
            file,
            expected,
            rows: Mutex::new(Vec::with_capacity(expected)),
        }))
    }

    pub fn record(&self, row: [u64; 3]) -> Result<(), Error> {
        let mut rows = self.rows.lock().unwrap();
        if rows.len() == self.expected {
            return Err(Error::Execution(
                "fixture profile sample budget exceeded".into(),
            ));
        }
        rows.push(row);
        if rows.len() == self.expected {
            let encoded = serde_json::to_vec(
                &serde_json::json!({
                    "pid": std::process::id(),
                    "node_id": std::env::var("RM_NODE_ID").ok(),
                    "columns": ["allocation_ns", "after_allocation_ns", "node_body_ns"],
                    "expected": self.expected,
                    "samples": *rows,
                    "scope": "process_async body only; excludes input import, output callback/publication and profile recording; includes warmup",
                }),
            )
            .map_err(|e| Error::Execution(e.to_string()))?;
            (&self.file)
                .write_all(&encoded)
                .map_err(|e| Error::Execution(e.to_string()))?;
        }
        Ok(())
    }
}
