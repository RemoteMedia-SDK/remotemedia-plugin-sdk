//! Crash-surviving records for cooperating cache managers. OS lock release alone
//! cannot authorize reuse: a dead host can still have contained live descendants.
use super::{lock_error, try_exclusive};
use crate::{Error, Result};
use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug)]
pub(crate) struct LeaseRecord {
    path: PathBuf,
    _file: File,
}

impl LeaseRecord {
    fn directory(cache: &Path, key: &str) -> PathBuf {
        cache.join(".locks").join(format!("{key}.owners"))
    }

    /// Caller holds the cache transaction lock. Names identify records, never
    /// process incarnations: neither PID reuse nor elapsed time permits recovery.
    pub(crate) fn new(cache: &Path, key: &str) -> Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = Self::directory(cache, key);
        std::fs::create_dir_all(&directory).map_err(lock_error)?;
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        for _ in 0..128 {
            let sequence = NEXT.fetch_add(1, Ordering::Relaxed);
            let path = directory.join(format!("{}-{stamp}-{sequence}.lease", std::process::id()));
            let file = match File::options()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&path)
            {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(lock_error(error)),
            };
            // A failed initialization leaves an unowned record and fails closed.
            fs2::FileExt::try_lock_exclusive(&file).map_err(lock_error)?;
            file.sync_all().map_err(lock_error)?;
            return Ok(Self { path, _file: file });
        }
        Err(Error::Execution(
            "Cannot allocate a unique Python cache lease record".into(),
        ))
    }

    fn entries(cache: &Path, key: &str) -> Result<Option<std::fs::ReadDir>> {
        match std::fs::read_dir(Self::directory(cache, key)) {
            Ok(entries) => Ok(Some(entries)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(lock_error(error)),
        }
    }

    pub(crate) fn present(cache: &Path, key: &str) -> Result<bool> {
        match Self::entries(cache, key)? {
            Some(mut entries) => entries
                .next()
                .transpose()
                .map(|entry| entry.is_some())
                .map_err(lock_error),
            None => Ok(false),
        }
    }

    /// Live read owners may coexist. An unowned record means its host did not
    /// confirm shutdown; leave the record and environment untouched.
    pub(crate) fn require_confirmed_owners(cache: &Path, key: &str) -> Result<()> {
        let Some(entries) = Self::entries(cache, key)? else {
            return Ok(());
        };
        for entry in entries {
            let path = entry.map_err(lock_error)?.path();
            let file = match File::options().read(true).write(true).open(&path) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(lock_error(error)),
            };
            // Drop unlinks while holding the record lock. If that raced with
            // this open, a now-absent path proves this particular owner released.
            if try_exclusive(&file)? && path.try_exists().map_err(lock_error)? {
                return Err(Error::Execution(format!(
                    "Python environment {key} is quarantined: unconfirmed cache owner at {}. Host exit or a free OS lock is not proof that descendants stopped",
                    path.display()
                )));
            }
        }
        Ok(())
    }
}

impl Drop for LeaseRecord {
    fn drop(&mut self) {
        // Keep the lock held until unlink. A failed unlink leaves a conservative
        // quarantine record. ProcessResources forgets this owner on unknown exit.
        if let Err(error) = std::fs::remove_file(&self.path) {
            tracing::warn!(path = %self.path.display(), %error, "Could not release Python cache lease record");
        }
    }
}
