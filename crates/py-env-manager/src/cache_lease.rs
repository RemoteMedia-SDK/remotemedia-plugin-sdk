use crate::{Error, Result, VenvInfo};
use std::{fs::File, path::Path, sync::Arc};

#[path = "cache_lease/record.rs"]
mod record;
pub(crate) use record::LeaseRecord;

/// Environment metadata with a shared cache lease. Keep this owner (or a clone)
/// until every process using the environment has terminated. Dropping the last
/// owner makes the environment eligible for eviction; it does not delete it.
/// Abrupt host death leaves a persistent record which quarantines the entry:
/// a free OS lock does not prove that all contained descendants have stopped.
///
/// Only cooperating cache managers honor this advisory lock. The lease does not
/// prevent external deletion or mutation by package installers.
#[derive(Debug, Clone)]
pub struct LeasedVenvInfo {
    info: VenvInfo,
    _lease: Arc<File>,
    _record: Arc<LeaseRecord>,
}

impl std::ops::Deref for LeasedVenvInfo {
    type Target = VenvInfo;
    fn deref(&self) -> &VenvInfo {
        &self.info
    }
}

impl LeasedVenvInfo {
    pub(crate) fn new(info: VenvInfo, file: File, record: Arc<LeaseRecord>) -> Result<Self> {
        fs2::FileExt::try_lock_shared(&file).map_err(lock_error)?;
        Ok(Self {
            info,
            _lease: Arc::new(file),
            _record: record,
        })
    }
}

pub(crate) fn lock_error(error: std::io::Error) -> Error {
    Error::Execution(format!("Python environment cache lock: {error}"))
}

// Lock files must remain outside removable environments, and must never be
// unlinked: otherwise two managers could lock different inodes for one key.
pub(crate) fn open_lock(cache: &Path, name: &str) -> Result<File> {
    let directory = cache.join(".locks");
    std::fs::create_dir_all(&directory).map_err(lock_error)?;
    File::options()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(directory.join(format!("{name}.lock")))
        .map_err(lock_error)
}

pub(crate) fn try_exclusive(file: &File) -> Result<bool> {
    match fs2::FileExt::try_lock_exclusive(file) {
        Ok(()) => Ok(true),
        Err(error) if error.raw_os_error() == fs2::lock_contended_error().raw_os_error() => {
            Ok(false)
        }
        Err(error) => Err(lock_error(error)),
    }
}

pub(crate) async fn transaction(cache: &Path) -> Result<File> {
    let file = open_lock(cache, "transaction")?;
    // Nonblocking attempts avoid occupying a runtime thread while another host
    // provisions an environment. Cancellation closes the file immediately.
    while !try_exclusive(&file)? {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    Ok(file)
}
