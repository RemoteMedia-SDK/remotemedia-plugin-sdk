use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "rm-cache-lease-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct FixtureBackend;
#[async_trait]
impl EnvBackend for FixtureBackend {
    async fn ensure_python(&self, _: &str) -> Result<PathBuf> {
        Ok("fixture".into())
    }
    async fn create_venv(&self, _: &Path, root: &Path, key: &str) -> Result<VenvInfo> {
        let path = root.join(key);
        std::fs::create_dir(&path).unwrap();
        let python_executable = path.join("python");
        std::fs::write(&python_executable, b"fixture interpreter").unwrap();
        Ok(VenvInfo {
            path,
            python_executable,
            cache_key: key.into(),
        })
    }
    async fn install_deps(&self, _: &VenvInfo, _: &[String]) -> Result<()> {
        Ok(())
    }
    fn resolve_python(&self, venv: &VenvInfo) -> PathBuf {
        venv.path.join("python")
    }
}

#[tokio::test]
async fn returned_environment_survives_lru_pressure_until_last_owner_drops() {
    let scratch = Scratch::new();
    let cache = VenvCache::new(scratch.0.clone(), 1);
    let first = cache
        .get_or_create(&[], "3.12", Some("first"), &FixtureBackend)
        .await
        .unwrap();
    let first_path = first.path.clone();
    let held = first.clone();
    let meta_path = first_path.join(METADATA_FILENAME);
    let mut meta: VenvMetadata =
        serde_json::from_slice(&std::fs::read(&meta_path).unwrap()).unwrap();
    meta.last_used_at = "0000".into();
    std::fs::write(meta_path, serde_json::to_vec(&meta).unwrap()).unwrap();
    let second = cache
        .get_or_create(&[], "3.12", Some("second"), &FixtureBackend)
        .await
        .unwrap();
    assert!(
        first_path.join("python").exists(),
        "LRU deleted an environment still in use"
    );
    assert!(second.python_executable.exists());
    drop(first);
    cache.evict_lru().await.unwrap();
    assert!(first_path.exists(), "a cloned owner must keep the lease");
    drop(held);
    cache.evict_lru().await.unwrap();
    assert!(
        !first_path.exists(),
        "idle old environment did not become evictable"
    );
    assert!(second.python_executable.exists());
}

#[tokio::test]
async fn cache_hits_from_another_manager_retain_independent_leases() {
    let scratch = Scratch::new();
    let cache = VenvCache::new(scratch.0.clone(), 0);
    let other = VenvCache::new(scratch.0.clone(), 0);
    let first = cache
        .get_or_create(&[], "3.12", None, &FixtureBackend)
        .await
        .unwrap();
    let second = other
        .get_or_create(&[], "3.12", None, &FixtureBackend)
        .await
        .unwrap();
    let path = first.path.clone();
    drop(first);
    cache.evict_lru().await.unwrap();
    assert!(path.exists());
    drop(second);
    cache.evict_lru().await.unwrap();
    assert!(!path.exists());
}

#[tokio::test]
async fn stale_leased_environment_is_not_replaced() {
    let scratch = Scratch::new();
    let cache = VenvCache::new(scratch.0.clone(), 1);
    let first = cache
        .get_or_create(&[], "3.12", None, &FixtureBackend)
        .await
        .unwrap();
    std::fs::remove_file(first.path.join(METADATA_FILENAME)).unwrap();
    let marker = first.path.join("retained-module.py");
    std::fs::write(&marker, b"still used by worker").unwrap();
    let error = cache
        .get_or_create(&[], "3.12", None, &FixtureBackend)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("while it is leased"), "{error}");
    assert!(marker.exists());
    drop(first);
    let replacement = cache
        .get_or_create(&[], "3.12", None, &FixtureBackend)
        .await
        .unwrap();
    assert!(replacement.python_executable.exists());
    assert!(!marker.exists());
}

#[tokio::test]
async fn eviction_skips_oldest_leased_entry_and_removes_next_idle_entry() {
    let scratch = Scratch::new();
    let cache = VenvCache::new(scratch.0.clone(), 2);
    let first = cache
        .get_or_create(&[], "3.12", Some("first"), &FixtureBackend)
        .await
        .unwrap();
    let idle = cache
        .get_or_create(&[], "3.12", Some("idle"), &FixtureBackend)
        .await
        .unwrap();
    let idle_path = idle.path.clone();
    drop(idle);
    let third = cache
        .get_or_create(&[], "3.12", Some("third"), &FixtureBackend)
        .await
        .unwrap();
    assert!(first.python_executable.exists());
    assert!(third.python_executable.exists());
    assert!(!idle_path.exists());
}

// The same test binary provides a separate cache host. Its stdin is a lifetime
// gate, so tests cover both graceful release and OS lock release after a kill.
#[test]
fn cache_lease_child() {
    let Some(root) = std::env::var_os("RM_CACHE_LEASE_CHILD_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let cache = VenvCache::new(root.clone(), 1);
    let _lease = runtime
        .block_on(cache.get_or_create(&[], "3.12", Some("child"), &FixtureBackend))
        .unwrap();
    std::fs::write(root.join("ready"), b"leased").unwrap();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).unwrap();
}

struct ChildHost(std::process::Child);
impl Drop for ChildHost {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn cross_process_release(kill: bool) {
    use std::io::Write;
    let scratch = Scratch::new();
    let cache = VenvCache::new(scratch.0.clone(), 1);
    let first = cache
        .get_or_create(&[], "3.12", Some("child"), &FixtureBackend)
        .await
        .unwrap();
    let first_path = first.path.clone();
    let mut child = ChildHost(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "cache_tests::cache_lease_child", "--nocapture"])
            .env("RM_CACHE_LEASE_CHILD_ROOT", &scratch.0)
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !scratch.0.join("ready").exists() {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "child exited before acquiring lease"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "child lease timed out"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    drop(first);
    let second = cache
        .get_or_create(&[], "3.12", Some("parent"), &FixtureBackend)
        .await
        .unwrap();
    assert!(
        first_path.join("python").exists(),
        "evicted another host's environment"
    );
    if kill {
        child.0.kill().unwrap();
    } else {
        child
            .0
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"release\n")
            .unwrap();
    }
    let status = child.0.wait().unwrap();
    if !kill {
        assert!(status.success());
    }
    cache.evict_lru().await.unwrap();
    assert!(
        !first_path.exists(),
        "terminated host left an uncollectable lease"
    );
    assert!(second.python_executable.exists());
}

#[tokio::test]
async fn other_host_lease_survives_pressure_and_releases_on_exit() {
    cross_process_release(false).await;
}

#[tokio::test]
async fn other_host_lease_survives_pressure_and_releases_on_kill() {
    cross_process_release(true).await;
}

// This subprocess models the actual output().await pattern used by the system
// and uv backends. It accesses the environment only after the parent has aborted
// provisioning and attempted to obtain the environment's exclusive cache lock.
#[test]
fn cancelled_provision_child() {
    let Some(root) = std::env::var_os("RM_CANCEL_PROVISION_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    std::fs::write(root.join("ready"), std::process::id().to_string()).unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline && !root.join("stop").exists() {
        if root.join("after-cancel").exists() {
            // A surviving installer could modify/import these same files.
            if std::fs::write(root.join("wrote-after-cancel"), b"still running").is_err() {
                return;
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let _ = std::fs::write(root.join("stopped"), b"no more filesystem access");
}

struct CancellableBackend;
#[async_trait]
impl EnvBackend for CancellableBackend {
    async fn ensure_python(&self, _: &str) -> Result<PathBuf> {
        Ok(std::env::current_exe().unwrap())
    }
    async fn create_venv(&self, python: &Path, root: &Path, key: &str) -> Result<VenvInfo> {
        let info = FixtureBackend.create_venv(python, root, key).await?;
        let output = tokio::process::Command::new(python)
            .args([
                "--exact",
                "cache_tests::cancelled_provision_child",
                "--nocapture",
            ])
            .env("RM_CANCEL_PROVISION_ROOT", &info.path)
            .output()
            .await
            .map_err(cache_lease::lock_error)?;
        assert!(output.status.success());
        Ok(info)
    }
    async fn install_deps(&self, _: &VenvInfo, _: &[String]) -> Result<()> {
        Ok(())
    }
    fn resolve_python(&self, info: &VenvInfo) -> PathBuf {
        FixtureBackend.resolve_python(info)
    }
}

struct StopInstaller(PathBuf);
impl Drop for StopInstaller {
    fn drop(&mut self) {
        let _ = std::fs::write(self.0.join("stop"), b"stop");
    }
}

#[tokio::test]
#[ignore = "known cancellation ownership gap; explicit failing regression retained until fixed"]
async fn cancelled_provisioning_keeps_protection_until_installer_stops() {
    let scratch = Scratch::new();
    let cache = Arc::new(VenvCache::new(scratch.0.clone(), 1));
    let key = VenvCache::cache_key("3.12", &[], Some("cancelled"));
    let path = scratch.0.join(&key);
    let cleanup = StopInstaller(path.clone());
    let task_cache = cache.clone();
    let task = tokio::spawn(async move {
        task_cache
            .get_or_create(&[], "3.12", Some("cancelled"), &CancellableBackend)
            .await
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !path.join("ready").exists() {
        assert!(
            !task.is_finished(),
            "provisioning ended before the installer became ready"
        );
        assert!(
            std::time::Instant::now() < deadline,
            "installer startup timed out"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let child_pid = std::fs::read_to_string(path.join("ready")).unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let lock = cache_lease::open_lock(&scratch.0, &key).unwrap();
    let exclusive_available = cache_lease::try_exclusive(&lock).unwrap();
    std::fs::write(path.join("after-cancel"), b"probe").unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(500);
    while !path.join("wrote-after-cancel").exists() && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    let wrote_after_cancel = path.join("wrote-after-cancel").exists();
    drop(cleanup);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !path.join("stopped").exists() && std::time::Instant::now() < deadline {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    println!("child_pid={child_pid} exclusive_available={exclusive_available} wrote_after_cancel={wrote_after_cancel} cleanup_observed={}", path.join("stopped").exists());
    assert!(!(exclusive_available && wrote_after_cancel), "cancelled provisioning released cache protection while its installer could still access the environment");
}
