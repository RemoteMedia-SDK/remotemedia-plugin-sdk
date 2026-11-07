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
