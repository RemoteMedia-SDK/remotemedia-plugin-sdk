use super::*;

// This subprocess uses the same contained output owner as the system
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
    async fn ensure_python(&self, _: &str, _: &ProvisioningContext) -> Result<PathBuf> {
        Ok(std::env::current_exe().unwrap())
    }
    async fn create_venv(
        &self,
        python: &Path,
        root: &Path,
        key: &str,
        context: &ProvisioningContext,
    ) -> Result<VenvInfo> {
        let info = FixtureBackend
            .create_venv(python, root, key, context)
            .await?;
        let mut command = context.command(python).map_err(cache_lease::lock_error)?;
        command
            .command_mut()
            .args([
                "--exact",
                "cache_tests::cancellation::cancelled_provision_child",
                "--nocapture",
            ])
            .env("RM_CANCEL_PROVISION_ROOT", &info.path);
        let output = command.output().await.map_err(cache_lease::lock_error)?;
        assert!(output.status.success());
        Ok(info)
    }
    async fn install_deps(
        &self,
        _: &VenvInfo,
        _: &[String],
        _: &ProvisioningContext,
    ) -> Result<()> {
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

pub(super) async fn check() {
    let scratch = Scratch::new();
    let cache = VenvCache::new(scratch.0.clone(), 1);
    #[cfg(target_os = "linux")]
    let cache = {
        let mut cache = cache;
        cache.process = ProvisioningContext::new(Some(ProcessNamespace {
            supervisor: std::env::var_os("RM_TEST_PROVISION_SUPERVISOR")
                .expect("set RM_TEST_PROVISION_SUPERVISOR to the built matching supervisor")
                .into(),
            unshare: "/usr/bin/unshare".into(),
        }));
        cache
    };
    let cache = Arc::new(cache);
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
    println!("child_pid={child_pid} exclusive_available={exclusive_available} wrote_after_cancel={wrote_after_cancel}");
    assert!(
        exclusive_available,
        "joined installer did not release protection"
    );
    assert!(
        !wrote_after_cancel,
        "installer wrote after joined cancellation"
    );
    assert!(
        !path.join(METADATA_FILENAME).exists(),
        "cancelled environment was published"
    );
    fs2::FileExt::unlock(&lock).unwrap();
    let replacement = cache
        .get_or_create(&[], "3.12", Some("cancelled"), &FixtureBackend)
        .await
        .unwrap();
    assert!(replacement.python_executable.exists());
}
