use super::*;
use std::{
    fs::File,
    time::{Duration, Instant},
};

/// Use installed system Python so this test cannot trigger uv's Python download
/// path. Environment creation and installation use the real selected backend.
struct OfflineBackend {
    installer: Arc<dyn EnvBackend>,
    python: PathBuf,
}
#[async_trait]
impl EnvBackend for OfflineBackend {
    async fn ensure_python(&self, version: &str, context: &ProvisioningContext) -> Result<PathBuf> {
        crate::interpreter::validate_python_version(&self.python, version, context).await?;
        Ok(self.python.clone())
    }
    async fn create_venv(
        &self,
        python: &Path,
        cache: &Path,
        key: &str,
        context: &ProvisioningContext,
    ) -> Result<VenvInfo> {
        self.installer
            .create_venv(python, cache, key, context)
            .await
    }
    async fn install_deps(
        &self,
        venv: &VenvInfo,
        deps: &[String],
        context: &ProvisioningContext,
    ) -> Result<()> {
        self.installer.install_deps(venv, deps, context).await
    }
    fn resolve_python(&self, venv: &VenvInfo) -> PathBuf {
        self.installer.resolve_python(venv)
    }
}

struct InstallerFixture {
    _scratch: Scratch,
    project: PathBuf,
    manager: PythonEnvManager,
}
impl InstallerFixture {
    fn new(uv: bool, phase: &str, additional: bool) -> Arc<Self> {
        let scratch = Scratch::new();
        let project = scratch.0.join("source");
        std::fs::create_dir(&project).unwrap();
        std::fs::write(
            project.join("backend.py"),
            include_str!("../../tests/support/cancellable_backend.py"),
        )
        .unwrap();
        std::fs::write(
            project.join("pyproject.toml"),
            "[build-system]\nrequires = []\nbuild-backend = \"backend\"\nbackend-path = [\".\"]\n",
        )
        .unwrap();
        std::fs::write(project.join("phase"), phase).unwrap();
        let mut config = PythonEnvConfig {
            python_version: "3.12".into(),
            no_index: true,
            require_python_version: true,
            cache_dir: Some(scratch.0.join("envs")),
            ..Default::default()
        };
        #[cfg(target_os = "linux")]
        {
            config.process_namespace = Some(ProcessNamespace {
                supervisor: std::env::var_os("RM_TEST_PROVISION_SUPERVISOR")
                    .expect("set RM_TEST_PROVISION_SUPERVISOR")
                    .into(),
                unshare: "/usr/bin/unshare".into(),
            });
        }
        if !additional {
            config
                .base_deps
                .push(project.to_string_lossy().into_owned());
        }
        let installer: Arc<dyn EnvBackend> = if uv {
            Arc::new(uv_backend::UvBackend::new().unwrap().with_no_index(true))
        } else {
            Arc::new(system_backend::SystemBackend::new().with_config(&config))
        };
        // A command name such as `python3` is a version request to uv, not
        // necessarily the interpreter that Command would resolve on PATH.
        let python = PathBuf::from(
            std::env::var_os("RM_TEST_INSTALLER_PYTHON")
                .expect("set RM_TEST_INSTALLER_PYTHON to an installed Python 3.12 executable"),
        );
        assert!(python.is_absolute() && python.is_file());
        let backend = Arc::new(OfflineBackend { installer, python });
        let mut cache = VenvCache::new(config.cache_dir.clone().unwrap(), 1);
        cache.process = ProvisioningContext::new(config.process_namespace.clone());
        // Deliberately omit SDK auto-discovery: this is a hermetic empty/source
        // environment, not a managed node installation.
        let manager = PythonEnvManager {
            backend,
            cache,
            config,
        };
        Arc::new(Self {
            _scratch: scratch,
            project,
            manager,
        })
    }

    fn dependencies(&self) -> Vec<String> {
        vec![self.project.to_string_lossy().into_owned()]
    }

    fn environment(&self) -> PathBuf {
        let paths: Vec<_> = std::fs::read_dir(&self.manager.cache.cache_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|p| p.is_dir() && p.file_name().unwrap() != ".locks")
            .collect();
        assert_eq!(paths.len(), 1);
        paths.into_iter().next().unwrap()
    }

    async fn wait_ready(&self, task: &mut tokio::task::JoinHandle<Result<()>>) {
        let deadline = Instant::now() + Duration::from_secs(60);
        while !self.project.join("ready-0").exists() {
            if task.is_finished() {
                panic!("installer ended before selected hook: {:?}", task.await);
            }
            if Instant::now() >= deadline {
                task.abort();
                let _ = task.await;
                panic!("installer did not reach selected hook within 60 seconds");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn assert_stopped(&self, target: &Path) {
        let mut locks = Vec::new();
        for depth in 0..=2 {
            assert!(self.project.join(format!("ready-{depth}")).exists());
            let lock = File::options()
                .read(true)
                .write(true)
                .open(self.project.join(format!("lifetime-{depth}")))
                .unwrap();
            assert!(
                cache_lease::try_exclusive(&lock).unwrap(),
                "installer descendant still owns its access lock"
            );
            locks.push(lock);
        }
        std::fs::write(self.project.join("after-cancel"), b"probe").unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        for depth in 0..=2 {
            assert!(!target.join(format!("late-installer-{depth}")).exists());
        }
    }

    async fn recover(&self, additional: bool) {
        std::fs::write(self.project.join("phase"), b"complete").unwrap();
        let env = self.manager.ensure_env(&[]).await.unwrap();
        if additional {
            self.manager
                .install_additional_deps(&env, &self.dependencies())
                .await
                .unwrap();
        }
        let env = self.manager.ensure_env_leased(&[]).await.unwrap();
        let mut command = self
            .manager
            .cache
            .process
            .clone()
            .with_resource(env.clone())
            .command(&env.python_executable)
            .unwrap();
        command.command_mut().args([
            "-I",
            "-c",
            "import rm_cancel_fixture as f; assert f.VALUE == 42",
        ]);
        let output = command.output().await.unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(env.path.join(METADATA_FILENAME).exists());
    }
}

async fn cancel_install(uv: bool, phase: &str, additional: bool) {
    let fixture = InstallerFixture::new(uv, phase, additional);
    let seed = if additional {
        Some(fixture.manager.ensure_env(&[]).await.unwrap())
    } else {
        None
    };
    let worker = fixture.clone();
    let mut task = tokio::spawn(async move {
        if let Some(seed) = seed {
            worker
                .manager
                .install_additional_deps(&seed, &worker.dependencies())
                .await
        } else {
            worker.manager.ensure_env(&[]).await.map(|_| ())
        }
    });
    fixture.wait_ready(&mut task).await;
    let target = fixture.environment();
    std::fs::write(fixture.project.join("target"), target.to_str().unwrap()).unwrap();
    let key = target.file_name().unwrap().to_str().unwrap();
    let lease = cache_lease::open_lock(&fixture.manager.cache.cache_dir, key).unwrap();
    let transaction =
        cache_lease::open_lock(&fixture.manager.cache.cache_dir, "transaction").unwrap();
    assert!(!cache_lease::try_exclusive(&lease).unwrap());
    assert!(!cache_lease::try_exclusive(&transaction).unwrap());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(cache_lease::try_exclusive(&lease).unwrap());
    assert!(cache_lease::try_exclusive(&transaction).unwrap());
    fixture.assert_stopped(&target).await;
    assert!(
        !target.join(METADATA_FILENAME).exists(),
        "cancelled installation remained reusable"
    );
    drop((lease, transaction));
    fixture.recover(additional).await;
    println!("installer={} phase={phase} additional={additional} locks_released=true late_writes=false recovered=true", if uv { "uv" } else { "pip" });
}

macro_rules! installer_case {
    ($name:ident, $uv:expr, $phase:literal) => {
        #[tokio::test]
        #[ignore = "requires RM_TEST_INSTALLER_PYTHON (Python 3.12), pip, uv, and an explicit Linux supervisor; offline source build"]
        async fn $name() {
            cancel_install($uv, $phase, false).await;
            cancel_install($uv, $phase, true).await;
        }
    };
}
installer_case!(pip_requirements_cancellation, false, "requirements");
installer_case!(pip_metadata_cancellation, false, "metadata");
installer_case!(pip_wheel_cancellation, false, "wheel");
installer_case!(uv_requirements_cancellation, true, "requirements");
installer_case!(uv_metadata_cancellation, true, "metadata");
installer_case!(uv_wheel_cancellation, true, "wheel");
