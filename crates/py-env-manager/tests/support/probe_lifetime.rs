//! Shared by the interpreter and mapped-package probe regression tests.
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

pub struct ProbeFixture {
    root: PathBuf,
    pub program: PathBuf,
    pub owner: Arc<()>,
    stopped: AtomicBool,
}

impl ProbeFixture {
    pub fn new(behavior: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let original = std::env::var_os("RM_TEST_PROBE_EXE")
            .expect("set RM_TEST_PROBE_EXE to python-probe-fixture");
        let root = std::env::temp_dir().join(format!(
            "rm-probe-{}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        let program = root.join(if cfg!(windows) {
            "probe with spaces.exe"
        } else {
            "probe with spaces"
        });
        std::fs::copy(original, &program).unwrap();
        std::fs::write(root.join("behavior"), behavior).unwrap();
        Self {
            root,
            program,
            owner: Arc::new(()),
            stopped: AtomicBool::new(false),
        }
    }

    pub async fn wait_ready(&self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !self.root.join("ready-0").exists() {
            assert!(Instant::now() < deadline, "probe tree startup timed out");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub async fn assert_stopped(&self) {
        assert_eq!(
            Arc::strong_count(&self.owner),
            1,
            "process resources still retained"
        );
        let mut locks = Vec::new();
        for depth in 0..=2 {
            assert!(
                self.root.join(format!("ready-{depth}")).exists(),
                "fixture did not start its full tree"
            );
            let lock = File::options()
                .read(true)
                .write(true)
                .open(self.root.join(format!("lifetime-{depth}")))
                .unwrap();
            fs2::FileExt::try_lock_exclusive(&lock)
                .expect("probe or descendant is still accessing its environment");
            locks.push(lock);
        }
        std::fs::write(self.root.join("after-return"), b"probe").unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        for depth in 0..=2 {
            assert!(!self.root.join(format!("late-{depth}")).exists());
        }
        self.stopped.store(true, Ordering::Release);
    }

    pub fn namespace_paths() -> Option<(PathBuf, PathBuf)> {
        if cfg!(target_os = "linux") {
            Some((
                std::env::var_os("RM_TEST_PROVISION_SUPERVISOR")
                    .expect("set RM_TEST_PROVISION_SUPERVISOR")
                    .into(),
                Path::new("/usr/bin/unshare").into(),
            ))
        } else {
            None
        }
    }
}

impl Drop for ProbeFixture {
    fn drop(&mut self) {
        // On assertion failure the fixture's twenty-second watchdog bounds life;
        // retain its directory if a process is still using it for diagnosis.
        if self.stopped.load(Ordering::Acquire) {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}
