//! Delay guardian scheduling to test lock ordering independently of kill speed.
use super::*;
use std::{
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    time::{Duration, Instant},
};

fn cache(root: PathBuf) -> VenvCache {
    let mut cache = VenvCache::new(root, 1);
    cache.process = ProvisioningContext::new(Some(ProcessNamespace {
        supervisor: std::env::var_os("RM_TEST_PROVISION_SUPERVISOR")
            .expect("set RM_TEST_PROVISION_SUPERVISOR")
            .into(),
        unshare: "/usr/bin/unshare".into(),
    }));
    cache
}

#[tokio::test]
async fn provisioning_host() {
    let Some(root) = std::env::var_os("RM_CACHE_CRASH_HOST") else {
        return;
    };
    cache(root.into())
        .get_or_create(&[], "3.12", Some("crash"), &CancellableBackend)
        .await
        .unwrap();
}

struct Guardian(OwnedFd);
impl Guardian {
    fn signal(&self, signal: i32) {
        let result = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.0.as_raw_fd(),
                signal,
                0,
                0,
            )
        };
        assert_eq!(result, 0, "{}", std::io::Error::last_os_error());
    }

    fn exited(&self, timeout: i32) -> bool {
        let mut poll = libc::pollfd {
            fd: self.0.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        assert!(unsafe { libc::poll(&mut poll, 1, timeout) } >= 0);
        assert_eq!(poll.revents & (libc::POLLNVAL | libc::POLLERR), 0);
        poll.revents & libc::POLLIN != 0
    }
}
impl Drop for Guardian {
    fn drop(&mut self) {
        // Always release the scheduling fault, including assertion unwinds.
        unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                self.0.as_raw_fd(),
                libc::SIGCONT,
                0,
                0,
            );
        }
    }
}

#[tokio::test]
async fn host_death_quarantines_cache_while_guardian_has_not_stopped_installer() {
    let scratch = Scratch::new();
    let key = VenvCache::cache_key("3.12", &[], Some("crash"));
    let path = scratch.0.join(&key);
    let mut child = ChildHost(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "cache_tests::cancellation::host_crash::provisioning_host",
                "--nocapture",
            ])
            .env("RM_CACHE_CRASH_HOST", &scratch.0)
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while !path.join("guardian").exists() || !path.join("ready").exists() {
        assert!(child.0.try_wait().unwrap().is_none(), "host exited early");
        assert!(Instant::now() < deadline, "installer startup timed out");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let pid: u32 = std::fs::read_to_string(path.join("guardian"))
        .unwrap()
        .parse()
        .unwrap();
    // The live host owns this guardian. Obtain its original handle before the
    // crash; all subsequent signalling and exit checks use that handle.
    let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    assert!(raw >= 0, "{}", std::io::Error::last_os_error());
    let guardian = Guardian(unsafe { OwnedFd::from_raw_fd(raw as i32) });
    assert!(!guardian.exited(0));
    guardian.signal(libc::SIGSTOP);
    loop {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
        if status
            .lines()
            .any(|line| line.starts_with("State:") && line.contains("T (stopped)"))
        {
            break;
        }
        assert!(Instant::now() < deadline, "guardian did not stop");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let lease = cache_lease::open_lock(&scratch.0, &key).unwrap();
    let transaction = cache_lease::open_lock(&scratch.0, "transaction").unwrap();
    assert!(!cache_lease::try_exclusive(&lease).unwrap());
    assert!(!cache_lease::try_exclusive(&transaction).unwrap());
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let lease_available = cache_lease::try_exclusive(&lease).unwrap();
    let transaction_available = cache_lease::try_exclusive(&transaction).unwrap();
    // Retain the original observation: OS locks alone become available. The
    // persistent record must protect actual manager operations after unlocking.
    drop((lease, transaction));
    std::fs::write(path.join("after-cancel"), b"probe").unwrap();
    let deadline = Instant::now() + Duration::from_millis(250);
    while !path.join("wrote-after-cancel").exists() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let wrote = path.join("wrote-after-cancel").exists();
    let other_cache = cache(scratch.0.clone());
    let replacement = other_cache
        .get_or_create(&[], "3.12", Some("crash"), &FixtureBackend)
        .await;
    let info = VenvInfo {
        path: path.clone(),
        python_executable: path.join("python"),
        cache_key: key.clone(),
    };
    let manager = PythonEnvManager {
        backend: Arc::new(FixtureBackend),
        config: PythonEnvConfig::default(),
        cache: other_cache,
    };
    let mutation = manager
        .install_additional_deps(&info, &["new-package".into()])
        .await;
    // Restore scheduling and verify cleanup before reporting the invariant.
    guardian.signal(libc::SIGCONT);
    assert!(
        guardian.exited(5000),
        "guardian did not finish host-death cleanup"
    );
    println!("host_dead=true guardian_delayed=true lease_available={lease_available} transaction_available={transaction_available} installer_wrote={wrote}");
    assert!(wrote, "fault did not retain a live environment accessor");
    assert!(replacement.unwrap_err().to_string().contains("quarantined"));
    assert!(mutation.unwrap_err().to_string().contains("quarantined"));
    assert!(
        path.join("wrote-after-cancel").exists(),
        "manager removed live environment"
    );
    // Guardian exit is known to this test, not to the second cache manager.
    // It must not guess that recovery is safe from time or free OS locks.
    let error = manager
        .cache
        .get_or_create(&[], "3.12", Some("crash"), &FixtureBackend)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("quarantined"));
}
