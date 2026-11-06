//! Explicit real-interpreter checks; no package downloads are needed.
use remotemedia_py_env::{
    system_backend::SystemBackend, EnvBackend, PythonEnvConfig, PythonEnvManager, PythonEnvMode,
};
use std::{path::PathBuf, process::Command};

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let name = format!(
            "rm-provisioning-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let path = std::env::temp_dir().join(name);
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn python() -> &'static str {
    if cfg!(windows) {
        "python"
    } else {
        "python3"
    }
}

fn python_version() -> String {
    let output = Command::new(python())
        .args([
            "-I",
            "-c",
            "import sys; print('%d.%d'%sys.version_info[:2])",
        ])
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn wheelhouse(scratch: &Scratch) -> PathBuf {
    let path = scratch.0.join("local wheels with spaces");
    std::fs::create_dir(&path).unwrap();
    let output = Command::new(python()).args(["-I", "-c", r#"
import base64,hashlib,io,csv,sys,zipfile
from pathlib import Path
files = {
 'rm_provisioning_fixture.py': b'VALUE = 42\n',
 'rm_provisioning_fixture-1.0.dist-info/METADATA': b'Metadata-Version: 2.1\nName: rm-provisioning-fixture\nVersion: 1.0\n',
 'rm_provisioning_fixture-1.0.dist-info/WHEEL': b'Wheel-Version: 1.0\nGenerator: remotemedia-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n',
}
record = io.StringIO(newline='')
writer = csv.writer(record)
for name,data in files.items():
 writer.writerow([name, 'sha256='+base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode(), len(data)])
writer.writerow(['rm_provisioning_fixture-1.0.dist-info/RECORD','',''])
files['rm_provisioning_fixture-1.0.dist-info/RECORD'] = record.getvalue().encode()
with zipfile.ZipFile(Path(sys.argv[1])/'rm_provisioning_fixture-1.0-py3-none-any.whl','w') as wheel:
 for name,data in files.items(): wheel.writestr(name,data)
"#]).arg(&path).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    path
}

#[tokio::test]
#[ignore = "requires system Python with venv and pip"]
async fn system_wheelhouse_install_and_cache_policy() {
    let scratch = Scratch::new();
    let wheelhouse = wheelhouse(&scratch);
    let config = PythonEnvConfig {
        mode: PythonEnvMode::System,
        python_version: python_version(),
        cache_dir: Some(scratch.0.join("envs")),
        base_deps: vec!["rm-provisioning-fixture==1.0".into()],
        find_links: vec![wheelhouse],
        no_index: true,
        require_python_version: true,
        ..Default::default()
    };
    let manager = PythonEnvManager::new(config.clone()).unwrap();
    let env = manager.ensure_env(&[]).await.unwrap();
    let output = Command::new(&env.python_executable)
        .args([
            "-I",
            "-c",
            "import rm_provisioning_fixture as f; assert f.VALUE == 42",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        env.cache_key,
        manager.ensure_env(&[]).await.unwrap().cache_key
    );

    // A changed wheel source must not silently reuse the successful environment.
    let mut unavailable = config.clone();
    unavailable.find_links = vec![scratch.0.join("empty wheelhouse")];
    std::fs::create_dir(&unavailable.find_links[0]).unwrap();
    assert!(PythonEnvManager::new(unavailable)
        .unwrap()
        .ensure_env(&[])
        .await
        .is_err());

    // Strict system selection rejects a different version before provisioning.
    let mut mismatch = config;
    mismatch.python_version = "999.1".into();
    let error = PythonEnvManager::new(mismatch)
        .unwrap()
        .ensure_env(&[])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("version mismatch"), "{error}");

    // Cache metadata alone cannot authorize an unusable/replaced interpreter.
    // Remove a possible symlink before writing, so the system Python is untouched.
    std::fs::remove_file(&env.python_executable).unwrap();
    std::fs::write(&env.python_executable, b"invalid interpreter").unwrap();
    assert!(manager.ensure_env(&[]).await.is_err());
}

#[tokio::test]
#[ignore = "requires system Python"]
async fn strict_system_version_rejects_mismatch_and_invalid_requests() {
    let backend = SystemBackend::new().with_config(&PythonEnvConfig {
        require_python_version: true,
        ..Default::default()
    });
    backend.ensure_python(&python_version()).await.unwrap();
    for version in ["999.1", "3.1.999", "3", "3.12.*", "3.12;print(1)"] {
        assert!(backend.ensure_python(version).await.is_err(), "{version}");
    }
}

#[cfg(feature = "bundled-uv")]
#[tokio::test]
#[ignore = "requires an installed uv binary and system Python"]
async fn uv_wheelhouse_install_without_indexes() {
    let scratch = Scratch::new();
    let wheelhouse = wheelhouse(&scratch);
    let backend = remotemedia_py_env::uv_backend::UvBackend::new()
        .unwrap()
        .with_find_links(vec![wheelhouse])
        .with_no_index(true);
    let env = backend
        .create_venv(std::path::Path::new(python()), &scratch.0, "uv-env")
        .await
        .unwrap();
    backend
        .install_deps(&env, &["rm-provisioning-fixture==1.0".into()])
        .await
        .unwrap();
    let output = Command::new(&env.python_executable)
        .args([
            "-I",
            "-c",
            "import rm_provisioning_fixture as f; assert f.VALUE == 42",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(backend
        .install_deps(&env, &["rm-provisioning-missing-fixture==987.6".into()])
        .await
        .is_err());
}
