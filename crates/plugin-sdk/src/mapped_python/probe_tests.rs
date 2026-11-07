use super::*;
#[path = "../../../py-env-manager/tests/support/probe_lifetime.rs"]
mod fixture;
use fixture::ProbeFixture;

fn command(fixture: &ProbeFixture) -> ProvisioningCommand {
    let namespace = ProbeFixture::namespace_paths().map(|(supervisor, unshare)| {
        remotemedia_py_env::ProcessNamespace {
            supervisor,
            unshare,
        }
    });
    ProvisioningContext::new(namespace)
        .with_resource(fixture.owner.clone())
        .command(&fixture.program)
        .unwrap()
}

#[tokio::test]
async fn package_timeout_joins_probe_and_detached_descendants() {
    let fixture = ProbeFixture::new("timeout");
    let error = verification_output(command(&fixture), Duration::from_secs(2))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error}");
    fixture.assert_stopped().await;
}

#[tokio::test]
async fn package_task_abort_joins_probe_and_detached_descendants() {
    let fixture = ProbeFixture::new("timeout");
    let command = command(&fixture);
    let task = tokio::spawn(verification_output(command, Duration::from_secs(30)));
    fixture.wait_ready().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    fixture.assert_stopped().await;
}

#[tokio::test]
async fn successful_probe_matches_executed_process_and_joins_descendants() {
    let fixture = ProbeFixture::new("success");
    let (probe, expected) = verification_output(command(&fixture), Duration::from_secs(2))
        .await
        .unwrap();
    probe.ensure_process(expected).unwrap();
    assert!(probe.base_executable.is_file());
    fixture.assert_stopped().await;
}

#[tokio::test]
async fn probe_from_another_process_is_rejected() {
    let fixture = ProbeFixture::new("wrong-pid");
    let (probe, expected) = verification_output(command(&fixture), Duration::from_secs(2))
        .await
        .unwrap();
    assert!(probe
        .ensure_process(expected)
        .unwrap_err()
        .to_string()
        .contains("delegated"));
    fixture.assert_stopped().await;
}
