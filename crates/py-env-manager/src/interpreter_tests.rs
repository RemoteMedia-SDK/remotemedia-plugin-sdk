use super::*;
#[path = "../tests/support/probe_lifetime.rs"]
mod fixture;
use fixture::ProbeFixture;

fn context(fixture: &ProbeFixture) -> ProvisioningContext {
    let namespace =
        ProbeFixture::namespace_paths().map(|(supervisor, unshare)| crate::ProcessNamespace {
            supervisor,
            unshare,
        });
    ProvisioningContext::new(namespace).with_resource(fixture.owner.clone())
}

#[tokio::test]
async fn version_timeout_joins_probe_and_detached_descendants() {
    let fixture = ProbeFixture::new("timeout");
    let context = context(&fixture);
    let error = validate_with_timeout(
        &fixture.program,
        "3.12",
        &context,
        std::time::Duration::from_secs(2),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("timed out"), "{error}");
    drop(context);
    fixture.assert_stopped().await;
}

#[tokio::test]
async fn version_task_abort_joins_probe_and_detached_descendants() {
    let fixture = ProbeFixture::new("timeout");
    let context = context(&fixture);
    let program = fixture.program.clone();
    let task =
        tokio::spawn(async move { validate_python_version(&program, "3.12", &context).await });
    fixture.wait_ready().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    fixture.assert_stopped().await;
}
