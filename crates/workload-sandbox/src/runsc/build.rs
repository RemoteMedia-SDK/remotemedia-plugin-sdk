use std::fs::File;
use std::future::Future;
use std::io::SeekFrom;

use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::oneshot;
use tokio::time::{timeout_at, Instant};

use super::{validate_request, RunscReservation};
use crate::{SandboxError, WorkloadExit, WorkloadPhase, WorkloadRequest};

/// Identity of one approved build output. The caller supplies it before launch.
pub struct BuildOutput {
    pub size: u64,
    pub sha256: [u8; 32],
}

impl RunscReservation {
    /// Run a disposable build and receive exactly the approved output on
    /// stdout. The returned anonymous file is rewound. It is released only
    /// after a successful exit and confirmed teardown of the build sandbox.
    pub async fn build(
        self,
        request: WorkloadRequest,
        expected: BuildOutput,
    ) -> Result<File, SandboxError> {
        self.build_until(request, expected, std::future::pending())
            .await
    }

    pub(super) async fn build_until(
        self,
        request: WorkloadRequest,
        expected: BuildOutput,
        cancelled: impl Future<Output = ()>,
    ) -> Result<File, SandboxError> {
        validate_request(&request, crate::WorkloadTarget::LinuxCpu)?;
        if request.phase != WorkloadPhase::Build
            || expected.size == 0
            || expected.size > request.limits.scratch_bytes
        {
            return Err(SandboxError::Invalid(
                "a build requires a nonempty approved output within its scratch limit",
            ));
        }

        // Build jobs never write to a host directory or receive the file as a
        // handle. The host drains the pipe into an anonymous bounded spool.
        let spool = tempfile::tempfile_in(&self.provider.0.config.state)?;
        let deadline = Instant::now() + request.limits.lifetime;
        let (capture, output) = oneshot::channel();
        let mut run = self
            .provider
            .start_reserved(self.bundle, request, Some(capture))
            .await?;
        let capture = timeout_at(deadline, async {
            let output = output
                .await
                .map_err(|_| SandboxError::Runtime("build output pipe"))?;
            verify_output(output, spool, expected).await
        });
        tokio::pin!(cancelled);
        let stopped = || SandboxError::Runtime("build cancelled");
        let captured = tokio::select! {
            biased;
            _ = &mut cancelled => Err(stopped()),
            result = capture => result.unwrap_or(Err(SandboxError::Timeout("build output"))),
        };
        let file = match captured {
            Ok(file) => file,
            Err(error) => {
                run.stop().await?;
                return Err(error);
            }
        };
        let exit = tokio::select! {
            biased;
            _ = &mut cancelled => {
                run.stop().await?;
                return Err(stopped());
            }
            exit = run.wait_for_exit() => exit?,
        };
        match exit {
            WorkloadExit::Exited(0) => Ok(file),
            WorkloadExit::Deadline => Err(SandboxError::Timeout("build lifetime")),
            _ => Err(SandboxError::Runtime("build did not exit successfully")),
        }
    }
}

async fn verify_output(
    output: impl tokio::io::AsyncRead + Unpin,
    spool: File,
    expected: BuildOutput,
) -> Result<File, SandboxError> {
    let mut output = output.take(expected.size + 1);
    let mut spool = tokio::fs::File::from_std(spool);
    let mut buffer = vec![0; 16 * 1024];
    let mut digest = Sha256::new();
    let mut written = 0;
    loop {
        let size = output.read(&mut buffer).await?;
        if size == 0 {
            break;
        }
        written += size as u64;
        if written > expected.size {
            return Err(SandboxError::Invalid(
                "build output exceeds the approved size",
            ));
        }
        digest.update(&buffer[..size]);
        spool.write_all(&buffer[..size]).await?;
    }
    let observed: [u8; 32] = digest.finalize().into();
    if written != expected.size || observed != expected.sha256 {
        return Err(SandboxError::Invalid(
            "build output differs from the approved identity",
        ));
    }
    spool.flush().await?;
    spool.seek(SeekFrom::Start(0)).await?;
    Ok(spool.into_std().await)
}
