use super::WorkerProcess;
use std::{
    future::Future,
    io::{self, Read},
    pin::Pin,
    process::Output,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
    thread::{self, JoinHandle},
    time::Duration,
};

#[derive(Default)]
struct Completion {
    output: Option<io::Result<Output>>,
    waker: Option<Waker>,
}
#[derive(Default)]
struct State {
    cancel: AtomicBool,
    completion: Mutex<Completion>,
}

/// Owns process capture through completion or cancellation. Drop synchronously
/// joins shutdown, so a surrounding future cannot release its guards first.
/// This is a provisioning/control-plane primitive, not a real-time audio path.
pub struct CapturedOutput {
    state: Arc<State>,
    owner: Option<JoinHandle<()>>,
}

impl Future for CapturedOutput {
    type Output = io::Result<Output>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut completion = this.state.completion.lock().unwrap();
        if let Some(output) = completion.output.take() {
            drop(completion);
            this.join();
            Poll::Ready(output)
        } else {
            completion.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
impl CapturedOutput {
    fn join(&mut self) {
        if let Some(owner) = self.owner.take() {
            let _ = owner.join();
        }
    }
}
impl Drop for CapturedOutput {
    fn drop(&mut self) {
        self.state.cancel.store(true, Ordering::Release);
        self.join();
    }
}

pub(super) fn begin(mut process: WorkerProcess) -> io::Result<CapturedOutput> {
    let stdout = process
        .take_stdout()
        .ok_or_else(|| io::Error::other("command stdout was not captured"))?;
    let stderr = process
        .take_stderr()
        .ok_or_else(|| io::Error::other("command stderr was not captured"))?;
    let state = Arc::new(State::default());
    let worker_state = state.clone();
    let owner = thread::Builder::new()
        .name("rm-command-owner".into())
        .spawn(move || {
            let output = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                run(process, stdout, stderr, &worker_state.cancel)
            }))
            .unwrap_or_else(|_| Err(io::Error::other("command owner panicked")));
            let waker = {
                let mut completion = worker_state.completion.lock().unwrap();
                completion.output = Some(output);
                completion.waker.take()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        })?;
    Ok(CapturedOutput {
        state,
        owner: Some(owner),
    })
}

fn reader(input: impl Read + Send + 'static) -> io::Result<JoinHandle<io::Result<Vec<u8>>>> {
    thread::Builder::new()
        .name("rm-command-output".into())
        .spawn(move || {
            let mut input = input;
            let mut bytes = Vec::new();
            input.read_to_end(&mut bytes)?;
            Ok(bytes)
        })
}

fn read_result(reader: JoinHandle<io::Result<Vec<u8>>>) -> io::Result<Vec<u8>> {
    reader
        .join()
        .map_err(|_| io::Error::other("command output reader panicked"))?
}

fn run(
    mut process: WorkerProcess,
    stdout: impl Read + Send + 'static,
    stderr: impl Read + Send + 'static,
    cancel: &AtomicBool,
) -> io::Result<Output> {
    let stdout = reader(stdout)?;
    let stderr = match reader(stderr) {
        Ok(reader) => reader,
        Err(error) => {
            process.terminate()?;
            let _ = read_result(stdout);
            return Err(error);
        }
    };
    let observed = loop {
        match process.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Err(error) => break Err(error),
            Ok(None) if cancel.load(Ordering::Acquire) => break process.terminate(),
            Ok(None) => thread::sleep(Duration::from_millis(5)),
        }
    };
    // Even natural main-process exit is insufficient: close descendants before
    // joining their inherited output pipes and releasing any retained resources.
    process.terminate()?;
    let stdout = read_result(stdout);
    let stderr = read_result(stderr);
    Ok(Output {
        status: observed?,
        stdout: stdout?,
        stderr: stderr?,
    })
}
