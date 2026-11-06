use super::*;
use remotemedia_shared_memory::peer::{
    CallResources, HostHandle, Limits, PeerConnection, PeerEvent, ReceivedOutput,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    mpsc as blocking, Mutex,
};
use tokio::sync::{mpsc, oneshot};

enum Frame {
    Output(Output),
    Complete(Result<(), String>),
}
struct Permit(Arc<AtomicUsize>);
impl Drop for Permit {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
struct Pending {
    sender: mpsc::UnboundedSender<Frame>,
    _permit: Permit,
}
#[derive(Default)]
struct Routing {
    calls: HashMap<u64, Pending>,
    closed: Option<String>,
}
struct State {
    host: HostHandle,
    input_copies: remotemedia_types::CopyContext,
    routing: Mutex<Routing>,
}
enum Command {
    Start {
        kind: CallKind,
        context: CallContext,
        arguments: serde_json::Value,
        inputs: Vec<(String, RuntimeData)>,
        resources: CallResources,
        permit: Permit,
        cancellation: blocking::Sender<Command>,
        reply: oneshot::Sender<Result<Call, Error>>,
    },
    Cancel(u64),
    Acknowledge(ReceivedOutput),
}

pub(super) struct Client {
    state: Arc<State>,
    commands: blocking::Sender<Command>,
    admitted: Arc<AtomicUsize>,
    limit: usize,
}
pub(super) struct Call {
    id: u64,
    receiver: mpsc::UnboundedReceiver<Frame>,
    commands: blocking::Sender<Command>,
    completed: bool,
}
pub(super) struct Output {
    guard: Option<ReceivedOutput>,
    commands: blocking::Sender<Command>,
}
impl Output {
    pub(super) fn into_data(self) -> RuntimeData {
        // Bulk variants imported by the dispatcher already hold immutable
        // shared owners. Clone retains those owners; no payload is materialized.
        self.guard.as_ref().unwrap().data().clone()
    }
}
impl Drop for Output {
    fn drop(&mut self) {
        if let Some(guard) = self.guard.take() {
            let _ = self.commands.send(Command::Acknowledge(guard));
        }
    }
}
impl Call {
    pub(super) async fn next(&mut self) -> Result<Option<Output>, Error> {
        match self.receiver.recv().await {
            Some(Frame::Output(output)) => Ok(Some(output)),
            Some(Frame::Complete(result)) => {
                self.completed = true;
                result.map_err(Error::Execution)?;
                Ok(None)
            }
            None => Err(Error::Execution(
                "native worker reply channel closed".into(),
            )),
        }
    }
}
impl Drop for Call {
    fn drop(&mut self) {
        if !self.completed {
            let _ = self.commands.send(Command::Cancel(self.id));
        }
    }
}

impl Client {
    pub(super) fn close(&self) {
        self.state.host.close();
    }

    pub(super) fn new(
        mut peer: PeerConnection,
        host: HostHandle,
        limits: Limits,
        input_copies: remotemedia_types::CopyContext,
    ) -> Result<Self, Error> {
        let state = Arc::new(State {
            host,
            input_copies,
            routing: Mutex::new(Routing::default()),
        });
        let (commands, requests) = blocking::channel();
        let control = state.clone();
        std::thread::Builder::new()
            .name("rm-native-host-send".into())
            .spawn(move || {
                for command in requests {
                    match command {
                        Command::Cancel(call) => {
                            let _ = control.host.cancel(call);
                        }
                        Command::Acknowledge(output) => drop(output),
                        Command::Start {
                            kind,
                            context,
                            arguments,
                            inputs,
                            resources,
                            permit,
                            cancellation,
                            reply,
                        } => {
                            let result = control.start(
                                kind,
                                context,
                                arguments,
                                inputs,
                                resources,
                                permit,
                                cancellation,
                            );
                            // If the awaiting future disappeared, dropping its Call
                            // enqueues cancellation. Admission stays pinned until the
                            // worker actually completes or the connection closes.
                            let _ = reply.send(result);
                        }
                    }
                }
            })
            .map_err(transport_error)?;
        let receive = state.clone();
        let acknowledgements = commands.clone();
        if let Err(error) = std::thread::Builder::new()
            .name("rm-native-host-receive".into())
            .spawn(move || {
                loop {
                    match peer.next_event() {
                        Ok(PeerEvent::Output(output)) => {
                            let sender = receive
                                .routing
                                .lock()
                                .unwrap()
                                .calls
                                .get(&output.call())
                                .map(|pending| pending.sender.clone());
                            // Never drop output guards under the routing lock: their
                            // acknowledgements may need to write to the worker pipe.
                            let output = Output {
                                guard: Some(output),
                                commands: acknowledgements.clone(),
                            };
                            if let Some(sender) = sender {
                                let _ = sender.send(Frame::Output(output));
                            }
                        }
                        Ok(PeerEvent::Complete { call, result }) => {
                            let pending = receive.routing.lock().unwrap().calls.remove(&call);
                            if let Some(Pending { sender, _permit }) = pending {
                                drop(_permit);
                                let _ = sender.send(Frame::Complete(result));
                            }
                        }
                        Ok(PeerEvent::Ready { .. }) => {
                            receive.fail("unexpected second native worker handshake".into());
                            break;
                        }
                        Err(error) => {
                            receive.fail(error.to_string());
                            break;
                        }
                    }
                }
            })
        {
            state.host.close();
            return Err(transport_error(error));
        }
        Ok(Self {
            state,
            commands,
            admitted: Arc::new(AtomicUsize::new(0)),
            limit: limits.calls,
        })
    }

    pub(super) async fn call(
        &self,
        kind: CallKind,
        context: CallContext,
        arguments: serde_json::Value,
        inputs: Vec<(String, RuntimeData)>,
        resources: CallResources,
    ) -> Result<Call, Error> {
        self.admitted
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.limit).then_some(count + 1)
            })
            .map_err(|_| Error::Execution("native worker call budget exhausted".into()))?;
        let permit = Permit(self.admitted.clone());
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::Start {
                kind,
                context,
                arguments,
                inputs,
                resources,
                permit,
                cancellation: self.commands.clone(),
                reply,
            })
            .map_err(|_| Error::Execution("native worker command thread stopped".into()))?;
        response.await.map_err(transport_error)?
    }
}
impl Drop for Client {
    fn drop(&mut self) {
        self.state.host.close();
    }
}

impl State {
    fn start(
        &self,
        kind: CallKind,
        context: CallContext,
        arguments: serde_json::Value,
        inputs: Vec<(String, RuntimeData)>,
        resources: CallResources,
        permit: Permit,
        commands: blocking::Sender<Command>,
    ) -> Result<Call, Error> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut routing = self.routing.lock().unwrap();
        if let Some(error) = &routing.closed {
            return Err(Error::Execution(error.clone()));
        }
        let resources = CallResources {
            copies: Some(self.input_copies.clone()),
            ..resources
        };
        // Prevent a fast worker output from racing route registration. The
        // receive thread still services allocation traffic within next_event;
        // the worker's independent reader can consume the whole input batch.
        let id = self
            .host
            .start_call_with_resources(kind, context, arguments, inputs, resources)
            .map_err(transport_error)?;
        routing.calls.insert(
            id,
            Pending {
                sender,
                _permit: permit,
            },
        );
        Ok(Call {
            id,
            receiver,
            commands,
            completed: false,
        })
    }
    fn fail(&self, error: String) {
        self.host.close();
        let pending = {
            let mut routing = self.routing.lock().unwrap();
            routing.closed = Some(error.clone());
            std::mem::take(&mut routing.calls)
        };
        for (_, call) in pending {
            let _ = call.sender.send(Frame::Complete(Err(error.clone())));
        }
    }
}
