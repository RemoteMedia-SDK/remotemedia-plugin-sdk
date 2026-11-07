use remotemedia_shared_memory::peer::{CallKind, WorkerEndpoint, WorkerRequest};
use remotemedia_types::{PayloadStorage, RuntimeData, TensorEncoding};
use std::{
    io,
    sync::{Arc, Condvar, Mutex},
};
mod copies;
mod invalid_copies;
mod source_completion;

#[derive(Default)]
struct Fixture {
    held: Mutex<Option<RuntimeData>>,
    proceed: Mutex<bool>,
    changed: Condvar,
}

pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().nth(2).as_deref() == Some("source-completion") {
        return source_completion::run();
    }
    if std::env::args().nth(2).as_deref() == Some("invalid-copy") {
        return invalid_copies::run();
    }
    if std::env::args().nth(2).as_deref() == Some("forged-release") {
        return forged_release();
    }
    // SAFETY: this fixture claims only the broker's inherited private channel;
    // it neither forks nor delegates buffers, and publishes only frozen owners.
    let endpoint = unsafe {
        WorkerEndpoint::from_inherited_stdio(vec![
            CallKind::Process,
            CallKind::Stream,
            CallKind::MultiInput,
            CallKind::Tick,
            CallKind::Control,
            CallKind::Rpc,
        ])
    }?;
    let fixture = Arc::new(Fixture::default());
    while let Ok(mut request) = endpoint.next_request() {
        let fixture = fixture.clone();
        std::thread::spawn(move || {
            let result = execute(&mut request, &fixture).map_err(|error| error.to_string());
            request.inputs.clear();
            request.complete(result).unwrap();
        });
    }
    Ok(())
}

fn forged_release() -> Result<(), Box<dyn std::error::Error>> {
    use remotemedia_shared_memory::{
        channel::Channel,
        peer::wire::{HostMessage, WorkerMessage, WIRE_VERSION},
    };
    // SAFETY: claim the private fixture endpoints once. This negative fixture
    // never maps or modifies grants; only an unknown lease is injected.
    let mut channel = unsafe { Channel::from_inherited_stdio() }?;
    let frame = channel.receive()?;
    let HostMessage::Hello { session, peer, .. } = serde_json::from_slice(&frame.metadata)? else {
        panic!("expected hello")
    };
    channel.send(
        &serde_json::to_vec(&WorkerMessage::Ready {
            version: WIRE_VERSION,
            snapshot_protocol: 0,
            output_copy_protocol: 0,
            source_completion_protocol: 0,
            session,
            peer,
            modes: vec![CallKind::Process],
        })?,
        None,
    )?;
    loop {
        let frame = channel.receive()?;
        if matches!(
            serde_json::from_slice::<HostMessage>(&frame.metadata)?,
            HostMessage::Execute { .. }
        ) {
            channel.send(
                &serde_json::to_vec(&WorkerMessage::InputReleased { lease: u64::MAX })?,
                None,
            )?;
        }
    }
}

fn execute(request: &mut WorkerRequest, fixture: &Fixture) -> io::Result<()> {
    match request.arguments["operation"].as_str().unwrap_or("echo") {
        "copy" => copies::execute(request, fixture)?,
        "snapshot_once" => {
            assert_eq!(request.snapshot_port_names(), ["latest"]);
            match request.read_snapshot("latest")? {
                Some(snapshot) => request.emit(snapshot.value.clone())?,
                None => request.emit(RuntimeData::Text("unpublished".into()))?,
            }
        }
        "snapshot_twice" => {
            let first = request.read_snapshot("latest")?.unwrap();
            assert_eq!((first.pts_us, first.seq), (100, 0));
            assert!(first.written_at_us > 0);
            request.emit(first.value.clone())?;
            // Output acknowledgement gates this second read. The host replaces
            // the publication before releasing the first output credit.
            let second = request.read_snapshot("latest")?.unwrap();
            assert_eq!((second.pts_us, second.seq), (200, 1));
            assert!(second.written_at_us >= first.written_at_us);
            request.emit(second.value.clone())?;
            request.emit(first.value.clone())?;
            *fixture.held.lock().unwrap() = Some(first.value.clone());
        }
        "echo" => {
            for data in request.inputs.values() {
                request.emit(data.clone())?;
            }
        }
        "slice" => emit_input_slice(request)?,
        "transform" => {
            let input = request.inputs.remove("input").unwrap();
            let (metadata, storage) = input
                .into_shared_parts(TensorEncoding::Runtime)
                .map_err(io::Error::other)?;
            let allocator = request.allocator();
            let output = match storage {
                PayloadStorage::Audio(input) => {
                    let mut output = allocator
                        .allocate_audio(input.len())
                        .map_err(io::Error::other)?;
                    let address = output.as_ptr();
                    for (dst, src) in output.iter_mut().zip(input.iter()) {
                        *dst = src * 2.0;
                    }
                    let frozen = output.freeze();
                    assert_eq!(address, frozen.as_ptr());
                    PayloadStorage::Audio(frozen)
                }
                PayloadStorage::Bytes(input) => {
                    let mut output = allocator
                        .allocate_bytes(input.len())
                        .map_err(io::Error::other)?;
                    for (dst, src) in output.iter_mut().zip(input.iter()) {
                        *dst = src.wrapping_add(1);
                    }
                    PayloadStorage::Bytes(output.freeze())
                }
                PayloadStorage::None => return Err(io::Error::other("expected bulk input")),
            };
            request.emit(
                metadata
                    .attach(output, TensorEncoding::Runtime)
                    .map_err(io::Error::other)?,
            )?;
        }
        "source" | "stream" => {
            let mut loan = request
                .allocator()
                .allocate_bytes(16)
                .map_err(io::Error::other)?;
            for (i, value) in loan.iter_mut().enumerate() {
                *value = i as u8;
            }
            let frozen = loan.freeze();
            if request.kind == CallKind::Stream {
                request.emit(RuntimeData::Binary(frozen.slice(2..6).unwrap()))?;
                request.emit(RuntimeData::Binary(frozen.slice(8..12).unwrap()))?;
            } else {
                request.emit(RuntimeData::Binary(frozen))?;
            }
        }
        "abandon_loan" | "hold_loan" => {
            abandon_loan(request, fixture)?;
        }
        "hold" => {
            *fixture.held.lock().unwrap() = request.inputs.remove("input");
        }
        "release" => {
            let held = fixture.held.lock().unwrap().take();
            drop(held);
        }
        "wait" | "snapshot_after_cancel" => {
            request.emit(RuntimeData::Text("started".into()))?;
            let mut proceed = fixture.proceed.lock().unwrap();
            while !*proceed {
                proceed = fixture.changed.wait(proceed).unwrap();
            }
            if !request.is_cancelled() {
                return Err(io::Error::other("cancellation was not delivered"));
            }
            if request.arguments["operation"] == "snapshot_after_cancel" {
                assert_eq!(
                    request.read_snapshot("latest").unwrap_err().kind(),
                    io::ErrorKind::Interrupted
                );
            }
        }
        "unblock" => {
            *fixture.proceed.lock().unwrap() = true;
            fixture.changed.notify_all();
        }
        "rpc" => {
            request.emit(RuntimeData::Json(serde_json::json!({
                "session": request.context.session_id, "node": request.context.node_id,
                "correlation": request.arguments["correlation"],
            })))?;
        }
        "private" => request.emit(RuntimeData::Binary(vec![1, 2, 3].into()))?,
        "large" => request.emit(RuntimeData::Text("z".repeat(512 * 1024)))?,
        "crash" => std::process::exit(27),
        _ => return Err(io::Error::other("unknown fixture operation")),
    }
    Ok(())
}

fn abandon_loan(request: &mut WorkerRequest, fixture: &Fixture) -> io::Result<()> {
    let mut loan = request
        .allocator()
        .allocate_bytes(16)
        .map_err(io::Error::other)?;
    loan.fill(99);
    if request.arguments["operation"] == "hold_loan" {
        request.emit(RuntimeData::Text("writing".into()))?;
        let mut proceed = fixture.proceed.lock().unwrap();
        while !*proceed {
            proceed = fixture.changed.wait(proceed).unwrap();
        }
    }
    drop(loan);
    Ok(())
}

fn emit_input_slice(request: &mut WorkerRequest) -> io::Result<()> {
    let data = request.inputs.remove("input").unwrap();
    let (metadata, storage) = data
        .into_shared_parts(TensorEncoding::Runtime)
        .map_err(io::Error::other)?;
    let storage = match storage {
        PayloadStorage::Bytes(data) => {
            PayloadStorage::Bytes(data.slice(2..data.len() - 2).map_err(io::Error::other)?)
        }
        PayloadStorage::Audio(data) => {
            PayloadStorage::Audio(data.slice(2..data.len() - 2).map_err(io::Error::other)?)
        }
        _ => return Err(io::Error::other("expected bulk input")),
    };
    request.emit(
        metadata
            .attach(storage, TensorEncoding::Runtime)
            .map_err(io::Error::other)?,
    )?;
    Ok(())
}
