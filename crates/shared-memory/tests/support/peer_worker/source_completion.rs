use super::*;
use remotemedia_shared_memory::{channel::Channel, peer::wire::*};
use std::time::{Duration, Instant};

pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    let behavior = std::env::args().nth(3).unwrap();
    if behavior.starts_with("forged-") || behavior == "missing-echo" {
        return forged(&behavior);
    }
    // SAFETY: only the fixture's private broker endpoints; no delegation.
    let endpoint =
        unsafe { WorkerEndpoint::from_inherited_stdio(vec![CallKind::Tick, CallKind::Process]) }?;
    let mut previous = None::<WorkerRequest>;
    while let Ok(request) = endpoint.next_request() {
        if let Some(previous) = previous.take() {
            assert!(previous.finish_source().is_err());
        }
        let mode = request.arguments["operation"].as_str().unwrap();
        let result = if mode == "quiet" {
            Ok(())
        } else {
            produce(&request, mode).map_err(|error| error.to_string())
        };
        request.complete(result)?;
        assert!(request.finish_source().is_err());
        previous = Some(request);
    }
    Ok(())
}

fn produce(request: &WorkerRequest, mode: &str) -> io::Result<()> {
    request.finish_source()?;
    request.finish_source()?;
    let mut loan = request
        .allocator()
        .allocate_bytes(8)
        .map_err(io::Error::other)?;
    loan.copy_from_slice(&[0, 1, 2, 3, 4, 5, 6, 7]);
    let bytes = loan.freeze();
    request.emit(RuntimeData::Binary(bytes.clone()))?;
    if mode == "cancel" {
        let started = Instant::now();
        while !request.is_cancelled() && started.elapsed() < Duration::from_secs(2) {
            std::thread::yield_now();
        }
        assert!(request.is_cancelled());
        assert!(request.finish_source().is_err());
        return Ok(());
    }
    request.emit(RuntimeData::Binary(bytes.slice(2..6).unwrap()))?;
    if mode == "error" {
        return Err(io::Error::other("source failure sentinel"));
    }
    Ok(())
}

fn forged(behavior: &str) -> Result<(), Box<dyn std::error::Error>> {
    // SAFETY: this negative fixture sends metadata only and never maps grants.
    let mut channel = unsafe { Channel::from_inherited_stdio() }?;
    let hello = channel.receive()?;
    let HostMessage::Hello {
        session,
        peer,
        source_completion_protocol,
        ..
    } = serde_json::from_slice(&hello.metadata)?
    else {
        panic!("expected hello")
    };
    let ready = WorkerMessage::Ready {
        version: WIRE_VERSION,
        session,
        peer,
        modes: vec![CallKind::Tick, CallKind::Process],
        snapshot_protocol: 0,
        output_copy_protocol: 0,
        source_completion_protocol: if behavior == "missing-echo" {
            0
        } else {
            source_completion_protocol
        },
    };
    channel.send(&serde_json::to_vec(&ready)?, None)?;
    if behavior == "missing-echo" {
        let _ = channel.receive();
        return Ok(());
    }
    let call = loop {
        let frame = channel.receive()?;
        if let HostMessage::Execute { call } = serde_json::from_slice(&frame.metadata)? {
            break call;
        }
    };
    if behavior == "forged-pending" {
        let (metadata, _) = RuntimeData::Json(serde_json::json!({"pending":true}))
            .into_shared_parts(TensorEncoding::Runtime)
            .unwrap();
        channel.send(
            &serde_json::to_vec(&WorkerMessage::Output {
                call,
                sequence: 1,
                message: OutputMessage {
                    metadata,
                    buffer: None,
                },
            })?,
            None,
        )?;
    }
    channel.send(
        &serde_json::to_vec(&WorkerMessage::Complete {
            call,
            result: if behavior == "forged-error" {
                Err("failure".into())
            } else {
                Ok(())
            },
            source_finished: true,
        })?,
        None,
    )?;
    let _ = channel.receive();
    Ok(())
}
