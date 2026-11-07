//! Negative protocol fixture: no writable grants are mapped or delegated.
use remotemedia_shared_memory::{
    channel::Channel,
    peer::wire::{
        AllocationFailure, BufferReference, CallKind, HostMessage, OutputMessage, WorkerMessage,
        WIRE_VERSION,
    },
    ElementType,
};
use remotemedia_types::RuntimeMetadata;

pub(super) fn run() -> Result<(), Box<dyn std::error::Error>> {
    // SAFETY: claim private endpoints once; all received handles stay unmapped.
    let mut channel = unsafe { Channel::from_inherited_stdio() }?;
    let frame = channel.receive()?;
    let HostMessage::Hello {
        session,
        peer,
        snapshot_protocol,
        output_copy_protocol,
        ..
    } = serde_json::from_slice(&frame.metadata)?
    else {
        panic!("expected hello")
    };
    channel.send(
        &serde_json::to_vec(&WorkerMessage::Ready {
            version: WIRE_VERSION,
            session,
            peer,
            snapshot_protocol,
            output_copy_protocol,
            modes: vec![CallKind::Process],
        })?,
        None,
    )?;
    let mut behavior = String::new();
    let call = loop {
        let frame = channel.receive()?;
        match serde_json::from_slice::<HostMessage>(&frame.metadata)? {
            HostMessage::Begin { arguments, .. } => {
                behavior = arguments["behavior"].as_str().unwrap().into()
            }
            HostMessage::Execute { call } => break call,
            _ => panic!("unexpected setup message"),
        }
    };
    channel.send(
        &serde_json::to_vec(&WorkerMessage::AllocateCopy {
            request: 1,
            call,
            element: ElementType::Bytes,
            elements: 16,
        })?,
        None,
    )?;
    let frame = channel.receive()?;
    let HostMessage::Grant { descriptor, .. } = serde_json::from_slice(&frame.metadata)? else {
        panic!("expected copy grant")
    };
    let allocation = descriptor.allocation;
    let finished = |call, success| WorkerMessage::CopyFinished {
        call,
        allocation,
        success,
    };
    if matches!(behavior.as_str(), "credit" | "overflow") {
        channel.send(
            &serde_json::to_vec(&WorkerMessage::AllocateCopy {
                request: 2,
                call,
                element: ElementType::F32Le,
                elements: if behavior == "overflow" { u64::MAX } else { 4 },
            })?,
            None,
        )?;
        let reply = channel.receive()?;
        let HostMessage::AllocationFailed { request: 2, reason } =
            serde_json::from_slice(&reply.metadata)?
        else {
            panic!("host must enforce output copy bounds")
        };
        assert_eq!(
            reason,
            if behavior == "overflow" {
                AllocationFailure::InvalidLength
            } else {
                AllocationFailure::Exhausted
            }
        );
        // The raw fixture has no mapping; closing the received file precedes
        // its release acknowledgement. No copy or publication was attempted.
        drop(frame);
        for message in [
            finished(call, false),
            WorkerMessage::ProducerReleased { allocation },
            WorkerMessage::Complete {
                call,
                result: Ok(()),
            },
        ] {
            channel.send(&serde_json::to_vec(&message)?, None)?;
        }
        let _ = channel.receive();
        return Ok(());
    }
    if behavior == "duplicate" || behavior == "failed-output" {
        channel.send(
            &serde_json::to_vec(&finished(call, behavior == "duplicate"))?,
            None,
        )?;
    }
    let message = match behavior.as_str() {
        "wrong-call" => finished(call + 1, true),
        "duplicate" => finished(call, true),
        "complete" => WorkerMessage::Complete {
            call,
            result: Ok(()),
        },
        "release" => WorkerMessage::ProducerReleased { allocation },
        "pending-output" | "failed-output" => WorkerMessage::Output {
            call,
            sequence: 1,
            message: OutputMessage {
                metadata: RuntimeMetadata::Binary,
                buffer: Some(BufferReference::Producer {
                    allocation,
                    offset: 0,
                    byte_len: 16,
                }),
            },
        },
        _ => panic!("unknown invalid copy case"),
    };
    channel.send(&serde_json::to_vec(&message)?, None)?;
    // The host rejects the frame and joins this peer before reclaiming its grant.
    let _ = channel.receive();
    Ok(())
}
