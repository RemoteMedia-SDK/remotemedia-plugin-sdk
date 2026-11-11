#![cfg(all(feature = "test-peer", any(windows, target_os = "linux")))]
#[path = "support/channel_protocol.rs"]
mod protocol;
use protocol::{Command, Event};
use remotemedia_shared_memory::{
    channel::{HandleAccess, Receiver, Sender, WorkerProcess, MAX_CONTROL_BYTES},
    PeerId, PoolConfig, SessionId, SharedPool, SizeClass,
};
use remotemedia_types::{PayloadStorage, RuntimeData, TensorEncoding};
use std::{fs::File, process::Command as ProcessCommand};

fn send(sender: &mut Sender, command: Command, file: Option<(&File, HandleAccess)>) {
    sender
        .send(&serde_json::to_vec(&command).unwrap(), file)
        .unwrap();
}
fn receive(receiver: &mut Receiver) -> Event {
    let frame = receiver.receive().unwrap();
    assert!(frame.file.is_none());
    serde_json::from_slice(&frame.metadata).unwrap()
}
fn spawn() -> (WorkerProcess, Sender, Receiver) {
    let mut command = ProcessCommand::new(env!("CARGO_BIN_EXE_shared-memory-test-peer"));
    command.arg("channel");
    let mut child = WorkerProcess::spawn(&mut command).unwrap();
    let (sender, receiver) = child.take_channel().unwrap().split().unwrap();
    (child, sender, receiver)
}

#[test]
fn process_resources_follow_confirmed_termination_not_original_child_wait() {
    use remotemedia_shared_memory::channel::ProcessResources;
    use std::sync::Arc;
    for explicit_terminate in [false, true] {
        let owner = Arc::new(());
        let mut command = ProcessCommand::new(env!("CARGO_BIN_EXE_shared-memory-test-peer"));
        command.arg("channel");
        let mut child =
            WorkerProcess::spawn_with_resources(&mut command, ProcessResources::new(owner.clone()))
                .unwrap();
        let (mut sender, mut receiver) = child.take_channel().unwrap().split().unwrap();
        send(
            &mut sender,
            Command::Hello(SessionId([94; 16]), PeerId(5)),
            None,
        );
        assert!(matches!(receive(&mut receiver), Event::Hello(..)));
        assert_eq!(Arc::strong_count(&owner), 2);
        send(&mut sender, Command::Stop, None);
        assert!(child.wait().unwrap().success());
        assert_eq!(
            Arc::strong_count(&owner),
            2,
            "wait alone does not confirm contained descendants exited"
        );
        if explicit_terminate {
            child.terminate().unwrap();
            assert_eq!(Arc::strong_count(&owner), 1);
        }
        drop(child);
        assert_eq!(Arc::strong_count(&owner), 1);
    }
}

#[test]
fn failed_spawn_releases_resources_when_no_process_was_created() {
    use remotemedia_shared_memory::channel::ProcessResources;
    use std::sync::Arc;
    let owner = Arc::new(());
    let scratch = tempfile::tempdir().unwrap();
    let mut command = ProcessCommand::new(scratch.path().join("missing-executable"));
    assert!(WorkerProcess::spawn_with_resources(
        &mut command,
        ProcessResources::new(owner.clone())
    )
    .is_err());
    assert_eq!(Arc::strong_count(&owner), 1);
}
fn pool() -> SharedPool {
    SharedPool::new(
        SessionId([91; 16]),
        PoolConfig {
            classes: vec![SizeClass {
                bytes: 4096,
                slots: 2,
            }],
            max_bytes: 8192,
            max_leases: 4,
        },
    )
    .unwrap()
}

fn transform(audio: bool) {
    transform_with_worker(audio, spawn());
}

#[cfg(all(target_os = "linux", feature = "test-linux-namespace"))]
#[test]
fn supervised_namespace_transfers_real_read_and_write_grants_without_staging() {
    use remotemedia_shared_memory::channel::NamespaceCommand;
    use std::path::Path;
    for audio in [false, true] {
        let owner = std::sync::Arc::new(());
        let mut command = NamespaceCommand::new(
            Path::new(env!("CARGO_BIN_EXE_remotemedia-worker-supervisor")),
            Path::new("/usr/bin/unshare"),
            env!("CARGO_BIN_EXE_shared-memory-test-peer"),
        )
        .unwrap();
        command.command_mut().arg("channel");
        let mut process = WorkerProcess::spawn_namespaced_with_resources(
            &mut command,
            remotemedia_shared_memory::channel::ProcessResources::new(owner.clone()),
        )
        .unwrap();
        assert_eq!(std::sync::Arc::strong_count(&owner), 2);
        let (sender, receiver) = process.take_channel().unwrap().split().unwrap();
        transform_with_worker(audio, (process, sender, receiver));
        assert_eq!(std::sync::Arc::strong_count(&owner), 1);
    }
}

fn transform_with_worker(audio: bool, worker: (WorkerProcess, Sender, Receiver)) {
    let pool = pool();
    let peer = PeerId(100);
    let (mut child, mut sender, mut receiver) = worker;
    send(&mut sender, Command::Hello(pool.session_id(), peer), None);
    assert!(
        matches!(receive(&mut receiver), Event::Hello(session, who) if session == pool.session_id() && who == peer)
    );
    let input = if audio {
        let mut loan = pool.allocate::<f32>(16).unwrap();
        for (i, v) in loan.iter_mut().enumerate() {
            *v = i as f32 * 0.5;
        }
        RuntimeData::Audio {
            samples: loan.freeze().into(),
            sample_rate: 48000,
            channels: 2,
            stream_id: Some("voice".into()),
            timestamp_us: Some(31),
            arrival_ts_us: None,
            metadata: None,
        }
    } else {
        let mut loan = pool.allocate::<u8>(16).unwrap();
        for (i, v) in loan.iter_mut().enumerate() {
            *v = i as u8;
        }
        RuntimeData::Binary(loan.freeze())
    };
    let delivery = pool
        .prepare_message(input, peer, TensorEncoding::Runtime)
        .unwrap();
    // SAFETY: this process owns the original child and binds the prepared file
    // and descriptor together on its private channel. Releases are validated below.
    let (message, file) = unsafe { delivery.into_transport() };
    let input_descriptor = message.buffer.clone().unwrap();
    send(
        &mut sender,
        Command::Input(message),
        Some((file.as_ref().unwrap(), HandleAccess::Read)),
    );
    drop(file);
    assert!(matches!(receive(&mut receiver), Event::Accepted));
    pool.accept(&input_descriptor).unwrap();
    let grant = if audio {
        pool.allocate_remote::<f32>(16, peer)
    } else {
        pool.allocate_remote::<u8>(16, peer)
    }
    .unwrap();
    let (file, write) = unsafe { grant.into_transport() };
    send(
        &mut sender,
        Command::Produce(write.clone()),
        Some((&file, HandleAccess::ReadWrite)),
    );
    drop(file);
    let Event::Published(published, metadata) = receive(&mut receiver) else {
        panic!("not published")
    };
    assert_eq!(published, write);
    let data = if audio {
        PayloadStorage::Audio(unsafe { pool.publish_remote::<f32>(&write) }.unwrap())
    } else {
        PayloadStorage::Bytes(unsafe { pool.publish_remote::<u8>(&write) }.unwrap())
    };
    let output = metadata.attach(data, TensorEncoding::Runtime).unwrap();
    match &output {
        RuntimeData::Audio {
            samples,
            stream_id,
            timestamp_us,
            ..
        } => {
            assert_eq!(
                samples.as_ref(),
                &(0..16).map(|i| i as f32 + 1.0).collect::<Vec<_>>()
            );
            assert_eq!(stream_id.as_deref(), Some("voice"));
            assert_eq!(*timestamp_us, Some(31));
        }
        RuntimeData::Binary(bytes) => assert_eq!(bytes.as_ref(), &(1..17).collect::<Vec<u8>>()),
        _ => unreachable!(),
    }
    let forwarded = pool
        .prepare_message(output.clone(), PeerId(101), TensorEncoding::Runtime)
        .unwrap();
    assert_eq!(
        forwarded.descriptor().buffer.as_ref().unwrap().allocation,
        write.allocation
    );
    drop(forwarded);
    assert!(pool.allocate::<u8>(1).is_err());
    send(&mut sender, Command::ReleaseInput, None);
    let Event::InputReleased(released) = receive(&mut receiver) else {
        panic!("not released")
    };
    assert_eq!(released, input_descriptor);
    unsafe { pool.release_remote(&released) }.unwrap();
    send(
        &mut sender,
        if audio {
            Command::ReleaseOutputAsync
        } else {
            Command::ReleaseOutput
        },
        None,
    );
    let Event::OutputReleased(released) = receive(&mut receiver) else {
        panic!("not released")
    };
    assert_eq!(released, write);
    unsafe { pool.release_producer(&released) }.unwrap();
    send(&mut sender, Command::Stop, None);
    assert!(child.wait().unwrap().success());
    assert!(receiver.receive().is_err()); // Original child-end handle is not retained by Command.
    pool.close();
    drop(child);
    assert!(pool
        .prepare_message(output.clone(), peer, TensorEncoding::Runtime)
        .is_err());
    drop(output);
}
#[test]
fn audio_metadata_and_direct_output_cross_private_worker_channel() {
    transform(true);
}
#[test]
fn binary_metadata_and_direct_output_cross_private_worker_channel() {
    transform(false);
}
#[test]
fn bounded_control_frames_and_worker_exit_do_not_reuse_channel() {
    let (mut child, mut sender, mut receiver) = spawn();
    send(
        &mut sender,
        Command::Hello(SessionId([92; 16]), PeerId(3)),
        None,
    );
    assert!(matches!(receive(&mut receiver), Event::Hello(..)));
    assert!(sender.send(&vec![0; MAX_CONTROL_BYTES + 1], None).is_err());
    for len in [0, 1, 4096, 200_000] {
        let bytes: Vec<_> = (0..len).map(|i| (i % 251) as u8).collect();
        send(&mut sender, Command::Echo(bytes.clone()), None);
        assert!(matches!(receive(&mut receiver), Event::Echo(value) if value == bytes));
    }
    send(&mut sender, Command::Crash, None);
    assert_eq!(child.wait().unwrap().code(), Some(23));
    assert!(receiver.receive().is_err());
    assert!(sender.send(b"after death", None).is_err());
}

#[test]
fn forged_reverse_handle_cannot_close_a_broker_handle() {
    use std::io::{Read, Seek, SeekFrom, Write};
    let (mut child, mut sender, mut receiver) = spawn();
    send(
        &mut sender,
        Command::Hello(SessionId([93; 16]), PeerId(4)),
        None,
    );
    assert!(matches!(receive(&mut receiver), Event::Hello(..)));
    let mut protected = tempfile::tempfile().unwrap();
    #[cfg(windows)]
    let raw = {
        use std::os::windows::io::AsRawHandle;
        protected.as_raw_handle() as usize as u64
    };
    #[cfg(target_os = "linux")]
    let raw = {
        use std::os::fd::AsRawFd;
        protected.as_raw_fd() as u64
    };
    send(&mut sender, Command::ForgedGrant(raw), None);
    assert!(receiver.receive().is_err());
    assert!(sender.send(b"cannot resume", None).is_err());
    child.terminate().unwrap();
    protected.write_all(b"still owned").unwrap();
    protected.seek(SeekFrom::Start(0)).unwrap();
    let mut value = String::new();
    protected.read_to_string(&mut value).unwrap();
    assert_eq!(value, "still owned");
}
