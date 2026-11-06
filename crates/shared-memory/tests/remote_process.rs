#![cfg(feature = "test-peer")]
use remotemedia_shared_memory::{
    PeerId, PoolConfig, SessionId, SharedPool, SizeClass, WriteDescriptor,
};
use std::{
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
};

struct Producer {
    child: Child,
    pool: SharedPool,
    descriptor: WriteDescriptor,
}
impl Drop for Producer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        if self.child.wait().is_ok() {
            // SAFETY: original child handle confirmed termination, not a PID lookup.
            unsafe { self.pool.release_dead_peer(self.descriptor.producer) };
        }
    }
}

fn exercise(publish: bool, crash: bool) {
    exercise_command(
        publish,
        crash,
        Command::new(env!("CARGO_BIN_EXE_shared-memory-test-peer")),
    );
}

fn exercise_command(publish: bool, crash: bool, mut command: Command) {
    let pool = SharedPool::new(
        SessionId([31; 16]),
        PoolConfig {
            classes: vec![SizeClass {
                bytes: 4096,
                slots: 1,
            }],
            max_bytes: 4096,
            max_leases: 4,
        },
    )
    .unwrap();
    let grant = pool.allocate_remote::<f32>(16, PeerId(70)).unwrap();
    // SAFETY: only our child receives this handle; the parent verifies unmap or
    // original-process termination before releasing this exact reservation.
    let (file, descriptor) = unsafe { grant.into_transport() };
    let child = command
        .arg("produce")
        .stdin(Stdio::piped())
        .stdout(Stdio::from(file))
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut producer = Producer {
        child,
        pool: pool.clone(),
        descriptor: descriptor.clone(),
    };
    let mut commands = producer.child.stdin.take().unwrap();
    let mut events = BufReader::new(producer.child.stderr.take().unwrap());
    writeln!(commands, "{}", serde_json::to_string(&descriptor).unwrap()).unwrap();
    commands.flush().unwrap();
    let mut event = String::new();
    events.read_line(&mut event).unwrap();
    assert_eq!(event.trim(), "writing");
    assert!(pool.allocate::<u8>(1).is_err());
    if !publish {
        writeln!(commands, "crash").unwrap();
        commands.flush().unwrap();
        assert_eq!(producer.child.wait().unwrap().code(), Some(23));
        unsafe { pool.release_dead_peer(descriptor.producer) };
        assert_eq!(pool.allocate::<f32>(16).unwrap().as_ref(), &[0.0; 16]);
        return;
    }
    writeln!(commands, "publish").unwrap();
    commands.flush().unwrap();
    event.clear();
    events.read_line(&mut event).unwrap();
    assert_eq!(event.trim(), "published");
    // SAFETY: the trusted child consumed its loan and retains only frozen views.
    let host = unsafe { pool.publish_remote::<f32>(&descriptor) }.unwrap();
    let delivery = pool
        .export(&host.slice(2..10).unwrap(), PeerId(71))
        .unwrap();
    assert_eq!(delivery.descriptor().allocation, descriptor.allocation);
    assert_eq!(delivery.descriptor().offset, 8);
    drop(delivery); // Unsent read lease must not cancel producer ownership.
    assert_eq!(
        host.as_ref(),
        &(0..16).map(|i| i as f32 * 0.5).collect::<Vec<_>>()
    );
    // Exercise both ownership directions: normal completion retains only the
    // worker's slice; a crash leaves the host's independent owner alive.
    let host = if crash {
        Some(host)
    } else {
        drop(host);
        assert!(pool.allocate::<u8>(1).is_err());
        None
    };
    writeln!(commands, "{}", if crash { "crash" } else { "release" }).unwrap();
    commands.flush().unwrap();
    if crash {
        assert_eq!(producer.child.wait().unwrap().code(), Some(23));
        unsafe { pool.release_dead_peer(descriptor.producer) };
    } else {
        event.clear();
        events.read_line(&mut event).unwrap();
        assert_eq!(event.trim(), "released");
        unsafe { pool.release_producer(&descriptor) }.unwrap();
        assert!(producer.child.wait().unwrap().success());
    }
    if let Some(host) = &host {
        assert!(pool.allocate::<u8>(1).is_err());
        assert_eq!(host[15], 7.5);
    }
    drop(host);
    let next = pool.allocate_remote::<f32>(16, PeerId(72)).unwrap();
    assert!(next.descriptor().allocation.generation > descriptor.allocation.generation);
}

#[test]
fn worker_writes_shared_output_and_retains_views_until_final_release() {
    exercise(true, false);
}

#[test]
fn worker_death_before_and_after_publication_reclaims_only_its_ownership() {
    exercise(false, true);
    exercise(true, true);
}

#[cfg(all(target_os = "linux", feature = "test-linux-namespace"))]
#[test]
fn namespace_producer_preserves_shared_storage_and_retained_view_ownership() {
    for (publish, crash) in [(false, true), (true, false), (true, true)] {
        let mut command = Command::new("unshare");
        command
            .args([
                "--user",
                "--map-current-user",
                "--pid",
                "--fork",
                "--kill-child=SIGKILL",
                "--",
            ])
            .arg(env!("CARGO_BIN_EXE_shared-memory-test-peer"));
        exercise_command(publish, crash, command);
    }
}
