#![cfg(feature = "test-peer")]
use remotemedia_shared_memory::{
    BufferDescriptor, Delivery, PeerId, PoolConfig, SessionId, SharedMemoryError, SharedPool,
    SizeClass,
};
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStderr, ChildStdin, Command, Stdio};

struct Peer {
    child: Child,
    commands: ChildStdin,
    events: BufReader<ChildStderr>,
    descriptor: BufferDescriptor,
    pool: SharedPool,
    released: bool,
}

impl Peer {
    fn spawn(pool: &SharedPool, delivery: Delivery) -> Self {
        // SAFETY: only our test worker receives the handle, and it never writes
        // it. The parent keeps the lease until final release/verified exit.
        let (file, descriptor) = unsafe { delivery.into_transport() };
        let mut child = Command::new(env!("CARGO_BIN_EXE_shared-memory-test-peer"))
            .stdin(Stdio::piped())
            .stdout(Stdio::from(file))
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let mut commands = child.stdin.take().unwrap();
        let events = BufReader::new(child.stderr.take().unwrap());
        writeln!(commands, "{}", serde_json::to_string(&descriptor).unwrap()).unwrap();
        commands.flush().unwrap();
        let mut peer = Self {
            child,
            commands,
            events,
            descriptor,
            pool: pool.clone(),
            released: false,
        };
        peer.expect("ready");
        pool.accept(&peer.descriptor).unwrap();
        peer
    }

    fn expect(&mut self, expected: &str) {
        let mut line = String::new();
        self.events.read_line(&mut line).unwrap();
        assert_eq!(line.trim(), expected, "worker {}", self.child.id());
    }

    fn finish(mut self, crash: bool) {
        writeln!(self.commands, "{}", if crash { "crash" } else { "release" }).unwrap();
        self.commands.flush().unwrap();
        if !crash {
            self.expect("released");
        }
        let status = self.child.wait().unwrap();
        if crash {
            assert_eq!(status.code(), Some(23));
            // SAFETY: wait() verified this original child's termination; the
            // test assigned a unique process incarnation, never a reused PID.
            unsafe {
                self.pool.release_dead_peer(self.descriptor.recipient);
            }
        } else {
            assert!(status.success());
            // SAFETY: worker unmapped every view before emitting released.
            unsafe {
                self.pool.release_remote(&self.descriptor).unwrap();
            }
        }
        self.released = true;
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.child.kill();
            if self.child.wait().is_ok() {
                // SAFETY: this exact test worker has terminated; cleanup also
                // runs on assertion failures so no test process is orphaned.
                unsafe {
                    self.pool.release_dead_peer(self.descriptor.recipient);
                }
            }
        }
    }
}

fn pool() -> SharedPool {
    SharedPool::new(
        SessionId([42; 16]),
        PoolConfig {
            classes: vec![SizeClass {
                bytes: 4096,
                slots: 1,
            }],
            max_bytes: 4096,
            max_leases: 4,
        },
    )
    .unwrap()
}

#[test]
fn binary_slice_is_shared_across_real_processes() {
    let pool = pool();
    let mut loan = pool.allocate::<u8>(1024).unwrap();
    for (i, value) in loan.iter_mut().enumerate() {
        *value = (i % 251) as u8;
    }
    let buffer = loan.freeze().slice(16..1024).unwrap();
    let first = Peer::spawn(&pool, pool.export(&buffer, PeerId(30)).unwrap());
    let descriptor = first.descriptor.clone();
    drop(buffer);
    let second = Peer::spawn(
        &pool,
        pool.forward(&descriptor, PeerId(40), 16, 512).unwrap(),
    );
    first.finish(false);
    second.finish(false);
    assert!(pool.allocate::<u8>(1024).is_ok());
}

#[test]
fn cross_process_forwarding_keeps_payload_without_host_rematerialization() {
    let pool = pool();
    let mut loan = pool.allocate::<f32>(128).unwrap();
    for (i, value) in loan.iter_mut().enumerate() {
        *value = i as f32 * 0.5;
    }
    let pointer = loan.as_ptr();
    let buffer = loan.freeze();
    assert_eq!(buffer.as_ptr(), pointer);
    let first = Peer::spawn(&pool, pool.export(&buffer, PeerId(1)).unwrap());
    let original = first.descriptor.clone();
    drop(buffer);
    // No host import, payload serialization, or intermediate buffer: the broker
    // forwards only a descriptor plus a duplicate handle for the same allocation.
    let forwarded = pool.forward(&original, PeerId(2), 16, 128).unwrap();
    assert_eq!(forwarded.descriptor().allocation, original.allocation);
    assert_ne!(forwarded.descriptor().lease, original.lease);
    let second = Peer::spawn(&pool, forwarded);
    assert!(matches!(
        pool.allocate::<u8>(1),
        Err(SharedMemoryError::Exhausted)
    ));
    first.finish(false);
    assert!(matches!(
        pool.allocate::<u8>(1),
        Err(SharedMemoryError::Exhausted)
    ));
    second.finish(false);
    let next = pool.allocate::<u8>(32).unwrap().freeze();
    let next = pool.export(&next, PeerId(3)).unwrap();
    assert_eq!(
        next.descriptor().allocation.generation,
        original.allocation.generation + 1
    );
    assert!(pool.accept(&original).is_err());
}

#[test]
fn crashed_recipient_reclaims_only_its_lease_and_survivor_outlives_close() {
    let pool = pool();
    let mut loan = pool.allocate::<f32>(128).unwrap();
    for (i, value) in loan.iter_mut().enumerate() {
        *value = i as f32 * 0.5;
    }
    let buffer = loan.freeze();
    let crashed = Peer::spawn(&pool, pool.export(&buffer, PeerId(10)).unwrap());
    let survivor = Peer::spawn(&pool, pool.export(&buffer, PeerId(20)).unwrap());
    drop(buffer);
    crashed.finish(true);
    assert!(matches!(
        pool.allocate::<u8>(1),
        Err(SharedMemoryError::Exhausted)
    ));
    pool.close();
    survivor.finish(false); // Reads retained slice after sibling death and close.
    assert!(matches!(
        pool.allocate::<u8>(1),
        Err(SharedMemoryError::Closed)
    ));
}
