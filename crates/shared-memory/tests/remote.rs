use remotemedia_shared_memory::{
    import, import_writer, writer_descriptor, PeerId, PoolConfig, SessionId, SharedMemoryError,
    SharedPool, SizeClass,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn pool() -> SharedPool {
    SharedPool::new(
        SessionId([81; 16]),
        PoolConfig {
            classes: vec![SizeClass {
                bytes: 4096,
                slots: 1,
            }],
            max_bytes: 4096,
            max_leases: 2,
        },
    )
    .unwrap()
}

#[test]
fn undelivered_and_abandoned_writers_recycle_initialized_storage() {
    let pool = pool();
    let grant = pool.allocate_remote::<f32>(8, PeerId(1)).unwrap();
    let first = grant.descriptor().allocation;
    assert!(pool.allocate::<u8>(1).is_err());
    drop(grant);
    let grant = pool.allocate_remote::<f32>(8, PeerId(1)).unwrap();
    assert!(grant.descriptor().allocation.generation > first.generation);
    // SAFETY: this harness owns the only grant, binds it to its descriptor, and
    // acknowledges only the imported owner's final unmap.
    let (file, descriptor) = unsafe { grant.into_transport() };
    let release_pool = pool.clone();
    let release_descriptor = descriptor.clone();
    let mut loan = unsafe {
        import_writer::<f32>(
            file,
            descriptor.clone(),
            pool.session_id(),
            PeerId(1),
            move || {
                release_pool.release_producer(&release_descriptor).unwrap();
            },
        )
    }
    .unwrap();
    assert_eq!(loan.as_ref(), &[0.0; 8]);
    loan.fill(9.0);
    drop(loan);
    assert!(matches!(
        unsafe { pool.publish_remote::<f32>(&descriptor) },
        Err(SharedMemoryError::StaleAllocation)
    ));
    assert_eq!(pool.allocate::<f32>(8).unwrap().as_ref(), &[0.0; 8]);
    assert!(pool.allocate_remote::<f32>(usize::MAX, PeerId(1)).is_err());
}

#[test]
fn worker_freeze_host_publication_and_forwarded_slices_retain_independent_owners() {
    let pool = pool();
    let grant = pool.allocate_remote::<f32>(8, PeerId(1)).unwrap();
    let (file, descriptor) = unsafe { grant.into_transport() };
    let release_pool = pool.clone();
    let release_descriptor = descriptor.clone();
    let releases = Arc::new(AtomicUsize::new(0));
    let count = releases.clone();
    let mut loan = unsafe {
        import_writer::<f32>(
            file,
            descriptor.clone(),
            pool.session_id(),
            PeerId(1),
            move || {
                release_pool.release_producer(&release_descriptor).unwrap();
                count.fetch_add(1, Ordering::SeqCst);
            },
        )
    }
    .unwrap();
    let address = loan.as_ptr();
    for (i, value) in loan.iter_mut().enumerate() {
        *value = i as f32 * 0.5;
    }
    let worker = loan.freeze();
    assert_eq!(worker.as_ptr(), address);
    assert_eq!(writer_descriptor(&worker), Some(&descriptor));
    assert!(worker.clone().try_into_loan().is_err());
    // SAFETY: consuming freeze revoked the only writable Rust access path.
    let host = unsafe { pool.publish_remote::<f32>(&descriptor) }.unwrap();
    assert!(pool.owns_buffer(&host));
    assert!(!pool.owns_buffer(&worker));
    assert_eq!(host.as_ref(), worker.as_ref());
    assert!(unsafe { pool.publish_remote::<f32>(&descriptor) }.is_err());
    let slice = host.slice(2..6).unwrap();
    assert!(pool.owns_buffer(&slice));
    let delivery = pool.export(&slice, PeerId(2)).unwrap();
    assert_eq!(delivery.descriptor().allocation, descriptor.allocation);
    assert_eq!(delivery.descriptor().offset, 8);
    let (file, read_descriptor) = unsafe { delivery.into_transport() };
    pool.accept(&read_descriptor).unwrap();
    let release_pool = pool.clone();
    let final_descriptor = read_descriptor.clone();
    let reader = unsafe {
        import::<f32>(file, read_descriptor, pool.session_id(), move || {
            release_pool.release_remote(&final_descriptor).unwrap();
        })
    }
    .unwrap();
    assert!(!pool.owns_buffer(&reader));
    drop(host);
    drop(slice);
    drop(worker);
    assert_eq!(releases.load(Ordering::SeqCst), 1);
    assert!(pool.allocate::<u8>(1).is_err());
    assert_eq!(reader.as_ref(), &[1.0, 1.5, 2.0, 2.5]);
    drop(reader);
    assert!(pool.allocate::<u8>(1).is_ok());
}

#[test]
fn changed_identity_layout_duplicate_release_and_closed_publication_fail() {
    let pool = pool();
    let grant = pool.allocate_remote::<u8>(8, PeerId(4)).unwrap();
    let (file, descriptor) = unsafe { grant.into_transport() };
    let mut wrong_peer = descriptor.clone();
    wrong_peer.producer = PeerId(9);
    let mut wrong_generation = descriptor.clone();
    wrong_generation.allocation.generation += 1;
    let mut wrong_version = descriptor.clone();
    wrong_version.version += 1;
    for wrong in [wrong_peer, wrong_generation, wrong_version] {
        assert!(unsafe { pool.publish_remote::<u8>(&wrong) }.is_err());
        assert!(unsafe { pool.release_producer(&wrong) }.is_err());
    }
    let mut wrong = descriptor.clone();
    wrong.byte_len += 1;
    assert!(unsafe { pool.publish_remote::<u8>(&wrong) }.is_err());
    wrong = descriptor.clone();
    wrong.session = SessionId([0; 16]);
    assert!(unsafe { pool.publish_remote::<u8>(&wrong) }.is_err());
    assert!(unsafe { pool.publish_remote::<f32>(&descriptor) }.is_err());
    drop(file); // No import was made; this grant has no remote aliases.
    pool.close();
    assert!(matches!(
        unsafe { pool.publish_remote::<u8>(&descriptor) },
        Err(SharedMemoryError::Closed)
    ));
    unsafe { pool.release_producer(&descriptor) }.unwrap();
    assert!(unsafe { pool.release_producer(&descriptor) }.is_err());
}

#[test]
fn failed_import_releases_exactly_once_and_empty_writer_freezes() {
    let pool = pool();
    let grant = pool.allocate_remote::<u8>(0, PeerId(1)).unwrap();
    let (file, descriptor) = unsafe { grant.into_transport() };
    let release_pool = pool.clone();
    let release_descriptor = descriptor.clone();
    let count = Arc::new(AtomicUsize::new(0));
    let released = count.clone();
    assert!(unsafe {
        import_writer::<f32>(file, descriptor, pool.session_id(), PeerId(1), move || {
            release_pool.release_producer(&release_descriptor).unwrap();
            released.fetch_add(1, Ordering::SeqCst);
        })
    }
    .is_err());
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let grant = pool.allocate_remote::<u8>(0, PeerId(1)).unwrap();
    let (file, descriptor) = unsafe { grant.into_transport() };
    let release_pool = pool.clone();
    let release_descriptor = descriptor.clone();
    let worker = unsafe {
        import_writer::<u8>(
            file,
            descriptor.clone(),
            pool.session_id(),
            PeerId(1),
            move || {
                release_pool.release_producer(&release_descriptor).unwrap();
            },
        )
    }
    .unwrap()
    .freeze();
    let host = unsafe { pool.publish_remote::<u8>(&descriptor) }.unwrap();
    assert!(worker.is_empty() && host.is_empty());
    pool.close();
    drop(worker);
    assert!(host.is_empty());
    drop(host);
}
