use remotemedia_shared_memory::{
    import, ElementType, PeerId, PoolConfig, SessionId, SharedMemoryError, SharedPool, SizeClass,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

fn pool(slots: usize, leases: usize) -> SharedPool {
    SharedPool::new(
        SessionId([7; 16]),
        PoolConfig {
            classes: vec![SizeClass { bytes: 4096, slots }],
            max_bytes: slots * 4096,
            max_leases: leases,
        },
    )
    .unwrap()
}

#[test]
fn exact_pool_membership_rejects_same_label_foreign_and_private_owners_without_leases() {
    let local = pool(2, 1);
    let foreign = pool(1, 1);
    assert_eq!(local.session_id(), foreign.session_id());
    let own = local.allocate::<u8>(16).unwrap().freeze();
    let foreign_owner = foreign.allocate::<u8>(16).unwrap().freeze();
    let empty = local.allocate::<f32>(0).unwrap().freeze();
    let grant = local.export(&own, PeerId(12)).unwrap();
    for _ in 0..32 {
        assert!(local.owns_buffer(&own.slice(3..9).unwrap()));
        assert!(local.owns_buffer(&empty));
        assert!(!local.owns_buffer(&foreign_owner));
        assert!(!local.owns_buffer(&remotemedia_types::SharedBuffer::from(Vec::<u8>::new())));
    }
    assert!(local.export(&own, PeerId(13)).is_err());
    drop(grant);
    assert!(local.export(&own, PeerId(13)).is_ok());
    local.close();
    assert!(local.owns_buffer(&own));
    assert!(local.owns_buffer(&empty));
}

#[test]
fn direct_audio_output_keeps_pointer_and_reuses_only_after_last_view() {
    let pool = pool(1, 4);
    let mut loan = pool.allocate::<f32>(128).unwrap();
    let pointer = loan.as_ptr();
    assert_eq!(loan.as_ref(), &[0.0; 128]);
    loan.fill(0.75);
    let buffer = loan.freeze();
    assert_eq!(buffer.as_ptr(), pointer);
    let retained = buffer.slice(2..4).unwrap();
    drop(buffer);
    assert!(matches!(
        pool.allocate::<u8>(1),
        Err(SharedMemoryError::Exhausted)
    ));
    assert_eq!(retained.as_slice(), &[0.75; 2]);
    drop(retained);
    assert_eq!(pool.allocate::<f32>(128).unwrap().as_ref(), &[0.0; 128]);
}

#[test]
fn pending_delivery_holds_storage_and_unsent_delivery_cancels() {
    let pool = pool(1, 1);
    let buffer = pool.allocate::<u8>(16).unwrap().freeze();
    let grant = pool.export(&buffer, PeerId(1)).unwrap();
    let first_id = grant.descriptor().allocation;
    assert!(matches!(
        pool.export(&buffer, PeerId(2)),
        Err(SharedMemoryError::Exhausted)
    ));
    drop(buffer);
    assert!(matches!(
        pool.allocate::<u8>(1),
        Err(SharedMemoryError::Exhausted)
    ));
    drop(grant); // Definitely never dispatched; safe cancellation.
    let next = pool.allocate::<u8>(16).unwrap().freeze();
    let next_grant = pool.export(&next, PeerId(1)).unwrap();
    assert_eq!(first_id.slot, next_grant.descriptor().allocation.slot);
    assert_eq!(
        first_id.generation + 1,
        next_grant.descriptor().allocation.generation
    );
}

#[test]
fn readonly_import_and_nested_forwarding_retain_same_generation() {
    let pool = pool(1, 8);
    let mut loan = pool.allocate::<f32>(16).unwrap();
    for (i, value) in loan.iter_mut().enumerate() {
        *value = i as f32;
    }
    let buffer = loan.freeze();
    let grant = pool
        .export(&buffer.slice(2..14).unwrap(), PeerId(1))
        .unwrap();
    // SAFETY: this test owns both sides and maps read-only while retaining leases.
    let (file, descriptor) = unsafe { grant.into_transport() };
    pool.accept(&descriptor).unwrap();
    let release_pool = pool.clone();
    let release_descriptor = descriptor.clone();
    let releases = Arc::new(AtomicUsize::new(0));
    let released = releases.clone();
    // SAFETY: the file is from this pool's authenticated grant; callback is the
    // only release path and import unmaps before invoking it.
    let input = unsafe {
        import::<f32>(file, descriptor.clone(), SessionId([7; 16]), move || {
            release_pool.release_remote(&release_descriptor).unwrap();
            released.fetch_add(1, Ordering::SeqCst);
        })
    }
    .unwrap();
    assert_eq!(
        input.as_slice(),
        &(2..14).map(|x| x as f32).collect::<Vec<_>>()
    );
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::Memory::{
            VirtualQuery, MEMORY_BASIC_INFORMATION, PAGE_READONLY,
        };
        let mut information: MEMORY_BASIC_INFORMATION = unsafe { std::mem::zeroed() };
        // SAFETY: input is a live mapped address; output points to initialized
        // storage of the API's required structure size.
        let queried = unsafe {
            VirtualQuery(
                input.as_ptr().cast(),
                &mut information,
                std::mem::size_of::<MEMORY_BASIC_INFORMATION>(),
            )
        };
        assert_ne!(queried, 0);
        assert_eq!(information.Protect, PAGE_READONLY);
    }
    #[cfg(target_os = "linux")]
    {
        let address = input.as_ptr() as usize;
        let maps = std::fs::read_to_string("/proc/self/maps").unwrap();
        let permissions = maps
            .lines()
            .find_map(|line| {
                let mut fields = line.split_whitespace();
                let (start, end) = fields.next()?.split_once('-')?;
                let start = usize::from_str_radix(start, 16).ok()?;
                let end = usize::from_str_radix(end, 16).ok()?;
                (start <= address && address < end).then(|| fields.next().unwrap())
            })
            .unwrap();
        assert_eq!(
            permissions, "r--s",
            "consumer mapping must be read-only and shared"
        );
    }
    let slice = input.slice(3..5).unwrap();
    let forwarded = pool.export(&slice, PeerId(2)).unwrap();
    assert_eq!(forwarded.descriptor().allocation, descriptor.allocation);
    assert_ne!(forwarded.descriptor().lease, descriptor.lease);
    assert_eq!(forwarded.descriptor().offset, 5 * 4);
    assert_eq!(forwarded.descriptor().byte_len, 2 * 4);
    drop(forwarded); // Never sent.
    drop(buffer);
    drop(input);
    pool.close();
    assert_eq!(releases.load(Ordering::SeqCst), 0);
    assert_eq!(slice.as_slice(), &[5.0, 6.0]);
    drop(slice);
    assert_eq!(releases.load(Ordering::SeqCst), 1);
    assert!(matches!(
        pool.allocate::<u8>(1),
        Err(SharedMemoryError::Closed)
    ));
}

#[test]
fn forged_ranges_recipients_and_duplicate_releases_are_rejected() {
    let pool = pool(1, 4);
    let buffer = pool.allocate::<f32>(8).unwrap().freeze();
    let grant = pool.export(&buffer, PeerId(10)).unwrap();
    let descriptor = grant.descriptor().clone();
    let mut forged = descriptor.clone();
    forged.recipient = PeerId(20);
    assert!(pool.accept(&forged).is_err());
    forged = descriptor.clone();
    forged.offset = u64::MAX;
    assert!(pool.accept(&forged).is_err());
    forged = descriptor.clone();
    forged.session = SessionId([8; 16]);
    assert!(pool.accept(&forged).is_err());
    assert!(pool.forward(&descriptor, PeerId(20), 0, 4).is_err()); // Pending.
    pool.accept(&descriptor).unwrap();
    assert!(pool.forward(&descriptor, PeerId(20), 1, 4).is_err()); // Unaligned.
    assert!(pool.forward(&descriptor, PeerId(20), 0, 36).is_err()); // Widening.
    drop(grant); // No handle has escaped.
                 // SAFETY: no reader was created. Duplicate notification must not reclaim
                 // the still-live local buffer or a later generation.
    assert!(unsafe { pool.release_remote(&descriptor) }.is_err());
    drop(buffer);
    let next = pool.allocate::<f32>(8).unwrap().freeze();
    assert!(pool.accept(&descriptor).is_err());
    assert!(unsafe { pool.release_remote(&descriptor) }.is_err());
    assert!(matches!(
        pool.allocate::<u8>(1),
        Err(SharedMemoryError::Exhausted)
    ));
    drop(next);
}

#[test]
fn failed_import_releases_without_exposing_a_view() {
    let pool = pool(1, 1);
    let buffer = pool.allocate::<u8>(16).unwrap().freeze();
    let grant = pool.export(&buffer, PeerId(1)).unwrap();
    let (file, mut descriptor) = unsafe { grant.into_transport() };
    pool.accept(&descriptor).unwrap();
    let original = descriptor.clone();
    let released_pool = pool.clone();
    descriptor.element = ElementType::F32Le;
    descriptor.offset = 1;
    let result = unsafe {
        import::<f32>(file, descriptor, SessionId([7; 16]), move || {
            // This callback runs only after import has abandoned all mapped views.
            released_pool.release_remote(&original).unwrap();
        })
    };
    assert!(matches!(result, Err(SharedMemoryError::InvalidLayout)));
    drop(buffer);
    assert!(pool.allocate::<u8>(16).is_ok());
}

#[test]
fn budgets_size_classes_empty_views_and_overflow_are_checked() {
    let config = PoolConfig {
        classes: vec![
            SizeClass { bytes: 8, slots: 1 },
            SizeClass {
                bytes: 64,
                slots: 1,
            },
        ],
        max_bytes: 72,
        max_leases: 2,
    };
    let pool = SharedPool::new(SessionId([1; 16]), config.clone()).unwrap();
    let small = pool.allocate::<u8>(8).unwrap();
    let large = pool.allocate::<u8>(64).unwrap();
    assert!(pool.allocate::<u8>(0).is_err()); // Empty views still own a slot.
    drop(small);
    assert!(pool.allocate::<u8>(0).unwrap().freeze().is_empty());
    drop(large);
    assert!(matches!(
        pool.allocate::<f32>(usize::MAX),
        Err(SharedMemoryError::InvalidLayout)
    ));
    assert!(SharedPool::new(
        SessionId([1; 16]),
        PoolConfig {
            max_bytes: 71,
            ..config
        }
    )
    .is_err());
}

#[test]
fn dispatched_but_unacknowledged_lease_is_not_reclaimed() {
    let pool = pool(1, 2);
    let buffer = pool.allocate::<u8>(8).unwrap().freeze();
    let grant = pool.export(&buffer, PeerId(90)).unwrap();
    // SAFETY: simulates a transport losing its handle before delivery; no peer
    // reads it, but the broker cannot infer non-delivery from sender destruction.
    let (file, descriptor) = unsafe { grant.into_transport() };
    drop(file);
    drop(buffer);
    assert!(matches!(
        pool.allocate::<u8>(1),
        Err(SharedMemoryError::Exhausted)
    ));
    // SAFETY: this test has independently established no delivery/no readers.
    unsafe {
        pool.release_remote(&descriptor).unwrap();
    }
    assert!(pool.accept(&descriptor).is_err());
    assert!(pool.allocate::<u8>(8).is_ok());
}

#[test]
fn concurrent_publish_import_and_release_never_reuse_live_storage() {
    let pool = pool(8, 16);
    let threads: Vec<_> = (0..8)
        .map(|thread| {
            let pool = pool.clone();
            std::thread::spawn(move || {
                for iteration in 0..100 {
                    let mut loan = pool.allocate::<u8>(128).unwrap();
                    assert!(loan.iter().all(|&v| v == 0));
                    let value = (thread * 17 + iteration) as u8;
                    loan.fill(value);
                    let buffer = loan.freeze();
                    let grant = pool.export(&buffer, PeerId(thread + 1)).unwrap();
                    let (file, descriptor) = unsafe { grant.into_transport() };
                    pool.accept(&descriptor).unwrap();
                    let release_pool = pool.clone();
                    let release_descriptor = descriptor.clone();
                    // SAFETY: this thread owns the grant and releases it only after
                    // the final imported view unmaps. Other threads use other leases.
                    let input = unsafe {
                        import::<u8>(file, descriptor, SessionId([7; 16]), move || {
                            release_pool.release_remote(&release_descriptor).unwrap();
                        })
                    }
                    .unwrap();
                    let retained = input.slice(4..96).unwrap();
                    drop(buffer);
                    drop(input);
                    std::thread::yield_now();
                    assert!(retained.iter().all(|&v| v == value));
                }
            })
        })
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
}
