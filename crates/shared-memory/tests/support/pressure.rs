use super::*;

#[test]
fn repeated_two_worker_pressure_preserves_slow_reader_and_reuses_generations() {
    let pool = pool(1);
    let (mut first, first_handle) = spawn(&pool);
    let (mut second, second_handle) = spawn(&pool);
    let mut previous = None;
    for round in 0..128_u8 {
        let input = bytes(&pool, &[round; 64]);
        let RuntimeData::Binary(ref buffer) = input else {
            unreachable!()
        };
        let allocation = pool
            .export(buffer, PeerId(900))
            .unwrap()
            .descriptor()
            .allocation;
        if let Some(old) = previous {
            assert!(
                allocation.generation > old,
                "a new publication reused an old generation"
            );
        }
        previous = Some(allocation.generation);
        let call = start(
            &first_handle,
            CallKind::Process,
            "echo",
            vec![("input".into(), input)],
        );
        let first_view = output(&mut first, call).into_data();
        complete(&mut first, call).unwrap();
        let call = start(
            &second_handle,
            CallKind::Process,
            "echo",
            vec![("input".into(), first_view.clone())],
        );
        let delayed = output(&mut second, call);
        assert!(pool.allocate::<u8>(1).is_err());

        // The second worker is awaiting this output's acknowledgement. Its
        // independent RPC must still complete while every payload slot is held.
        let rpc = start(&second_handle, CallKind::Rpc, "rpc", vec![]);
        assert!(matches!(
            output(&mut second, rpc).into_data(),
            RuntimeData::Json(_)
        ));
        complete(&mut second, rpc).unwrap();
        let second_view = delayed.into_data();
        complete(&mut second, call).unwrap();
        assert_eq!(binary(&first_view).as_ptr(), binary(&second_view).as_ptr());
        assert_eq!(binary(&second_view), &[round; 64]);
        let RuntimeData::Binary(buffer) = second_view else {
            unreachable!()
        };
        assert_eq!(
            pool.export(&buffer, PeerId(901))
                .unwrap()
                .descriptor()
                .allocation,
            allocation
        );
        let slow = buffer.slice(7..23).unwrap();
        drop((first_view, buffer));
        assert!(pool.allocate::<u8>(1).is_err(), "slow reader lost its slot");
        assert_eq!(&*slow, &[round; 16]);
        drop(slow);
        // The next round can only begin if both workers released their input
        // leases and the final host view released this same backing slot.
    }
    first_handle.close();
    second_handle.close();
    assert!(pool.allocate::<u8>(4096).is_ok());
}
