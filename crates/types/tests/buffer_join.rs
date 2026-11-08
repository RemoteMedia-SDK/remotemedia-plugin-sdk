use remotemedia_types::{BufferBudget, BufferLoan, ForwardingScope, RuntimeData, SharedBuffer};
use std::sync::Arc;

#[test]
fn adjacent_views_join_without_copy_for_every_storage_kind() {
    let arc = Arc::new(vec![1u8, 2, 3, 4]);
    let buffers: Vec<SharedBuffer<u8>> = vec![
        arc.clone().into(),
        Arc::<[u8]>::from([1, 2, 3, 4]).into(),
        SharedBuffer::from_owner(vec![1u8, 2, 3, 4]),
        BufferLoan::from_owner(vec![1u8, 2, 3, 4]).freeze(),
    ];
    assert_eq!(buffers[0].as_ptr(), arc.as_ptr());
    for source in buffers {
        let pointer = source.as_ptr();
        let first = source.slice(0..2).unwrap();
        let next = source.slice(2..4).unwrap();
        let joined = first.try_join(&next).unwrap();
        assert_eq!(joined.as_ptr(), pointer);
        assert!(joined.shares_storage(&source));
        assert_eq!(joined.storage_range(), 0..4);
        assert!(next.try_join(&first).is_none());
        assert!(first.try_join(&source).is_none());
        assert!(first.try_join(&SharedBuffer::from(vec![3, 4])).is_none());
        drop((first, next, source));
        assert_eq!(&*joined, &[1, 2, 3, 4]);
    }
    assert_eq!(Arc::strong_count(&arc), 1);
}

#[test]
fn joining_retains_credit_and_rejects_different_budget_provenance() {
    let source: SharedBuffer<u8> = vec![1, 2, 3, 4].into();
    let a = BufferBudget::new(1, 4);
    let b = BufferBudget::new(1, 4);
    let RuntimeData::Binary(first) = a.admit(RuntimeData::Binary(source.clone())).unwrap() else {
        unreachable!()
    };
    let RuntimeData::Binary(second) = b.admit(RuntimeData::Binary(source)).unwrap() else {
        unreachable!()
    };
    assert!(first.shares_storage(&second));
    assert!(first
        .slice(0..2)
        .unwrap()
        .try_join(&second.slice(2..4).unwrap())
        .is_none());
    let joined = first
        .slice(0..2)
        .unwrap()
        .try_join(&first.slice(2..4).unwrap())
        .unwrap();
    drop((first, second));
    assert_eq!(a.usage().owners, 1);
    assert_eq!(b.usage().owners, 0);
    drop(joined);
    assert_eq!(a.usage().owners, 0);
}

#[test]
fn joining_cannot_widen_forwarding_admission() {
    let source: SharedBuffer<u8> = vec![1, 2, 3, 4].into();
    let scope = ForwardingScope::default();
    let RuntimeData::Binary(accepted) = scope.accept(RuntimeData::Binary(source.clone())) else {
        unreachable!()
    };
    assert!(accepted
        .slice(0..2)
        .unwrap()
        .try_join(&source.slice(2..4).unwrap())
        .is_none());
    let joined = accepted
        .slice(0..2)
        .unwrap()
        .try_join(&accepted.slice(2..4).unwrap())
        .unwrap();
    assert!(scope.accepts_output(&RuntimeData::Binary(joined)));
}
