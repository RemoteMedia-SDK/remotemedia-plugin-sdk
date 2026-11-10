use remotemedia_types::{BufferLoan, SharedBuffer};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

struct Value {
    value: u8,
    releases: Arc<AtomicUsize>,
}
impl Drop for Value {
    fn drop(&mut self) {
        self.releases.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn every_portable_storage_kind_retains_initialized_nested_views_across_threads() {
    for kind in ["vec", "arc", "owner", "loan"] {
        let releases = Arc::new(AtomicUsize::new(0));
        let values: Vec<_> = (0..8)
            .map(|value| Value {
                value,
                releases: releases.clone(),
            })
            .collect();
        let mut pointer = values.as_ptr() as usize;
        let data = match kind {
            "vec" => SharedBuffer::from(values),
            "arc" => {
                // Prepare Arc storage before measuring the buffer wrapper;
                // converting Vec to Arc is an explicit allocation/copy boundary.
                let values: Arc<[Value]> = values.into();
                pointer = values.as_ptr() as usize;
                SharedBuffer::from(values)
            }
            "owner" => SharedBuffer::from_owner(values),
            "loan" => BufferLoan::from_owner(values).freeze(),
            _ => unreachable!(),
        };
        assert_eq!(data.as_ptr() as usize, pointer, "{kind}");
        assert_eq!(pointer % std::mem::align_of::<Value>(), 0);
        assert!(data.slice(8..8).unwrap().is_empty());
        assert!(data.slice(usize::MAX..usize::MAX).is_err());
        assert!(data.slice(0..9).is_err());
        let retained = data.slice(2..6).unwrap().slice(1..3).unwrap();
        assert!(data.shares_storage(&retained));
        drop(data);
        assert_eq!(releases.load(Ordering::SeqCst), 0);
        std::thread::spawn(move || {
            assert_eq!(retained.storage_range(), 3..5);
            assert_eq!(
                retained.as_ptr() as usize,
                pointer + 3 * std::mem::size_of::<Value>()
            );
            assert_eq!(
                retained.iter().map(|item| item.value).collect::<Vec<_>>(),
                vec![3, 4]
            );
        })
        .join()
        .unwrap();
        assert_eq!(releases.load(Ordering::SeqCst), 8, "{kind}");
    }
}
