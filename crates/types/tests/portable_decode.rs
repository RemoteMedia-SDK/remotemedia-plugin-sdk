use remotemedia_types::{
    AudioSamples, CopyContext, CopyCounts, CopyPolicy, CopyReason, SharedBuffer,
};
use serde::{
    de::{self, DeserializeSeed, IntoDeserializer, SeqAccess},
    Deserialize, Deserializer,
};
use std::sync::{Arc, Barrier};

fn counts(context: &CopyContext) -> CopyCounts {
    context
        .report()
        .entries
        .into_iter()
        .find(|(reason, _)| *reason == CopyReason::PortableDeserialization)
        .unwrap()
        .1
}

#[allow(dead_code)]
struct NeverDecode(u8);
impl<'de> Deserialize<'de> for NeverDecode {
    fn deserialize<D: Deserializer<'de>>(_: D) -> Result<Self, D::Error> {
        panic!("payload element decoder invoked");
    }
}

#[test]
fn strict_rejects_before_element_decoder_and_accepts_empty_buffers() {
    let context = CopyContext::new(CopyPolicy::RequireZeroCopy, "strict.decoder");
    let result = context.deserialize::<SharedBuffer<NeverDecode>, _>(
        &mut serde_json::Deserializer::from_str("[1]"),
    );
    let error = result.err().unwrap();
    assert!(error.to_string().contains("strict.decoder"));
    assert_eq!(
        counts(&context),
        CopyCounts {
            rejected: 1,
            rejected_bytes: 1,
            ..Default::default()
        }
    );
    let empty: SharedBuffer<NeverDecode> = context
        .deserialize(&mut serde_json::Deserializer::from_str("[]"))
        .unwrap();
    assert!(empty.is_empty());
    assert_eq!(counts(&context).rejected, 1);
}

#[test]
fn decoded_element_storage_is_counted_once_for_bytes_and_audio() {
    let context = CopyContext::new(CopyPolicy::AllowCopies, "allowed.decoder");
    let data: SharedBuffer<u8> = context
        .deserialize(&mut serde_json::Deserializer::from_str("[1,2,3]"))
        .unwrap();
    assert_eq!(&*data, &[1, 2, 3]);
    let audio: AudioSamples = context
        .deserialize(&mut serde_json::Deserializer::from_str("[1.0,-0.5]"))
        .unwrap();
    assert_eq!(audio.as_slice(), &[1.0, -0.5]);
    assert_eq!(
        counts(&context),
        CopyCounts {
            completed: 2,
            copied_bytes: 11,
            ..Default::default()
        }
    );
}

#[test]
fn completed_payload_stays_counted_if_its_enclosing_message_is_invalid() {
    #[derive(Deserialize)]
    #[allow(dead_code)]
    struct Envelope {
        payload: SharedBuffer<u8>,
        required: bool,
    }
    let context = CopyContext::new(CopyPolicy::AllowCopies, "invalid.envelope");
    let result = context.deserialize::<Envelope, _>(&mut serde_json::Deserializer::from_str(
        r#"{"payload":[1,2,3]}"#,
    ));
    assert!(result.err().unwrap().to_string().contains("required"));
    assert_eq!(
        counts(&context),
        CopyCounts {
            completed: 1,
            copied_bytes: 3,
            ..Default::default()
        }
    );
}

#[test]
fn partial_decoder_errors_and_panics_are_failed_attempts() {
    let context = CopyContext::new(CopyPolicy::AllowCopies, "partial.decoder");
    let result: Result<SharedBuffer<u8>, _> =
        context.deserialize(&mut serde_json::Deserializer::from_str("[1,2,\"bad\"]"));
    assert!(result.is_err());
    assert_eq!(
        counts(&context),
        CopyCounts {
            failed: 1,
            failed_requested_bytes: 3,
            ..Default::default()
        }
    );
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        context.deserialize::<SharedBuffer<NeverDecode>, _>(
            &mut serde_json::Deserializer::from_str("[1]"),
        )
    }));
    assert!(panicked.is_err());
    assert_eq!(
        counts(&context),
        CopyCounts {
            failed: 2,
            failed_requested_bytes: 4,
            ..Default::default()
        }
    );
}

struct Nested;
impl<'de> Deserialize<'de> for Nested {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let inner = CopyContext::new(CopyPolicy::AllowCopies, "nested.inner");
        let _: SharedBuffer<u8> = inner
            .deserialize(&mut serde_json::Deserializer::from_str("[7,8]"))
            .unwrap();
        let result: Result<SharedBuffer<u8>, _> =
            inner.deserialize(&mut serde_json::Deserializer::from_str("[9,\"bad\"]"));
        assert!(result.is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            inner.deserialize::<SharedBuffer<NeverDecode>, _>(
                &mut serde_json::Deserializer::from_str("[1]"),
            )
        }))
        .is_err());
        assert_eq!(
            counts(&inner),
            CopyCounts {
                completed: 1,
                copied_bytes: 2,
                failed: 2,
                failed_requested_bytes: 3,
                ..Default::default()
            }
        );
        SharedBuffer::<u8>::deserialize(deserializer)?;
        Ok(Self)
    }
}

#[test]
fn nested_success_error_and_unwind_restore_outer_policy() {
    let outer = CopyContext::new(CopyPolicy::RequireZeroCopy, "nested.outer");
    let result = outer.deserialize::<Nested, _>(&mut serde_json::Deserializer::from_str("[3]"));
    assert!(result.err().unwrap().to_string().contains("nested.outer"));
    assert_eq!(
        counts(&outer),
        CopyCounts {
            rejected: 1,
            rejected_bytes: 1,
            ..Default::default()
        }
    );
    // No explicit scope after returning: ordinary Serde remains compatibility mode.
    assert_eq!(
        &*serde_json::from_str::<SharedBuffer<u8>>("[4]").unwrap(),
        &[4]
    );
}

struct Hinted {
    values: std::vec::IntoIter<u8>,
    hint: Option<usize>,
}
impl<'de> SeqAccess<'de> for Hinted {
    type Error = de::value::Error;
    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Self::Error> {
        self.values
            .next()
            .map(|value| seed.deserialize(value.into_deserializer()))
            .transpose()
    }
    fn size_hint(&self) -> Option<usize> {
        self.hint
    }
}

#[test]
fn size_hints_neither_grant_permission_nor_determine_copy_counts() {
    for hint in [None, Some(0), Some(1), Some(usize::MAX)] {
        for policy in [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy] {
            let context = CopyContext::new(policy, "hinted.decoder");
            let sequence = Hinted {
                values: vec![1, 2, 3].into_iter(),
                hint,
            };
            let result: Result<SharedBuffer<u8>, _> =
                context.deserialize(de::value::SeqAccessDeserializer::new(sequence));
            if policy == CopyPolicy::AllowCopies {
                let output = result.unwrap().into_vec();
                assert_eq!(output, vec![1, 2, 3]);
                assert!(output.capacity() <= 1024 * 1024);
                assert_eq!(
                    counts(&context),
                    CopyCounts {
                        completed: 1,
                        copied_bytes: 3,
                        ..Default::default()
                    }
                );
            } else {
                assert!(result.is_err());
                assert_eq!(
                    counts(&context),
                    CopyCounts {
                        rejected: 1,
                        rejected_bytes: 1,
                        ..Default::default()
                    }
                );
            }
            let empty = Hinted {
                values: vec![].into_iter(),
                hint,
            };
            let output: SharedBuffer<u8> = context
                .deserialize(de::value::SeqAccessDeserializer::new(empty))
                .unwrap();
            assert!(output.is_empty());
        }
    }
}

#[test]
fn concurrent_scopes_do_not_leak_policy_or_counts_across_threads() {
    let barrier = Arc::new(Barrier::new(2));
    let threads: Vec<_> = [CopyPolicy::AllowCopies, CopyPolicy::RequireZeroCopy]
        .into_iter()
        .map(|policy| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let context = CopyContext::new(policy, "thread.decoder");
                for _ in 0..32 {
                    barrier.wait();
                    let result: Result<SharedBuffer<u8>, _> =
                        context.deserialize(&mut serde_json::Deserializer::from_str("[1,2,3]"));
                    assert_eq!(result.is_ok(), policy == CopyPolicy::AllowCopies);
                }
                (policy, counts(&context))
            })
        })
        .collect();
    for thread in threads {
        let (policy, actual) = thread.join().unwrap();
        let expected = if policy == CopyPolicy::AllowCopies {
            CopyCounts {
                completed: 32,
                copied_bytes: 96,
                ..Default::default()
            }
        } else {
            CopyCounts {
                rejected: 32,
                rejected_bytes: 32,
                ..Default::default()
            }
        };
        assert_eq!(actual, expected);
    }
}
