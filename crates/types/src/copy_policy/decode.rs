//! Synchronous Serde policy scope and allocation-time payload accounting.
use super::{CopyContext, CopyEvent, CopyLedger, CopyOperation, CopyReason};
use serde::{
    de::{DeserializeSeed, Error, SeqAccess, Visitor},
    Deserialize, Deserializer,
};
use std::{cell::RefCell, fmt, marker::PhantomData, mem::size_of};

thread_local! {
    static CURRENT: RefCell<Option<CopyContext>> = const { RefCell::new(None) };
}

struct Restore(Option<CopyContext>);
impl Drop for Restore {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            current.replace(self.0.take());
        });
    }
}

impl CopyContext {
    /// Deserialize synchronously under this boundary's bulk materialization policy.
    /// SharedBuffer and AudioSamples use it before decoding any payload element.
    /// Nested calls restore the previous scope, including on error or unwind.
    /// It does not propagate to spawned threads or deferred/asynchronous work.
    /// Arbitrary custom deserializers and metadata allocations are not governed.
    /// Generic buffer elements count inline storage, not recursively allocated
    /// data. Media u8/f32 counts are exact logical destination bytes. A completed
    /// buffer is counted even if a surrounding message later fails validation.
    pub fn deserialize<'de, T, D>(&self, deserializer: D) -> Result<T, D::Error>
    where
        T: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        let _restore = Restore(CURRENT.with(|current| current.replace(Some(self.clone()))));
        T::deserialize(deserializer)
    }
}

pub(crate) fn deserialize_vec<'de, T, D>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    let context = CURRENT
        .with(|current| current.borrow().clone())
        .unwrap_or_else(|| CopyContext::legacy().clone());
    deserializer.deserialize_seq(PayloadVisitor {
        context,
        marker: PhantomData,
    })
}

struct PayloadVisitor<T> {
    context: CopyContext,
    marker: PhantomData<T>,
}
impl<'de, T: Deserialize<'de>> Visitor<'de> for PayloadVisitor<T> {
    type Value = Vec<T>;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a payload element sequence")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Vec<T>, A::Error> {
        // Hints can be missing or inaccurate. They only guide bounded capacity;
        // neither accounting nor strict policy relies on a declared length.
        let capacity = sequence
            .size_hint()
            .unwrap_or(0)
            .min(1024 * 1024 / size_of::<T>().max(1));
        let mut output = Vec::new();
        let mut attempt = None;
        while let Some(value) = sequence.next_element_seed(PayloadElement {
            context: &self.context,
            attempt: &mut attempt,
            marker: PhantomData,
        })? {
            // No destination allocation until an element passes policy.
            if output.is_empty() {
                output.reserve(capacity);
            }
            output.push(value);
        }
        if let Some(attempt) = attempt {
            attempt.complete();
        }
        Ok(output)
    }
}

struct PayloadElement<'context, 'attempt, T> {
    context: &'context CopyContext,
    attempt: &'attempt mut Option<CopyOperation>,
    marker: PhantomData<T>,
}
impl<'de, T: Deserialize<'de>> DeserializeSeed<'de> for PayloadElement<'_, '_, T> {
    type Value = T;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<T, D::Error> {
        let reason = CopyReason::PortableDeserialization;
        let bytes = size_of::<T>() as u64;
        self.context
            .check(reason, bytes)
            .map_err(D::Error::custom)?;
        if bytes != 0 {
            let attempt = self.attempt.get_or_insert_with(|| {
                self.context.record(reason, CopyEvent::Started);
                CopyOperation {
                    context: self.context.clone(),
                    reason,
                    bytes: 0,
                    completed: None,
                }
            });
            attempt.bytes = attempt
                .bytes
                .checked_add(bytes)
                .ok_or_else(|| D::Error::custom("payload deserialization byte count overflow"))?;
        }
        T::deserialize(deserializer)
    }
}
