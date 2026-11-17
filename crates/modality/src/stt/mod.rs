//! Speech-to-text primitives shared by every transcription node.
//!
//! # Streaming transcript updates
//!
//! Any transcription node can offer streaming `delta` / `correction` /
//! `final` output without writing its own correction logic. The node:
//!
//! 1. embeds a [`TranscriptStreamingConfig`] in its config (`mode` defaults
//!    to `off`, which keeps the plain-text-final behaviour);
//! 2. keeps one [`TranscriptStabilizer`] per stream;
//! 3. after each inference pass, calls `push_tokens` / `push_text` /
//!    `push_append` with whatever the model produced, with timestamps if it
//!    has them;
//! 4. on end of utterance (VAD, EOU, flush or end of stream), calls
//!    `finalize` or `finalize_tokens`;
//! 5. emits `config.render(&update)` for each returned update.
//!
//! Which [`HypothesisScope`] to pass:
//!
//! | Model kind                                     | Scope       | Notes |
//! |------------------------------------------------|-------------|-------|
//! | Re-decode the whole utterance buffer (Whisper) | `Utterance` | timestamps optional |
//! | Running partial from the model or vendor       | `Utterance` | `agreement_updates: 1` if the model's partials are already stable |
//! | Sliding window over a trimmed buffer           | `Window`    | with timestamps, call `commit_until_ms` when trimming; without them, call `commit_unstable` |
//! | Append-only fragments                          | (use `push_append`) | |
//! | Batch only, one result per utterance           | none        | just `finalize_text` |
//!
//! The algorithm is described in [`stabilizer`]. The Python port in
//! `remotemedia.modality.stt` is held to this one by the shared fixture
//! `tests/fixtures/stt_stabilizer_cases.json`.

pub mod stabilizer;
pub mod types;

pub use stabilizer::TranscriptStabilizer;
pub use types::{
    HypothesisScope, TranscriptEdit, TranscriptFinal, TranscriptStreamingConfig,
    TranscriptStreamingMode, TranscriptToken, TranscriptUpdate,
};

#[cfg(test)]
mod tests;
