//! Speech-to-text wire types shared by STT-emitting nodes.
//!
//! Consumers: `examples/whisper-loadable/` (v0.2 migration target),
//! `examples/candle-transformers-loadable/` (`CandleSTTNode`),
//! `examples/foundry-local-loadable/` (`FoundryLocalSTTNode`,
//! planned).
//!
//! # Planned types (land with first consumer)
//!
//! - `TranscriptionSegment { text, start_ms, end_ms, speaker_id,
//!   is_final, confidence }` — single segment of a transcript, used
//!   for both streaming partials and final results.
//! - `StreamingTranscript` — accumulator that joins partial segments
//!   with stable IDs, emits a final once `is_final` lands.
//! - `SttConfig { language, beam_size, vad_filter, timestamps, … }`
//!   — common config shape; provider-specific knobs live in a
//!   per-node `arch_params` Value.
//!
//! Every type will derive `Serialize + Deserialize + JsonSchema +
//! Debug + Clone` so manifests can reference them and the schema
//! generator can export them to clients.
