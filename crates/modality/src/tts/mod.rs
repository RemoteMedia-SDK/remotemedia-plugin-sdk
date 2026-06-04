//! Text-to-speech wire types shared by TTS-emitting nodes.
//!
//! Consumers: in-tree TTS nodes (Kokoro, VibeVoice, Voxtral),
//! `examples/candle-transformers-loadable/` (`CandleTTSNode`),
//! future OpenAI / Anthropic TTS wrappers.
//!
//! # Planned types (land with first consumer)
//!
//! - `VoiceSpec { voice_id, language, gender }` — selecting a voice
//!   in a provider-agnostic shape.
//! - `AudioFormat` enum — Pcm16 / F32 / Opus / Mp3, etc.
//! - `TtsConfig { voice, speed, pitch, sample_rate, format }` —
//!   common config shape.
//! - `emit_chunk(…, callback, channel)` — small helper that tags
//!   audio chunks onto a `RuntimeData::Audio` channel.
//!
//! Same derive surface as `stt` types.
