//! Transport-agnostic, modality-organised primitives shared across
//! the RemoteMedia SDK and its loadable plugins.
//!
//! This crate intentionally has **no** HTTP, no vendor SDK, no Candle
//! / ORT / model-loading code — only the small set of types that
//! every provider (in-tree `OpenAIChatNode`, dlopen'd
//! `FoundryLocalChatNode`, the upcoming `candle-transformers-loadable`
//! plugin, future OpenAI/Anthropic wrappers) needs to share so that
//! pipelines downstream see one wire format regardless of who
//! produced the data.
//!
//! Sized so loadable cdylib plugins (which **must not** link
//! `remotemedia-core`) can depend on this crate directly. The shared
//! `RuntimeData` and `Error` enums come from `remotemedia-types`, so
//! a value built against this crate emits the same bytes whether it
//! runs in-process or behind dlopen.
//!
//! # Modules (feature-gated)
//!
//! | Feature      | Module        | What it covers                                                                          |
//! |--------------|---------------|------------------------------------------------------------------------------------------|
//! | `llm`        | [`llm`]       | Chat history, OpenAI-shaped tool specs, streaming tool-call dispatch.                    |
//! | `stt`        | [`stt`]       | TranscriptionSegment, streaming partial+final accumulator, common STT config.            |
//! | `tts`        | [`tts`]       | VoiceSpec, AudioFormat, streaming chunk emit helpers, common TTS config.                 |
//! | `t2i`        | [`t2i`]       | GenerationParams, scheduler enum, common text-to-image config.                           |
//! | `vision`     | [`vision`]    | Classification, preprocessing knobs, vision config.                                      |
//! | `embedding`  | [`embedding`] | FeatureVector (shared with vision feature extractors), PoolingStrategy, L2-normalize, embedding config. |
//!
//! Default features = `[]` (strict opt-in). A consumer pulls only
//! the modalities it actually emits.

#[cfg(feature = "llm")]
pub mod llm;

#[cfg(feature = "stt")]
pub mod stt;

#[cfg(feature = "tts")]
pub mod tts;

#[cfg(feature = "t2i")]
pub mod t2i;

#[cfg(feature = "vision")]
pub mod vision;

#[cfg(feature = "embedding")]
pub mod embedding;
