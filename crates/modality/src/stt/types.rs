//! Wire types for streaming transcription updates.

use remotemedia_types::RuntimeData;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// One unit of transcript text, usually a word.
///
/// Every field except `text` is optional, so a model that cannot produce
/// timestamps or confidences builds the same token as one that can.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TranscriptToken {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub confidence: Option<f32>,
}

impl TranscriptToken {
    /// A token with no timing.
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            start_ms: None,
            end_ms: None,
            confidence: None,
        }
    }

    /// A token with a start and end time, in milliseconds.
    pub fn timed(text: impl Into<String>, start_ms: u64, end_ms: u64) -> Self {
        Self {
            text: text.into(),
            start_ms: Some(start_ms),
            end_ms: Some(end_ms),
            confidence: None,
        }
    }

    pub fn with_confidence(mut self, confidence: f32) -> Self {
        self.confidence = Some(confidence);
        self
    }
}

/// What a hypothesis covers. This decides how the stabilizer lines it up
/// against text it has already committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HypothesisScope {
    /// The whole utterance so far: the model re-decoded everything since the
    /// last final, or it reports its own running partial (Deepgram interim
    /// results, Vosk partials, Parakeet). The stabilizer finds where the
    /// committed text ends inside the hypothesis by matching words, so
    /// timestamps are not needed.
    #[default]
    Utterance,
    /// A sliding audio window that may start partway through the utterance
    /// (Whisper over a trimmed buffer). Tokens that end at or before the
    /// committed time are dropped when timestamps are present. The rest is
    /// lined up with the committed text by word overlap. A model without
    /// timestamps must call `commit_unstable` before it slides its window
    /// past text that is still unstable.
    Window,
}

/// Which transcript events a node sends downstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum TranscriptStreamingMode {
    /// The old behaviour: one `RuntimeData::Text` per finished utterance.
    #[default]
    Off,
    /// `delta` events for committed text only, then `final`. Text is never
    /// taken back, so this suits consumers that only append, such as an LLM
    /// prompt or a TTS lookahead.
    Stable,
    /// `delta` and `correction` events that include the tentative tail, then
    /// `final`. Use it for live captions.
    Full,
}

impl TranscriptStreamingMode {
    pub fn is_off(self) -> bool {
        matches!(self, Self::Off)
    }
}

/// Configuration shared by every transcription node. Embed it with
/// `#[serde(flatten)]`, or under a `transcript_updates` key, in the node's
/// own config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TranscriptStreamingConfig {
    /// Which events to emit. Defaults to `off`, which emits only plain-text
    /// finals.
    #[serde(default)]
    pub mode: TranscriptStreamingMode,
    /// How many consecutive hypotheses must agree on a token before it is
    /// committed. `1` trusts each hypothesis as soon as it arrives, which
    /// suits models that already stabilise their own partials.
    #[serde(default = "default_agreement_updates")]
    pub agreement_updates: usize,
    /// How many agreed tokens at the end of the hypothesis stay revisable.
    /// The last word of a decode window is often cut off mid-word.
    #[serde(default = "default_trailing_guard_tokens")]
    pub trailing_guard_tokens: usize,
}

fn default_agreement_updates() -> usize {
    2
}

fn default_trailing_guard_tokens() -> usize {
    1
}

impl Default for TranscriptStreamingConfig {
    fn default() -> Self {
        Self {
            mode: TranscriptStreamingMode::default(),
            agreement_updates: default_agreement_updates(),
            trailing_guard_tokens: default_trailing_guard_tokens(),
        }
    }
}

impl TranscriptStreamingConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.agreement_updates == 0 {
            return Err("agreement_updates must be at least 1".to_string());
        }
        Ok(())
    }

    /// Turn an update into the `RuntimeData` the node should emit, or `None`
    /// if this mode does not send it.
    ///
    /// In `off` mode only finals go out, as `RuntimeData::Text`, so existing
    /// pipelines see no change. Every other mode sends `RuntimeData::Json`.
    pub fn render(&self, update: &TranscriptUpdate) -> Option<RuntimeData> {
        match (self.mode, update) {
            (TranscriptStreamingMode::Off, TranscriptUpdate::Final(f)) => {
                Some(RuntimeData::Text(f.text.clone()))
            }
            (TranscriptStreamingMode::Off, _) => None,
            _ => Some(update.to_runtime_data()),
        }
    }
}

/// One change to the current utterance's displayed token list: remove
/// `removed` tokens at `index`, then insert `tokens` there.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TranscriptEdit {
    /// Utterance this edit belongs to. Starts at 0 and goes up after each
    /// `final` (or `reset`).
    pub utterance_id: u64,
    /// Goes up by one on every event the stabilizer sends.
    pub revision: u64,
    /// Token index where the edit starts.
    pub index: usize,
    /// How many tokens to remove at `index`. Always 0 for `delta`.
    pub removed: usize,
    /// Tokens to insert at `index`.
    pub tokens: Vec<TranscriptToken>,
    /// `tokens` joined with spaces.
    pub text: String,
    /// Tokens `[0, stable_tokens)` will not change again in this utterance.
    pub stable_tokens: usize,
    /// The whole utterance text after this edit, for clients that would
    /// rather replace than patch, or that missed an event.
    pub transcript: String,
}

/// The finished text of one utterance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct TranscriptFinal {
    pub utterance_id: u64,
    pub revision: u64,
    pub text: String,
    pub tokens: Vec<TranscriptToken>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_ms: Option<u64>,
}

/// A streaming transcription event, tagged by `type` on the wire.
///
/// Applying every `delta` and `correction` of an utterance, in order, to an
/// empty token list gives exactly the tokens of its `final`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TranscriptUpdate {
    /// Tokens appended at the end. `removed` is 0 and `index` is the
    /// previous length. A delta with no tokens only moves `stable_tokens`.
    Delta(TranscriptEdit),
    /// Tentative tokens from `index` onward were replaced.
    Correction(TranscriptEdit),
    /// The utterance is closed.
    Final(TranscriptFinal),
}

impl TranscriptUpdate {
    pub fn utterance_id(&self) -> u64 {
        match self {
            Self::Delta(e) | Self::Correction(e) => e.utterance_id,
            Self::Final(f) => f.utterance_id,
        }
    }

    pub fn revision(&self) -> u64 {
        match self {
            Self::Delta(e) | Self::Correction(e) => e.revision,
            Self::Final(f) => f.revision,
        }
    }

    pub fn is_final(&self) -> bool {
        matches!(self, Self::Final(_))
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("TranscriptUpdate always serialises")
    }

    pub fn to_runtime_data(&self) -> RuntimeData {
        RuntimeData::Json(self.to_json())
    }
}
