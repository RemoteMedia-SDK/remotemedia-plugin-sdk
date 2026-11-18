//! Shared speech-to-text (STT) streaming contract for Whisper-family nodes.
//!
//! Both the Candle-backed `WhisperNode` and the whisper.cpp-backed loadable
//! plugin implement the [`Whisper`] trait defined here. The partial-result
//! state machine ([`WhisperStreamingState`]) and the wire types
//! ([`TranscriptEvent`], [`DecodedSegment`], [`StreamingOptions`]) are shared
//! so every backend emits byte-for-byte identical streaming events — a
//! downstream consumer cannot tell Candle from whisper.cpp apart.
//!
//! The audio windowing (resample, decode scheduling, window bound, trim) is
//! Whisper-specific and lives here. Agreement, alignment and commit go
//! through the model-agnostic [`TranscriptStabilizer`] in
//! `remotemedia_modality::stt`, which every other transcription node uses
//! too. The only backend-specific piece is [`Whisper::decode_window`], which
//! runs one inference pass over a normalized 16 kHz mono decode window and
//! returns the raw segments. [`Whisper::stream_audio`] / [`Whisper::finalize`]
//! drive the state on top of it. After each call, a node in a
//! `transcript_updates` mode other than `off` drains
//! [`WhisperStreamingState::take_updates`] for the shared `delta` /
//! `correction` / `final` events.

use std::collections::VecDeque;

pub use remotemedia_modality::stt::{
    HypothesisScope, TranscriptStabilizer, TranscriptStreamingConfig, TranscriptStreamingMode,
    TranscriptToken, TranscriptUpdate,
};
use remotemedia_types::Error;
use serde::{Deserialize, Serialize};

fn invalid_input(message: impl Into<String>) -> Error {
    Error::InvalidInput {
        message: message.into(),
        node_id: "Whisper".to_string(),
        context: String::new(),
    }
}

/// Whisper's fixed inference sample rate.
pub const WHISPER_SAMPLE_RATE: u32 = 16_000;

/// One decoded phrase returned by a single inference pass. Times are absolute
/// within the decode window (the state machine adds the window offset).
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedSegment {
    pub text: String,
    pub start_ms: u64,
    pub end_ms: u64,
}

/// Tuning knobs for the partial-result state machine.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct StreamingOptions {
    /// New normalized audio required between streaming inference passes.
    pub decode_interval_ms: u32,
    /// Audio retained before the last committed timestamp for decoder context.
    pub overlap_ms: u32,
    /// Hard upper bound for audio decoded by any streaming inference pass.
    pub max_window_ms: u32,
    /// Consecutive hypotheses required before text becomes stable.
    pub agreement_updates: usize,
    /// Agreed trailing words kept revisable to avoid premature commitment.
    pub trailing_guard_words: usize,
    /// Which shared updates [`WhisperStreamingState::take_updates`] yields.
    #[serde(default)]
    pub transcript_updates: TranscriptStreamingMode,
}

impl Default for StreamingOptions {
    fn default() -> Self {
        Self {
            decode_interval_ms: 2_000,
            overlap_ms: 750,
            max_window_ms: 30_000,
            agreement_updates: 2,
            trailing_guard_words: 2,
            transcript_updates: TranscriptStreamingMode::Off,
        }
    }
}

/// A single streaming transcription event.
///
/// `kind` is one of `"partial"` / `"correction"` / `"final"`. `revision`
/// monotonically increments; `replaces` points at the revision this event
/// supersedes (used by clients to patch rather than replace). `stable_text`
/// is the committed prefix; `unstable_text` is the speculative tail.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TranscriptEvent {
    #[serde(rename = "type")]
    pub kind: String,
    pub revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replaces: Option<u64>,
    pub text: String,
    pub stable_text: String,
    pub unstable_text: String,
    pub audio_start_ms: u64,
    pub audio_end_ms: u64,
}

/// Common configuration shared by every Whisper backend.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WhisperConfig {
    /// Target language (ISO 639-1) or `"auto"`.
    #[serde(default = "default_language")]
    pub language: String,
    /// `"transcribe"` or `"translate"`.
    #[serde(default = "default_task")]
    pub task: String,
    /// Enable streaming partial transcriptions.
    #[serde(default = "default_true")]
    pub streaming: bool,
    /// New normalized audio required between streaming inference passes.
    #[serde(default = "default_decode_interval")]
    pub streaming_decode_interval_ms: u32,
    /// Audio retained before the last committed timestamp for decoder context.
    #[serde(default = "default_overlap")]
    pub streaming_overlap_ms: u32,
    /// Hard upper bound for audio decoded by any streaming inference pass.
    #[serde(default = "default_max_window")]
    pub streaming_max_window_ms: u32,
    /// Consecutive hypotheses required before text becomes stable.
    #[serde(default = "default_agreement")]
    pub streaming_agreement_updates: usize,
    /// Agreed trailing words kept revisable.
    #[serde(default = "default_guard")]
    pub streaming_trailing_guard_words: usize,
    /// Shared `delta` / `correction` / `final` updates (`off` by default).
    #[serde(default)]
    pub transcript_updates: TranscriptStreamingMode,
}

impl Default for WhisperConfig {
    fn default() -> Self {
        Self {
            language: default_language(),
            task: default_task(),
            streaming: true,
            streaming_decode_interval_ms: default_decode_interval(),
            streaming_overlap_ms: default_overlap(),
            streaming_max_window_ms: default_max_window(),
            streaming_agreement_updates: default_agreement(),
            streaming_trailing_guard_words: default_guard(),
            transcript_updates: TranscriptStreamingMode::Off,
        }
    }
}

impl WhisperConfig {
    /// Validate configuration; mirrors `WhisperNodeConfig::validate` in
    /// candle-nodes so both backends reject the same malformed input.
    pub fn validate(&self) -> Result<(), String> {
        if self.language != "auto" && self.language.len() != 2 {
            return Err(format!(
                "Invalid language code: {}. Use 'auto' or ISO 639-1 code (e.g., 'en', 'es')",
                self.language
            ));
        }
        if self.task != "transcribe" && self.task != "translate" {
            return Err(format!(
                "Invalid task: {}. Use 'transcribe' or 'translate'",
                self.task
            ));
        }
        if self.streaming_decode_interval_ms == 0 {
            return Err("streaming_decode_interval_ms must be greater than zero".to_string());
        }
        if !(1000..=30000).contains(&self.streaming_max_window_ms) {
            return Err("streaming_max_window_ms must be between 1000 and 30000".to_string());
        }
        if self.streaming_overlap_ms >= self.streaming_max_window_ms {
            return Err(
                "streaming_overlap_ms must be smaller than streaming_max_window_ms".to_string(),
            );
        }
        if self.streaming_decode_interval_ms > self.streaming_max_window_ms {
            return Err(
                "streaming_decode_interval_ms must not exceed streaming_max_window_ms".to_string(),
            );
        }
        if self.streaming_agreement_updates < 2 {
            return Err("streaming_agreement_updates must be at least 2".to_string());
        }
        Ok(())
    }

    /// Build [`StreamingOptions`] from this config.
    pub fn streaming_options(&self) -> StreamingOptions {
        StreamingOptions {
            decode_interval_ms: self.streaming_decode_interval_ms,
            overlap_ms: self.streaming_overlap_ms,
            max_window_ms: self.streaming_max_window_ms,
            agreement_updates: self.streaming_agreement_updates,
            trailing_guard_words: self.streaming_trailing_guard_words,
            transcript_updates: self.transcript_updates,
        }
    }
}

fn default_language() -> String {
    "auto".to_string()
}
fn default_task() -> String {
    "transcribe".to_string()
}
fn default_true() -> bool {
    true
}
fn default_decode_interval() -> u32 {
    2_000
}
fn default_overlap() -> u32 {
    750
}
fn default_max_window() -> u32 {
    30_000
}
fn default_agreement() -> usize {
    2
}
fn default_guard() -> usize {
    2
}

/// Whisper-family STT backend contract.
///
/// The only backend-specific method is [`Whisper::decode_window`]; everything
/// else (the partial-result state machine) is provided. A node owns a
/// [`WhisperStreamingState`] and, on each audio arrival, calls
/// [`Whisper::stream_audio`] to advance it and collect any new
/// [`TranscriptEvent`]s, and [`Whisper::finalize`] on a flush to emit the
/// final event.
pub trait Whisper: Send + Sync {
    /// Effective configuration (may consult a runtime override).
    fn config(&self) -> WhisperConfig;

    /// Run one inference pass over a normalized 16 kHz mono decode window and
    /// return the raw decoded segments. Backend-specific.
    ///
    /// Segment times must come from the model (timestamp tokens or word
    /// alignment). The state trims audio and force-commits at the window
    /// bound by these times. A single segment spanning the whole window is
    /// not enough. A backend without timestamps should instead re-decode
    /// the whole utterance and feed `TranscriptStabilizer` with
    /// `HypothesisScope::Utterance`. Measured with LiteRT whisper-tiny on a
    /// 29 s clip, that path stays within 0.04 WER of a one-shot decode; this
    /// window path, given whole-window spans, does not.
    fn decode_window(&self, samples: &[f32]) -> Result<Vec<DecodedSegment>, Error>;

    /// Feed a chunk of (possibly non-16k / multi-channel) audio into the
    /// shared streaming state machine and return any new transcript events.
    ///
    /// `state` is the node-owned [`WhisperStreamingState`]; the caller holds
    /// the lock across the call to keep revision numbering deterministic.
    fn stream_audio(
        &self,
        state: &mut WhisperStreamingState,
        samples: &[f32],
        sample_rate: u32,
        channels: u32,
    ) -> Result<Vec<TranscriptEvent>, Error> {
        let options = self.config().streaming_options();
        state
            .push_audio(samples, sample_rate, channels)
            .map_err(|e| Error::Execution(e.to_string()))?;
        let window = state.decode_window(options, false);
        let Some(window) = window else {
            return Ok(Vec::new());
        };
        let segments = self.decode_window(&window.samples)?;
        let event = state.apply_hypothesis(segments, window.start_ms, options, false);
        Ok(event.into_iter().collect())
    }

    /// Finalize: force a decode pass and emit the final [`TranscriptEvent`].
    fn finalize(
        &self,
        state: &mut WhisperStreamingState,
    ) -> Result<Option<TranscriptEvent>, Error> {
        let options = self.config().streaming_options();
        let window = state.decode_window(options, true);
        let Some(window) = window else {
            return Ok(None);
        };
        let segments = self.decode_window(&window.samples)?;
        let event = state.apply_hypothesis(segments, window.start_ms, options, true);
        Ok(event)
    }
}

// ─── Streaming state machine (lifted from candle-nodes, backend-agnostic) ───

#[derive(Debug, Default)]
struct StreamingNormalizer {
    sample_rate: u32,
    channels: u32,
    pending: Vec<f32>,
    source_position: f64,
}

impl StreamingNormalizer {
    fn push(
        &mut self,
        samples: &[f32],
        sample_rate: u32,
        channels: u32,
    ) -> Result<Vec<f32>, Error> {
        if sample_rate == 0 || channels == 0 {
            return Err(invalid_input(format!(
                "non-zero sample rate and channel count (got {sample_rate} Hz, {channels} ch)"
            )));
        }
        if !samples.len().is_multiple_of(channels as usize) {
            return Err(invalid_input(format!(
                "complete interleaved audio frames ({} samples for {channels} channels)",
                samples.len()
            )));
        }
        if self.sample_rate == 0 {
            self.sample_rate = sample_rate;
            self.channels = channels;
        } else if self.sample_rate != sample_rate || self.channels != channels {
            return Err(invalid_input(format!(
                "consistent sample rate/channels required (saw {} Hz/{} ch, now {} Hz/{} ch)",
                self.sample_rate, self.channels, sample_rate, channels
            )));
        }

        let mono: Vec<f32> = if channels == 1 {
            samples.to_vec()
        } else {
            samples
                .chunks_exact(channels as usize)
                .map(|frame| frame.iter().sum::<f32>() / channels as f32)
                .collect()
        };
        if sample_rate == WHISPER_SAMPLE_RATE {
            return Ok(mono);
        }

        self.pending.extend(mono);
        let step = sample_rate as f64 / WHISPER_SAMPLE_RATE as f64;
        let mut output = Vec::new();
        while self.source_position + 1.0 < self.pending.len() as f64 {
            let left = self.source_position.floor() as usize;
            let fraction = (self.source_position - left as f64) as f32;
            output.push(self.pending[left] * (1.0 - fraction) + self.pending[left + 1] * fraction);
            self.source_position += step;
        }

        let consumed = (self.source_position.floor() as usize).saturating_sub(1);
        if consumed > 0 {
            self.pending.drain(..consumed);
            self.source_position -= consumed as f64;
        }
        Ok(output)
    }
}

/// Streaming state for a Whisper-family backend.
///
/// This type handles the audio side: normalising input to 16 kHz mono,
/// scheduling decodes, bounding the window and trimming audio that has been
/// committed. Agreement, alignment and commit go through the shared,
/// model-agnostic [`TranscriptStabilizer`] from `remotemedia-modality`. That
/// is the same code every other transcription node uses.
///
/// It produces two kinds of output:
/// - the legacy [`TranscriptEvent`]s (`partial` / `correction` / `final`),
///   returned by [`Self::apply_hypothesis`], [`Whisper::stream_audio`] and
///   [`Whisper::finalize`];
/// - the shared [`TranscriptUpdate`]s (`delta` / `correction` / `final`),
///   collected for [`Self::take_updates`]. Which ones are produced follows
///   [`StreamingOptions::transcript_updates`].
pub struct WhisperStreamingState {
    normalizer: StreamingNormalizer,
    audio: VecDeque<f32>,
    buffer_start_sample: u64,
    total_samples: u64,
    new_samples_since_decode: usize,
    stabilizer: TranscriptStabilizer,
    updates: Vec<TranscriptUpdate>,
    revision: u64,
    last_emitted_text: Option<String>,
    finalized: bool,
}

impl Default for WhisperStreamingState {
    fn default() -> Self {
        Self {
            normalizer: StreamingNormalizer::default(),
            audio: VecDeque::new(),
            buffer_start_sample: 0,
            total_samples: 0,
            new_samples_since_decode: 0,
            stabilizer: TranscriptStabilizer::new(stabilizer_config(StreamingOptions::default())),
            updates: Vec::new(),
            revision: 0,
            last_emitted_text: None,
            finalized: false,
        }
    }
}

fn stabilizer_config(options: StreamingOptions) -> TranscriptStreamingConfig {
    TranscriptStreamingConfig {
        mode: options.transcript_updates,
        agreement_updates: options.agreement_updates.max(1),
        trailing_guard_tokens: options.trailing_guard_words,
    }
}

impl WhisperStreamingState {
    /// Append normalized-or-not audio; auto downmixes/resamples to 16k mono.
    pub fn push_audio(
        &mut self,
        samples: &[f32],
        sample_rate: u32,
        channels: u32,
    ) -> Result<usize, Error> {
        if self.finalized {
            // The last utterance was finalized (a flush at a pause): this
            // audio starts the next one. The stabilizer already moved on to
            // the next `utterance_id`; times keep counting from the start
            // of the stream.
            self.finalized = false;
            self.last_emitted_text = None;
            self.new_samples_since_decode = 0;
        }
        let normalized = self.normalizer.push(samples, sample_rate, channels)?;
        let count = normalized.len();
        self.audio.extend(normalized);
        self.total_samples += count as u64;
        self.new_samples_since_decode += count;
        Ok(count)
    }

    /// Return the next decode window if enough new audio has arrived (or
    /// `force`). Returns `None` if nothing to decode yet.
    pub fn decode_window(
        &mut self,
        options: StreamingOptions,
        force: bool,
    ) -> Option<DecodeWindow> {
        if self.audio.is_empty() {
            return None;
        }
        self.stabilizer.reconfigure(stabilizer_config(options));
        self.force_commit_for_bound(options);
        self.trim_audio(options, false);
        let interval_samples = ms_to_samples(options.decode_interval_ms as u64) as usize;
        if !force && self.new_samples_since_decode < interval_samples {
            return None;
        }
        self.new_samples_since_decode = 0;
        Some(DecodeWindow {
            samples: self.audio.iter().copied().collect(),
            start_ms: samples_to_ms(self.buffer_start_sample),
        })
    }

    /// Apply a decode hypothesis. Segment times are relative to the window,
    /// which starts at `window_start_ms`. Returns the next legacy event, if
    /// any. The shared updates are kept for [`Self::take_updates`].
    pub fn apply_hypothesis(
        &mut self,
        relative_segments: Vec<DecodedSegment>,
        window_start_ms: u64,
        options: StreamingOptions,
        final_result: bool,
    ) -> Option<TranscriptEvent> {
        self.stabilizer.reconfigure(stabilizer_config(options));
        // The window ends where the audio does: no audio is pushed while a
        // window is decoded.
        let window_end_ms = samples_to_ms(self.total_samples).max(window_start_ms);
        let mut previous_end = window_start_ms;
        let tokens: Vec<TranscriptToken> = relative_segments
            .into_iter()
            .map(|segment| {
                let (mut start, mut end) = (segment.start_ms + window_start_ms, segment.end_ms + window_start_ms);
                // A segment without a span (a decoder clamps a timestamp that
                // falls past the window to its end, so start == end) still
                // lies inside the window: give it the honest bound, from the
                // previous segment's end to the window's end, instead of an
                // instant.
                if end <= start {
                    start = previous_end.min(window_end_ms);
                    end = window_end_ms;
                }
                previous_end = end;
                TranscriptToken::timed(normalize_text(&segment.text), start, end)
            })
            .collect();

        if final_result {
            let updates = self
                .stabilizer
                .finalize_tokens(tokens, HypothesisScope::Window);
            let text = updates
                .iter()
                .find_map(|update| match update {
                    TranscriptUpdate::Final(f) => Some(f.text.clone()),
                    _ => None,
                })
                .unwrap_or_default();
            self.updates.extend(updates);
            self.finalized = true;
            let event = self.make_event("final", text.clone(), text, String::new());
            self.trim_audio(options, true);
            return Some(event);
        }

        let updates = self.stabilizer.push_tokens(tokens, HypothesisScope::Window);
        self.updates.extend(updates);
        self.force_commit_for_bound(options);
        self.trim_audio(options, false);

        let text = self.stabilizer.transcript();
        if self.last_emitted_text.as_deref() == Some(text.as_str()) || text.is_empty() {
            return None;
        }
        let kind = if self.last_emitted_text.is_some() {
            "correction"
        } else {
            "partial"
        };
        let stable = self.stabilizer.stable_text();
        let unstable = self.stabilizer.unstable_text();
        Some(self.make_event(kind, text, stable, unstable))
    }

    /// Drain the shared `delta` / `correction` / `final` updates produced
    /// since the last call. Empty when `transcript_updates` is `off`, except
    /// for the `final`.
    pub fn take_updates(&mut self) -> Vec<TranscriptUpdate> {
        std::mem::take(&mut self.updates)
    }

    /// The shared stabilizer this state drives.
    pub fn stabilizer(&self) -> &TranscriptStabilizer {
        &self.stabilizer
    }

    /// When the audio exceeds `max_window_ms`, the window is about to slide
    /// past tentative text. Commit it first: everything up to the new window
    /// start plus `overlap_ms`, or at least the oldest token.
    fn force_commit_for_bound(&mut self, options: StreamingOptions) {
        let max_window_start_ms = samples_to_ms(
            self.total_samples
                .saturating_sub(ms_to_samples(options.max_window_ms as u64)),
        );
        if max_window_start_ms <= samples_to_ms(self.buffer_start_sample) {
            return;
        }
        let required_commit_ms = max_window_start_ms + options.overlap_ms as u64;
        let before = self.stabilizer.unstable_len();
        let mut updates = self.stabilizer.commit_until_ms(required_commit_ms);
        if before > 0 && self.stabilizer.unstable_len() == before {
            updates = self.stabilizer.commit_unstable_prefix(1);
        }
        self.updates.extend(updates);
    }

    fn trim_audio(&mut self, options: StreamingOptions, final_result: bool) {
        let target_ms = if final_result {
            samples_to_ms(self.total_samples)
        } else {
            self.stabilizer
                .committed_through_ms()
                .unwrap_or(0)
                .saturating_sub(options.overlap_ms as u64)
        };
        let target_sample = ms_to_samples(target_ms).min(self.total_samples);
        let remove = target_sample.saturating_sub(self.buffer_start_sample) as usize;
        let remove = remove.min(self.audio.len());
        self.audio.drain(..remove);
        self.buffer_start_sample += remove as u64;

        self.enforce_audio_bound(options);
    }

    fn enforce_audio_bound(&mut self, options: StreamingOptions) {
        let max_samples = ms_to_samples(options.max_window_ms as u64) as usize;
        if self.audio.len() > max_samples {
            let extra = self.audio.len() - max_samples;
            self.audio.drain(..extra);
            self.buffer_start_sample += extra as u64;
        }
    }

    fn make_event(
        &mut self,
        kind: &str,
        text: String,
        stable_text: String,
        unstable_text: String,
    ) -> TranscriptEvent {
        let final_result = kind == "final";
        let changed = self.last_emitted_text.as_deref() != Some(text.as_str());
        let replaces = (changed && self.revision > 0).then_some(self.revision);
        if changed || self.revision == 0 {
            self.revision += 1;
        }
        self.last_emitted_text = Some(text.clone());

        TranscriptEvent {
            kind: kind.to_string(),
            revision: self.revision,
            replaces: if kind == "correction" || (final_result && changed) {
                replaces
            } else {
                None
            },
            text,
            stable_text,
            unstable_text,
            audio_start_ms: samples_to_ms(self.buffer_start_sample),
            audio_end_ms: samples_to_ms(self.total_samples),
        }
    }
}

/// One decode window handed to the backend.
pub struct DecodeWindow {
    pub samples: Vec<f32>,
    pub start_ms: u64,
}

fn normalize_text(text: &str) -> String {
    let mut visible = String::with_capacity(text.len());
    let mut remaining = text;
    while let Some(start) = remaining.find("<|") {
        visible.push_str(&remaining[..start]);
        let Some(end) = remaining[start + 2..].find("|>") else {
            visible.push_str(&remaining[start..]);
            remaining = "";
            break;
        };
        remaining = &remaining[start + 2 + end + 2..];
    }
    visible.push_str(remaining);
    visible.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn ms_to_samples(ms: u64) -> u64 {
    ms * WHISPER_SAMPLE_RATE as u64 / 1000
}

fn samples_to_ms(samples: u64) -> u64 {
    samples * 1000 / WHISPER_SAMPLE_RATE as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Decodes "words" from the window: one word per 500 ms of absolute
    /// audio, named by its index. Word 3 is misheard as "tree" until the
    /// window has heard 1 s past it.
    struct FakeWhisper {
        config: WhisperConfig,
        decodes: Mutex<Vec<(usize, u64)>>,
    }

    impl FakeWhisper {
        fn new(mode: TranscriptStreamingMode) -> (Self, WhisperStreamingState) {
            let config = WhisperConfig {
                streaming_decode_interval_ms: 1_000,
                streaming_max_window_ms: 4_000,
                streaming_overlap_ms: 500,
                transcript_updates: mode,
                ..WhisperConfig::default()
            };
            let fake = Self {
                config,
                decodes: Mutex::new(Vec::new()),
            };
            (fake, WhisperStreamingState::default())
        }
    }

    impl Whisper for FakeWhisper {
        fn config(&self) -> WhisperConfig {
            self.config.clone()
        }

        fn decode_window(&self, samples: &[f32]) -> Result<Vec<DecodedSegment>, Error> {
            // The window start is encoded in the first sample (see `chunk`).
            let start_ms = samples[0] as u64;
            let len_ms = samples_to_ms(samples.len() as u64);
            self.decodes.lock().unwrap().push((samples.len(), start_ms));
            let end_ms = start_ms + len_ms;
            let mut out = Vec::new();
            let mut t = start_ms.div_ceil(500) * 500;
            while t + 500 <= end_ms {
                let index = t / 500;
                let word = if index == 3 && end_ms < 2_500 {
                    "tree".to_string()
                } else {
                    format!("w{index}")
                };
                out.push(DecodedSegment {
                    text: format!(" {word}"),
                    start_ms: t - start_ms,
                    end_ms: t + 500 - start_ms,
                });
                t += 500;
            }
            Ok(out)
        }
    }

    /// One 500 ms chunk of 16 kHz audio whose samples hold their absolute
    /// time in ms, so the fake decoder can see where its window starts.
    fn chunk(index: u64) -> Vec<f32> {
        (0..8_000u64)
            .map(|i| (index * 500 + i * 1000 / 16_000) as f32)
            .collect()
    }

    fn run(
        mode: TranscriptStreamingMode,
        chunks: u64,
    ) -> (Vec<TranscriptEvent>, Vec<TranscriptUpdate>, usize) {
        let (fake, mut state) = FakeWhisper::new(mode);
        let mut events = Vec::new();
        let mut updates = Vec::new();
        for i in 0..chunks {
            events.extend(fake.stream_audio(&mut state, &chunk(i), 16_000, 1).unwrap());
            updates.extend(state.take_updates());
        }
        events.extend(fake.finalize(&mut state).unwrap());
        updates.extend(state.take_updates());
        let max_window = fake
            .decodes
            .lock()
            .unwrap()
            .iter()
            .map(|d| d.0)
            .max()
            .unwrap();
        (events, updates, max_window)
    }

    #[test]
    fn legacy_events_keep_their_shape() {
        let (events, updates, _) = run(TranscriptStreamingMode::Off, 6);
        assert_eq!(events.first().unwrap().kind, "partial");
        assert!(events
            .iter()
            .any(|e| e.kind == "correction" && e.replaces.is_some()));
        let last = events.last().unwrap();
        assert_eq!(last.kind, "final");
        assert_eq!(last.text, "w0 w1 w2 w3 w4 w5");
        assert!(last.unstable_text.is_empty());
        assert!(events.windows(2).all(|w| w[1].revision >= w[0].revision));
        // `off`: only the shared final comes out.
        assert_eq!(updates.len(), 1);
        assert!(matches!(&updates[0], TranscriptUpdate::Final(f) if f.text == last.text));
    }

    #[test]
    fn shared_updates_replay_to_the_final() {
        let (events, updates, _) = run(TranscriptStreamingMode::Full, 6);
        let mut shown: Vec<String> = Vec::new();
        let mut final_text = None;
        for update in &updates {
            match update {
                TranscriptUpdate::Delta(e) | TranscriptUpdate::Correction(e) => {
                    assert_eq!(e.index + e.removed, shown.len());
                    shown.truncate(e.index);
                    shown.extend(e.tokens.iter().map(|t| t.text.clone()));
                }
                TranscriptUpdate::Final(f) => final_text = Some(f.text.clone()),
            }
        }
        assert!(updates
            .iter()
            .any(|u| matches!(u, TranscriptUpdate::Correction(_))));
        assert_eq!(final_text.as_deref(), Some(shown.join(" ").as_str()));
        assert_eq!(final_text.unwrap(), events.last().unwrap().text);
    }

    #[test]
    fn audio_after_a_final_starts_the_next_utterance() {
        let (fake, mut state) = FakeWhisper::new(TranscriptStreamingMode::Full);
        let mut updates = Vec::new();
        for i in 0..4 {
            fake.stream_audio(&mut state, &chunk(i), 16_000, 1).unwrap();
            updates.extend(state.take_updates());
        }
        fake.finalize(&mut state).unwrap();
        updates.extend(state.take_updates());
        // A pause closed the first utterance; speech goes on.
        for i in 4..8 {
            fake.stream_audio(&mut state, &chunk(i), 16_000, 1).unwrap();
            updates.extend(state.take_updates());
        }
        fake.finalize(&mut state).unwrap();
        updates.extend(state.take_updates());

        let finals: Vec<_> = updates
            .iter()
            .filter_map(|u| match u {
                TranscriptUpdate::Final(f) => Some(f),
                _ => None,
            })
            .collect();
        assert_eq!(finals.len(), 2, "{finals:?}");
        assert_eq!(finals[0].utterance_id, 0);
        assert_eq!(finals[1].utterance_id, 1);
        // The fake mishears word 3 until it has heard 1 s past it.
        assert_eq!(finals[0].text, "w0 w1 w2 tree");
        assert_eq!(finals[1].text, "w4 w5 w6 w7");
        // Times count from the start of the stream.
        assert!(finals[1].start_ms.unwrap() >= finals[0].end_ms.unwrap());
    }

    #[test]
    fn a_segment_without_a_span_gets_the_window_bounds() {
        let mut state = WhisperStreamingState::default();
        let options = StreamingOptions {
            transcript_updates: TranscriptStreamingMode::Full,
            ..StreamingOptions::default()
        };
        // 2 s of audio, decoded as one window starting at 0.
        state.push_audio(&vec![0.0; 32_000], 16_000, 1).unwrap();
        let window = state.decode_window(options, true).unwrap();
        // The decoder clamped both timestamps to the window end.
        state.apply_hypothesis(
            vec![DecodedSegment {
                text: " hello there".into(),
                start_ms: 2_000,
                end_ms: 2_000,
            }],
            window.start_ms,
            options,
            true,
        );
        let finals: Vec<_> = state
            .take_updates()
            .into_iter()
            .filter_map(|u| match u {
                TranscriptUpdate::Final(f) => Some(f),
                _ => None,
            })
            .collect();
        assert_eq!(finals.len(), 1);
        assert_eq!(finals[0].start_ms, Some(0));
        assert_eq!(finals[0].end_ms, Some(2_000));
    }

    #[test]
    fn long_stream_stays_within_the_window_bound() {
        let (events, _, max_window) = run(TranscriptStreamingMode::Stable, 24);
        assert!(
            max_window <= ms_to_samples(4_000) as usize,
            "window grew to {max_window}"
        );
        let expected: Vec<String> = (0..24).map(|i| format!("w{i}")).collect();
        assert_eq!(events.last().unwrap().text, expected.join(" "));
    }
}
