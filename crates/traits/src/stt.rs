//! Shared speech-to-text (STT) streaming contract for Whisper-family nodes.
//!
//! Both the Candle-backed `WhisperNode` and the whisper.cpp-backed loadable
//! plugin implement the [`Whisper`] trait defined here. The partial-result
//! state machine ([`WhisperStreamingState`]) and the wire types
//! ([`TranscriptEvent`], [`DecodedSegment`], [`StreamingOptions`]) are shared
//! so every backend emits byte-for-byte identical streaming events — a
//! downstream consumer cannot tell Candle from whisper.cpp apart.
//!
//! The algorithm is lifted from `candle-nodes/src/whisper/streaming.rs` and
//! kept backend-agnostic: it depends only on `remotemedia_types::Error` and
//! `std`. The only backend-specific piece is [`Whisper::decode_window`], which
//! runs one inference pass over a normalized 16 kHz mono decode window and
//! returns the raw segments. [`Whisper::stream_audio`] / [`Whisper::finalize`]
//! drive the shared state machine on top of it.

use std::collections::VecDeque;

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
}

impl Default for StreamingOptions {
    fn default() -> Self {
        Self {
            decode_interval_ms: 2_000,
            overlap_ms: 750,
            max_window_ms: 30_000,
            agreement_updates: 2,
            trailing_guard_words: 2,
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
            return Err("streaming_overlap_ms must be smaller than streaming_max_window_ms".to_string());
        }
        if self.streaming_decode_interval_ms > self.streaming_max_window_ms {
            return Err("streaming_decode_interval_ms must not exceed streaming_max_window_ms".to_string());
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
    fn push(&mut self, samples: &[f32], sample_rate: u32, channels: u32) -> Result<Vec<f32>, Error> {
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

/// Incremental partial-result accumulator for streaming Whisper.
///
/// Mirrors `candle-nodes::whisper::WhisperStreamingState` exactly (same
/// revision/stable/unstable/correction semantics) so every backend produces
/// identical events.
pub struct WhisperStreamingState {
    normalizer: StreamingNormalizer,
    audio: VecDeque<f32>,
    buffer_start_sample: u64,
    total_samples: u64,
    new_samples_since_decode: usize,
    hypotheses: VecDeque<Vec<DecodedSegment>>,
    committed: Vec<DecodedSegment>,
    unstable: Vec<DecodedSegment>,
    committed_through_ms: u64,
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
            hypotheses: VecDeque::new(),
            committed: Vec::new(),
            unstable: Vec::new(),
            committed_through_ms: 0,
            revision: 0,
            last_emitted_text: None,
            finalized: false,
        }
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
            return Err(invalid_input(
                "audio pushed after final flush".to_string(),
            ));
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
    pub fn decode_window(&mut self, options: StreamingOptions, force: bool) -> Option<DecodeWindow> {
        if self.audio.is_empty() {
            return None;
        }
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

    /// Apply a decode hypothesis (segments already offset by `window_start_ms`)
    /// and return the next event, if any.
    pub fn apply_hypothesis(
        &mut self,
        relative_segments: Vec<DecodedSegment>,
        window_start_ms: u64,
        options: StreamingOptions,
        final_result: bool,
    ) -> Option<TranscriptEvent> {
        let mut segments: Vec<DecodedSegment> = relative_segments
            .into_iter()
            .map(|mut segment| {
                segment.text = normalize_text(&segment.text);
                segment.start_ms += window_start_ms;
                segment.end_ms += window_start_ms;
                segment
            })
            .flat_map(split_segment_words)
            .filter(|segment| segment.end_ms > self.committed_through_ms && !segment.text.is_empty())
            .collect();
        segments.sort_by_key(|segment| (segment.start_ms, segment.end_ms));
        let overlap = committed_prefix_overlap(&self.committed, &segments);
        if overlap > 0 {
            segments.drain(..overlap);
        }

        if final_result {
            self.commit_segments(segments);
            self.unstable.clear();
            self.hypotheses.clear();
            self.finalized = true;
            let event = self.make_event("final", true);
            self.trim_audio(options, true);
            return Some(event);
        }

        self.hypotheses.push_back(segments.clone());
        while self.hypotheses.len() > options.agreement_updates.max(2) {
            self.hypotheses.pop_front();
        }

        if self.hypotheses.len() >= options.agreement_updates.max(2) {
            let common_words = common_prefix_words(self.hypotheses.iter());
            let eligible_words = common_words.saturating_sub(options.trailing_guard_words);
            let commit_count = complete_segments_within_words(&segments, eligible_words);
            if commit_count > 0 {
                self.commit_segments(segments.drain(..commit_count).collect());
                self.hypotheses.clear();
            }
        }

        self.unstable = segments;
        self.force_commit_for_bound(options);
        self.trim_audio(options, false);

        let text = self.full_text();
        if self.last_emitted_text.as_deref() == Some(text.as_str()) || text.is_empty() {
            return None;
        }
        let kind = if self.last_emitted_text.is_some() {
            "correction"
        } else {
            "partial"
        };
        Some(self.make_event(kind, false))
    }

    fn force_commit_for_bound(&mut self, options: StreamingOptions) {
        let max_window_start_ms =
            samples_to_ms(self.total_samples.saturating_sub(ms_to_samples(options.max_window_ms as u64)));
        if max_window_start_ms <= samples_to_ms(self.buffer_start_sample) {
            return;
        }

        let required_commit_ms = max_window_start_ms + options.overlap_ms as u64;
        let mut count = self
            .unstable
            .iter()
            .take_while(|segment| segment.end_ms <= required_commit_ms)
            .count();
        if count == 0 && !self.unstable.is_empty() {
            count = 1;
        }
        if count > 0 {
            let forced: Vec<_> = self.unstable.drain(..count).collect();
            self.commit_segments(forced);
            self.hypotheses.clear();
        }
    }

    fn commit_segments(&mut self, segments: Vec<DecodedSegment>) {
        for segment in segments {
            self.committed_through_ms = self.committed_through_ms.max(segment.end_ms);
            self.committed.push(segment);
        }
    }

    fn trim_audio(&mut self, options: StreamingOptions, final_result: bool) {
        let target_ms = if final_result {
            samples_to_ms(self.total_samples)
        } else {
            self.committed_through_ms
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

    fn make_event(&mut self, kind: &str, final_result: bool) -> TranscriptEvent {
        let text = self.full_text();
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
            stable_text: join_segments(&self.committed),
            unstable_text: if final_result {
                String::new()
            } else {
                join_segments(&self.unstable)
            },
            audio_start_ms: samples_to_ms(self.buffer_start_sample),
            audio_end_ms: samples_to_ms(self.total_samples),
        }
    }

    fn full_text(&self) -> String {
        join_nonempty(&join_segments(&self.committed), &join_segments(&self.unstable))
    }
}

/// One decode window handed to the backend.
pub struct DecodeWindow {
    pub samples: Vec<f32>,
    pub start_ms: u64,
}

fn common_prefix_words<'a>(hypotheses: impl Iterator<Item = &'a Vec<DecodedSegment>>) -> usize {
    let words: Vec<Vec<&str>> = hypotheses
        .map(|segments| {
            segments
                .iter()
                .flat_map(|segment| segment.text.split_whitespace())
                .collect()
        })
        .collect();
    let Some(first) = words.first() else {
        return 0;
    };
    (0..first.len())
        .take_while(|&index| words.iter().all(|candidate| candidate.get(index) == first.get(index)))
        .count()
}

fn complete_segments_within_words(segments: &[DecodedSegment], word_limit: usize) -> usize {
    let mut words = 0;
    segments
        .iter()
        .take_while(|segment| {
            words += segment.text.split_whitespace().count();
            words <= word_limit
        })
        .count()
}

fn committed_prefix_overlap(committed: &[DecodedSegment], candidate: &[DecodedSegment]) -> usize {
    let limit = committed.len().min(candidate.len());
    (1..=limit)
        .rev()
        .find(|&length| {
            committed[committed.len() - length..]
                .iter()
                .zip(&candidate[..length])
                .all(|(left, right)| comparison_word(&left.text) == comparison_word(&right.text))
        })
        .unwrap_or(0)
}

fn comparison_word(word: &str) -> String {
    word.chars()
        .filter(|character| character.is_alphanumeric() || *character == '\'')
        .flat_map(char::to_lowercase)
        .collect()
}

fn join_segments(segments: &[DecodedSegment]) -> String {
    segments
        .iter()
        .map(|segment| segment.text.as_str())
        .filter(|text| !text.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whisper timestamps delimit phrases rather than individual words. Local
/// agreement operates at word boundaries, so distribute each word across its
/// model-derived phrase interval.
fn split_segment_words(segment: DecodedSegment) -> Vec<DecodedSegment> {
    let words: Vec<_> = segment.text.split_whitespace().collect();
    if words.len() <= 1 {
        return vec![segment];
    }
    let word_count = words.len() as u64;
    let duration = segment.end_ms.saturating_sub(segment.start_ms);
    words
        .into_iter()
        .enumerate()
        .map(|(index, word)| DecodedSegment {
            text: word.to_string(),
            start_ms: segment.start_ms + duration * index as u64 / word_count,
            end_ms: segment.start_ms + duration * (index + 1) as u64 / word_count,
        })
        .collect()
}

fn join_nonempty(left: &str, right: &str) -> String {
    match (left.is_empty(), right.is_empty()) {
        (true, _) => right.to_string(),
        (_, true) => left.to_string(),
        _ => format!("{left} {right}"),
    }
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
