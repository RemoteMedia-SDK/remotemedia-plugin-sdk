//! Model-agnostic transcript stabilizer.
//!
//! A transcription node feeds it whatever its model produces after each
//! inference pass: a full re-decode, a sliding-window decode, a vendor's
//! running partial, or appended text fragments. It gets back `delta`,
//! `correction` and `final` events. Correction detection does not depend on
//! timestamps. Hypotheses are aligned by matching words, and timestamps are
//! used only to drop text a sliding window has already moved past.
//!
//! The algorithm has three steps:
//!
//! 1. **Align.** Find where the committed text ends inside the new
//!    hypothesis. Whatever follows it is the hypothesis *tail*.
//! 2. **Agree.** LocalAgreement-n: a tail token is committed once the last
//!    `agreement_updates` tails share it as a prefix (compared without case or
//!    punctuation), except for the last `trailing_guard_tokens`. Committed
//!    tokens never change again within the utterance.
//! 3. **Diff.** Compare the tokens to display (committed only in `stable`
//!    mode; committed plus tail in `full` mode) with what was last emitted.
//!    An extension becomes a `delta` and anything else a `correction`.

use std::collections::VecDeque;

use super::types::{
    HypothesisScope, TranscriptEdit, TranscriptFinal, TranscriptStreamingConfig,
    TranscriptStreamingMode, TranscriptToken, TranscriptUpdate,
};

/// Longest committed n-gram used to anchor a hypothesis.
const MAX_ANCHOR_TOKENS: usize = 3;

/// Turns a stream of transcription hypotheses into delta, correction and
/// final events. Keep one per stream (per participant, per session).
#[derive(Debug, Clone)]
pub struct TranscriptStabilizer {
    config: TranscriptStreamingConfig,
    utterance_id: u64,
    revision: u64,
    committed: Vec<TranscriptToken>,
    unstable: Vec<TranscriptToken>,
    /// Recent hypothesis tails, relative to `committed`, for agreement.
    tails: VecDeque<Vec<TranscriptToken>>,
    /// Token list the client holds for this utterance.
    emitted: Vec<TranscriptToken>,
    emitted_stable: usize,
    committed_through_ms: Option<u64>,
    append_buffer: String,
    dirty: bool,
}

impl Default for TranscriptStabilizer {
    fn default() -> Self {
        Self::new(TranscriptStreamingConfig::default())
    }
}

impl TranscriptStabilizer {
    pub fn new(config: TranscriptStreamingConfig) -> Self {
        Self {
            config,
            utterance_id: 0,
            revision: 0,
            committed: Vec::new(),
            unstable: Vec::new(),
            tails: VecDeque::new(),
            emitted: Vec::new(),
            emitted_stable: 0,
            committed_through_ms: None,
            append_buffer: String::new(),
            dirty: false,
        }
    }

    pub fn config(&self) -> &TranscriptStreamingConfig {
        &self.config
    }

    pub fn mode(&self) -> TranscriptStreamingMode {
        self.config.mode
    }

    /// Replace the configuration. It applies from the next call. Hypotheses
    /// already gathered for agreement are kept.
    pub fn reconfigure(&mut self, config: TranscriptStreamingConfig) {
        self.config = config;
    }

    /// End time of the latest committed token that has a time, kept across
    /// utterances. An audio window can drop audio before this point.
    pub fn committed_through_ms(&self) -> Option<u64> {
        self.committed_through_ms
    }

    /// The tentative tail of the current utterance.
    pub fn unstable_text(&self) -> String {
        join(&self.unstable)
    }

    /// Number of tentative tokens.
    pub fn unstable_len(&self) -> usize {
        self.unstable.len()
    }

    /// Id of the utterance in progress.
    pub fn utterance_id(&self) -> u64 {
        self.utterance_id
    }

    /// Committed text of the current utterance.
    pub fn stable_text(&self) -> String {
        join(&self.committed)
    }

    /// Committed text plus the tentative tail.
    pub fn transcript(&self) -> String {
        join_all(&self.committed, &self.unstable)
    }

    /// Feed a plain-text hypothesis. Words are split on whitespace.
    pub fn push_text(&mut self, text: &str, scope: HypothesisScope) -> Vec<TranscriptUpdate> {
        self.push_tokens(tokenize(text), scope)
    }

    /// Feed a token hypothesis. Timestamps and confidences are optional. A
    /// token holding several words, such as a timed phrase segment, is split,
    /// and its time span is divided evenly among the words.
    pub fn push_tokens(
        &mut self,
        tokens: Vec<TranscriptToken>,
        scope: HypothesisScope,
    ) -> Vec<TranscriptUpdate> {
        self.dirty = true;
        let tail = self.align(clean(tokens), scope);
        self.agree(tail);
        self.emit_diff().into_iter().collect()
    }

    /// Feed a fragment from a model that only appends text, such as a
    /// realtime API's `transcript.delta`. Fragments are concatenated as
    /// given, so include their leading spaces.
    pub fn push_append(&mut self, fragment: &str) -> Vec<TranscriptUpdate> {
        self.append_buffer.push_str(fragment);
        let text = self.append_buffer.clone();
        self.push_text(&text, HypothesisScope::Utterance)
    }

    /// Commit the whole tentative tail. A `Window` model without timestamps
    /// calls this before sliding its window past unstable text.
    pub fn commit_unstable(&mut self) -> Vec<TranscriptUpdate> {
        self.commit_unstable_prefix(self.unstable.len())
    }

    /// Commit the first `count` tentative tokens, for example when a window
    /// bound forces out the oldest words before they reach agreement.
    pub fn commit_unstable_prefix(&mut self, count: usize) -> Vec<TranscriptUpdate> {
        let count = count.min(self.unstable.len());
        if count == 0 {
            return Vec::new();
        }
        let forced: Vec<_> = self.unstable.drain(..count).collect();
        self.commit(forced);
        self.tails.clear();
        self.emit_diff().into_iter().collect()
    }

    /// Commit tentative tokens that end at or before `ms`. A `Window` model
    /// with timestamps calls this when it trims audio up to `ms`.
    pub fn commit_until_ms(&mut self, ms: u64) -> Vec<TranscriptUpdate> {
        let count = self
            .unstable
            .iter()
            .take_while(|t| t.end_ms.is_some_and(|end| end <= ms))
            .count();
        self.commit_unstable_prefix(count)
    }

    /// Close the utterance by committing the current tail.
    pub fn finalize(&mut self) -> Vec<TranscriptUpdate> {
        let tail = std::mem::take(&mut self.unstable);
        self.commit(tail);
        self.close()
    }

    /// Close the utterance with the model's final hypothesis.
    ///
    /// In `off` mode nothing was shown yet, so an `Utterance`-scope final is
    /// taken word for word. In other modes, committed tokens stay as they
    /// are and only the aligned tail is taken from the final hypothesis.
    pub fn finalize_tokens(
        &mut self,
        tokens: Vec<TranscriptToken>,
        scope: HypothesisScope,
    ) -> Vec<TranscriptUpdate> {
        self.dirty = true;
        let tokens = clean(tokens);
        if self.config.mode.is_off() && scope == HypothesisScope::Utterance {
            self.committed.clear();
            self.commit(tokens);
        } else {
            let tail = self.align(tokens, scope);
            self.commit(tail);
        }
        self.unstable.clear();
        self.close()
    }

    pub fn finalize_text(&mut self, text: &str, scope: HypothesisScope) -> Vec<TranscriptUpdate> {
        self.finalize_tokens(tokenize(text), scope)
    }

    /// Drop the current utterance without a final, for example on barge-in
    /// or cancel. The committed-time watermark is kept.
    pub fn reset(&mut self) {
        if self.dirty {
            self.utterance_id += 1;
        }
        self.clear_utterance();
    }

    /// Clear everything, including the time watermark, for a new stream.
    pub fn reset_stream(&mut self) {
        self.reset();
        self.committed_through_ms = None;
    }

    // ── internals ────────────────────────────────────────────────────────

    fn align(
        &self,
        mut tokens: Vec<TranscriptToken>,
        scope: HypothesisScope,
    ) -> Vec<TranscriptToken> {
        // In a window, the committed-time watermark estimates how many leading
        // tokens were already committed. Timestamps can be coarse (a phrase or
        // a whole window spread evenly over its words), so that estimate only
        // says where to look for the text overlap. Tokens are dropped by time
        // only when no text anchor is found.
        let watermark = match scope {
            HypothesisScope::Window if tokens.iter().any(|t| t.end_ms.is_some()) => {
                self.committed_through_ms
            }
            _ => None,
        };
        let drop_by_time = |mut tokens: Vec<TranscriptToken>| {
            if let Some(through) = watermark {
                tokens.retain(|t| t.end_ms.is_none_or(|end| end > through));
            }
            tokens
        };
        if self.committed.is_empty() {
            return drop_by_time(tokens);
        }
        let expected = watermark.map(|through| {
            tokens
                .iter()
                .filter(|t| t.end_ms.is_some_and(|end| end <= through))
                .count()
        });
        match anchor_end(&self.committed, &tokens, scope, expected) {
            Some(end) => tokens.split_off(end),
            // Nothing matched. An utterance hypothesis still repeats the
            // committed words, so skip that many. A window falls back to its
            // timestamps, or, without any, starts after the committed text.
            None => match scope {
                HypothesisScope::Utterance => {
                    let cut = self.committed.len().min(tokens.len());
                    tokens.split_off(cut)
                }
                HypothesisScope::Window => drop_by_time(tokens),
            },
        }
    }

    fn agree(&mut self, tail: Vec<TranscriptToken>) {
        let needed = self.config.agreement_updates.max(1);
        let guard = self.config.trailing_guard_tokens;
        let commit_count = if needed == 1 {
            self.tails.clear();
            tail.len().saturating_sub(guard)
        } else {
            self.tails.push_back(tail.clone());
            while self.tails.len() > needed {
                self.tails.pop_front();
            }
            if self.tails.len() < needed {
                0
            } else {
                common_prefix_len(&self.tails).saturating_sub(guard)
            }
        };

        let mut tail = tail;
        if commit_count > 0 {
            let agreed: Vec<_> = tail.drain(..commit_count).collect();
            self.commit(agreed);
            for previous in &mut self.tails {
                let n = commit_count.min(previous.len());
                previous.drain(..n);
            }
        }
        self.unstable = tail;
    }

    fn commit(&mut self, tokens: Vec<TranscriptToken>) {
        for token in tokens {
            if let Some(end) = token.end_ms {
                self.committed_through_ms =
                    Some(self.committed_through_ms.map_or(end, |t| t.max(end)));
            }
            self.committed.push(token);
        }
    }

    fn emit_diff(&mut self) -> Option<TranscriptUpdate> {
        let display: Vec<TranscriptToken> = match self.config.mode {
            TranscriptStreamingMode::Off => return None,
            TranscriptStreamingMode::Stable => self.committed.clone(),
            TranscriptStreamingMode::Full => {
                let mut all = self.committed.clone();
                all.extend(self.unstable.iter().cloned());
                all
            }
        };
        let stable = self.committed.len();
        let prefix = self
            .emitted
            .iter()
            .zip(&display)
            .take_while(|(a, b)| a.text == b.text)
            .count();
        let removed = self.emitted.len() - prefix;
        let unchanged = removed == 0 && display.len() == prefix;
        if unchanged && stable == self.emitted_stable {
            return None;
        }

        let inserted = display[prefix..].to_vec();
        self.revision += 1;
        let edit = TranscriptEdit {
            utterance_id: self.utterance_id,
            revision: self.revision,
            index: prefix,
            removed,
            text: join(&inserted),
            tokens: inserted,
            stable_tokens: stable,
            transcript: join(&display),
        };
        self.emitted = display;
        self.emitted_stable = stable;
        Some(if removed == 0 {
            TranscriptUpdate::Delta(edit)
        } else {
            TranscriptUpdate::Correction(edit)
        })
    }

    fn close(&mut self) -> Vec<TranscriptUpdate> {
        let mut out: Vec<_> = self.emit_diff().into_iter().collect();
        if !self.committed.is_empty() {
            self.revision += 1;
            out.push(TranscriptUpdate::Final(TranscriptFinal {
                utterance_id: self.utterance_id,
                revision: self.revision,
                text: join(&self.committed),
                start_ms: self.committed.first().and_then(|t| t.start_ms),
                end_ms: self.committed.last().and_then(|t| t.end_ms),
                tokens: std::mem::take(&mut self.committed),
            }));
        }
        if self.dirty || !out.is_empty() {
            self.utterance_id += 1;
        }
        self.clear_utterance();
        out
    }

    fn clear_utterance(&mut self) {
        self.committed.clear();
        self.unstable.clear();
        self.tails.clear();
        self.emitted.clear();
        self.emitted_stable = 0;
        self.append_buffer.clear();
        self.dirty = false;
    }
}

/// Find the index in `tokens` just past where the committed text ends.
///
/// The last 3, 2 or 1 committed tokens are searched for, longest first, and
/// the match closest to an expected end within a radius wins:
/// - `Utterance`: the expected end is `committed.len()`.
/// - `Window` with timestamps: the expected end is `time_estimate`, the
///   number of tokens that end before the committed watermark. Coarse
///   timestamps can be several words off, so the radius is wide.
/// - `Window` without timestamps: no estimate, so the earliest match wins.
///
/// A single-word match must sit close to the expected end (or, with no
/// estimate, near the start), so a common word like "the" elsewhere in the
/// hypothesis does not anchor it.
fn anchor_end(
    committed: &[TranscriptToken],
    tokens: &[TranscriptToken],
    scope: HypothesisScope,
    time_estimate: Option<usize>,
) -> Option<usize> {
    let n = committed.len();
    let keys: Vec<String> = tokens.iter().map(|t| compare_key(&t.text)).collect();
    for g in (1..=MAX_ANCHOR_TOKENS.min(n)).rev() {
        let needle: Vec<String> = committed[n - g..]
            .iter()
            .map(|t| compare_key(&t.text))
            .collect();
        if keys.len() < g {
            continue;
        }
        let ends = (0..=keys.len() - g)
            .filter(|&i| keys[i..i + g] == needle[..])
            .map(|i| i + g);
        let target = match scope {
            HypothesisScope::Utterance => Some((n, 3 + n / 4)),
            HypothesisScope::Window => time_estimate.map(|e| (e, 3 + tokens.len() / 3)),
        };
        let best = match target {
            Some((expected, radius)) => {
                let radius = if g == 1 && scope == HypothesisScope::Window {
                    radius.min(2)
                } else {
                    radius
                };
                ends.filter(|&e| e.abs_diff(expected) <= radius)
                    .min_by_key(|&e| (e.abs_diff(expected), std::cmp::Reverse(e)))
            }
            None => {
                let mut ends = ends;
                let limit = if g == 1 { g + 3 } else { usize::MAX };
                ends.next().filter(|&e| e <= limit)
            }
        };
        if best.is_some() {
            return best;
        }
    }
    None
}

fn common_prefix_len(tails: &VecDeque<Vec<TranscriptToken>>) -> usize {
    let Some(first) = tails.front() else {
        return 0;
    };
    (0..first.len())
        .take_while(|&i| {
            let key = compare_key(&first[i].text);
            tails
                .iter()
                .all(|t| t.get(i).is_some_and(|tok| compare_key(&tok.text) == key))
        })
        .count()
}

/// Compare words without case or punctuation, so "Hello," matches "hello".
/// A token made only of punctuation compares by its lowercased text.
pub(crate) fn compare_key(word: &str) -> String {
    let key: String = word
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '\'')
        .flat_map(char::to_lowercase)
        .collect();
    if key.is_empty() {
        word.to_lowercase()
    } else {
        key
    }
}

fn tokenize(text: &str) -> Vec<TranscriptToken> {
    text.split_whitespace().map(TranscriptToken::new).collect()
}

/// Trim tokens, drop empty ones, and split multi-word tokens, dividing a
/// timed token's span evenly among its words.
fn clean(tokens: Vec<TranscriptToken>) -> Vec<TranscriptToken> {
    let mut out = Vec::with_capacity(tokens.len());
    for token in tokens {
        let words: Vec<&str> = token.text.split_whitespace().collect();
        match words.len() {
            0 => {}
            1 => out.push(TranscriptToken {
                text: words[0].to_string(),
                ..token
            }),
            count => {
                let span = match (token.start_ms, token.end_ms) {
                    (Some(s), Some(e)) => Some((s, e.saturating_sub(s))),
                    _ => None,
                };
                let count = count as u64;
                for (i, word) in words.iter().enumerate() {
                    let i = i as u64;
                    out.push(TranscriptToken {
                        text: (*word).to_string(),
                        start_ms: span.map(|(s, d)| s + d * i / count).or(token.start_ms),
                        end_ms: span.map(|(s, d)| s + d * (i + 1) / count).or(token.end_ms),
                        confidence: token.confidence,
                    });
                }
            }
        }
    }
    out
}

fn join(tokens: &[TranscriptToken]) -> String {
    tokens
        .iter()
        .map(|t| t.text.as_str())
        .collect::<Vec<_>>()
        .join(" ")
}

fn join_all(a: &[TranscriptToken], b: &[TranscriptToken]) -> String {
    a.iter()
        .chain(b)
        .map(|t| t.text.as_str())
        .collect::<Vec<_>>()
        .join(" ")
}
