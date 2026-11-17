use super::*;
use serde_json::{json, Value};

fn cfg(mode: TranscriptStreamingMode, agreement: usize, guard: usize) -> TranscriptStreamingConfig {
    TranscriptStreamingConfig {
        mode,
        agreement_updates: agreement,
        trailing_guard_tokens: guard,
    }
}

/// Apply the edits of one utterance to a token list, as a client would.
fn replay(updates: &[TranscriptUpdate]) -> (Vec<String>, Option<String>) {
    let mut shown: Vec<String> = Vec::new();
    let mut final_text = None;
    for update in updates {
        match update {
            TranscriptUpdate::Delta(e) | TranscriptUpdate::Correction(e) => {
                assert_eq!(e.index + e.removed, shown.len(), "edit must reach the end");
                shown.truncate(e.index);
                shown.extend(e.tokens.iter().map(|t| t.text.clone()));
                assert_eq!(shown.join(" "), e.transcript);
            }
            TranscriptUpdate::Final(f) => final_text = Some(f.text.clone()),
        }
    }
    (shown, final_text)
}

#[test]
fn off_mode_emits_only_a_plain_text_final() {
    let mut s = TranscriptStabilizer::new(cfg(TranscriptStreamingMode::Off, 2, 1));
    assert!(s
        .push_text("hello wor", HypothesisScope::Utterance)
        .is_empty());
    assert!(s
        .push_text("hello world", HypothesisScope::Utterance)
        .is_empty());
    let out = s.finalize_text("Hello world.", HypothesisScope::Utterance);
    assert_eq!(out.len(), 1);
    let rendered = s.config().render(&out[0]).unwrap();
    assert!(matches!(rendered, remotemedia_types::RuntimeData::Text(ref t) if t == "Hello world."));
}

#[test]
fn full_mode_corrects_tentative_words_without_timestamps() {
    let mut s = TranscriptStabilizer::new(cfg(TranscriptStreamingMode::Full, 2, 1));
    let mut all = Vec::new();
    all.extend(s.push_text("I scream", HypothesisScope::Utterance));
    all.extend(s.push_text("ice cream is", HypothesisScope::Utterance));
    all.extend(s.push_text("ice cream is great", HypothesisScope::Utterance));
    all.extend(s.push_text("ice cream is great today", HypothesisScope::Utterance));
    all.extend(s.finalize());

    assert!(matches!(all[0], TranscriptUpdate::Delta(_)));
    let TranscriptUpdate::Correction(c) = &all[1] else {
        panic!("expected a correction, got {:?}", all[1]);
    };
    assert_eq!((c.index, c.removed), (0, 2));
    assert_eq!(c.text, "ice cream is");

    let (shown, final_text) = replay(&all);
    assert_eq!(final_text.as_deref(), Some("ice cream is great today"));
    assert_eq!(shown.join(" "), "ice cream is great today");
}

#[test]
fn stable_mode_never_retracts() {
    let mut s = TranscriptStabilizer::new(cfg(TranscriptStreamingMode::Stable, 2, 1));
    let mut all = Vec::new();
    for h in [
        "the cat",
        "the cap sat",
        "the cat sat on",
        "the cat sat on the mat",
    ] {
        all.extend(s.push_text(h, HypothesisScope::Utterance));
    }
    all.extend(s.finalize());
    assert!(all
        .iter()
        .all(|u| !matches!(u, TranscriptUpdate::Correction(_))));
    let (shown, final_text) = replay(&all);
    assert_eq!(shown.join(" "), final_text.unwrap());
}

#[test]
fn committed_words_survive_a_rewrite_of_the_prefix() {
    // The model rewrites a committed word. The committed text is kept and the
    // rest of the hypothesis is lined up after it by the n-gram anchor.
    let mut s = TranscriptStabilizer::new(cfg(TranscriptStreamingMode::Full, 1, 0));
    s.push_text("we are gonna go", HypothesisScope::Utterance);
    assert_eq!(s.stable_text(), "we are gonna go");
    s.push_text("we are going to go home", HypothesisScope::Utterance);
    assert_eq!(s.transcript(), "we are gonna go home");
}

#[test]
fn window_scope_uses_timestamps_when_present() {
    let mut s = TranscriptStabilizer::new(cfg(TranscriptStreamingMode::Full, 1, 0));
    s.push_tokens(
        vec![
            TranscriptToken::timed("one", 0, 400),
            TranscriptToken::timed("two", 400, 800),
        ],
        HypothesisScope::Window,
    );
    // The window slid: it starts at 600 ms, and "two" comes back as "too".
    let out = s.push_tokens(
        vec![
            TranscriptToken::timed("too", 600, 800),
            TranscriptToken::timed("three", 800, 1200),
        ],
        HypothesisScope::Window,
    );
    assert_eq!(s.transcript(), "one two three");
    assert!(matches!(out[0], TranscriptUpdate::Delta(ref d) if d.text == "three"));
}

#[test]
fn window_scope_without_timestamps_aligns_on_overlap() {
    let mut s = TranscriptStabilizer::new(cfg(TranscriptStreamingMode::Full, 1, 0));
    s.push_text("the quick brown fox", HypothesisScope::Window);
    s.push_text("brown fox jumps over", HypothesisScope::Window);
    assert_eq!(s.transcript(), "the quick brown fox jumps over");
}

#[test]
fn phrase_tokens_are_split_with_timing() {
    let mut s = TranscriptStabilizer::new(cfg(TranscriptStreamingMode::Full, 1, 0));
    let out = s.push_tokens(
        vec![TranscriptToken::timed(" hello there world ", 0, 900)],
        HypothesisScope::Utterance,
    );
    let TranscriptUpdate::Delta(d) = &out[0] else {
        panic!()
    };
    let spans: Vec<_> = d.tokens.iter().map(|t| (t.start_ms, t.end_ms)).collect();
    assert_eq!(
        spans,
        vec![
            (Some(0), Some(300)),
            (Some(300), Some(600)),
            (Some(600), Some(900))
        ]
    );
}

#[test]
fn append_fragments_and_utterance_ids() {
    let mut s = TranscriptStabilizer::new(cfg(TranscriptStreamingMode::Stable, 1, 1));
    let mut all = Vec::new();
    for f in ["Hel", "lo", " there", " friend"] {
        all.extend(s.push_append(f));
    }
    all.extend(s.finalize());
    let (_, final_text) = replay(&all);
    assert_eq!(final_text.as_deref(), Some("Hello there friend"));
    assert!(all.iter().all(|u| u.utterance_id() == 0));

    let next = s.finalize_text("second", HypothesisScope::Utterance);
    assert_eq!(next.last().unwrap().utterance_id(), 1);
    let revisions: Vec<_> = all.iter().chain(&next).map(|u| u.revision()).collect();
    assert!(revisions.windows(2).all(|w| w[1] == w[0] + 1));
}

#[test]
fn empty_utterance_emits_nothing() {
    let mut s = TranscriptStabilizer::new(cfg(TranscriptStreamingMode::Full, 2, 1));
    assert!(s.push_text("   ", HypothesisScope::Utterance).is_empty());
    assert!(s.finalize().is_empty());
}

#[test]
fn wire_shape_is_tagged() {
    let mut s = TranscriptStabilizer::new(cfg(TranscriptStreamingMode::Full, 1, 0));
    let out = s.push_text("hi", HypothesisScope::Utterance);
    assert_eq!(
        out[0].to_json(),
        json!({
            "type": "delta", "utterance_id": 0, "revision": 1, "index": 0, "removed": 0,
            "tokens": [{"text": "hi"}], "text": "hi", "stable_tokens": 1, "transcript": "hi"
        })
    );
}

// ── shared conformance fixture (also run by the Python port) ─────────────

const FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/stt_stabilizer_cases.json"
);

fn scope_of(step: &Value) -> HypothesisScope {
    serde_json::from_value(step.get("scope").cloned().unwrap_or(json!("utterance"))).unwrap()
}

fn run_step(s: &mut TranscriptStabilizer, step: &Value) -> Vec<TranscriptUpdate> {
    let text = || step["text"].as_str().unwrap();
    let tokens = || serde_json::from_value::<Vec<TranscriptToken>>(step["tokens"].clone()).unwrap();
    match step["op"].as_str().unwrap() {
        "push_text" => s.push_text(text(), scope_of(step)),
        "push_tokens" => s.push_tokens(tokens(), scope_of(step)),
        "push_append" => s.push_append(text()),
        "commit_unstable" => s.commit_unstable(),
        "commit_unstable_prefix" => {
            s.commit_unstable_prefix(step["count"].as_u64().unwrap() as usize)
        }
        "commit_until_ms" => s.commit_until_ms(step["ms"].as_u64().unwrap()),
        "finalize" => s.finalize(),
        "finalize_text" => s.finalize_text(text(), scope_of(step)),
        "finalize_tokens" => s.finalize_tokens(tokens(), scope_of(step)),
        "reset" => {
            s.reset();
            Vec::new()
        }
        other => panic!("unknown op {other}"),
    }
}

/// Runs every case in the fixture and compares the events. With
/// `STT_FIXTURE_UPDATE=1` it writes the observed events back as `expected`
/// instead. Review that diff by hand: the Python port must match it.
#[test]
fn conformance_fixture() {
    let raw = std::fs::read_to_string(FIXTURE).expect("fixture present");
    let mut doc: Value = serde_json::from_str(&raw).unwrap();
    let update = std::env::var("STT_FIXTURE_UPDATE").is_ok_and(|v| v == "1");
    for case in doc["cases"].as_array_mut().unwrap() {
        let config: TranscriptStreamingConfig =
            serde_json::from_value(case["config"].clone()).unwrap();
        let mut s = TranscriptStabilizer::new(config);
        let observed: Vec<Value> = case["steps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|step| serde_json::to_value(run_step(&mut s, step)).unwrap())
            .collect();
        if update {
            case["expected"] = Value::Array(observed);
        } else {
            assert_eq!(
                Value::Array(observed),
                case["expected"],
                "case {}",
                case["name"]
            );
        }
    }
    if update {
        let pretty = serde_json::to_string_pretty(&doc).unwrap() + "\n";
        std::fs::write(FIXTURE, pretty).unwrap();
    }
}
