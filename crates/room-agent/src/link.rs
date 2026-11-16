//! The agent link's frames, as a member reads and writes them.
//!
//! The contract is owned by scaling's `agent-participants` capability
//! (`crates/runner/src/port_core.rs`); this module is the member side of it.
//!
//! Inbound, attributed to their author by the runtime:
//!
//! * a room-log line, `{"kind": "text" | "voice", "text", "seq", "history",
//!   "author_kind": "person" | "agent", "interrupted"}`;
//! * `{"kind": "history_end"}`, after the history lines;
//! * `{"kind": "reply_interrupted", "reply"}`: the room no longer wants that
//!   reply;
//! * plain Text: a person's live line (a transcript);
//! * anything else (`floor` events, including `refused` for `floor_busy`,
//!   `turn`, `presence`, `refused`) is ignored.
//!
//! Outbound: `presence` (`listening`, `thinking`, `idle`) and one sentence
//! line per finished sentence of a reply.

use remotemedia_types::RuntimeData;
use serde_json::{json, Value};

/// Frame kinds of the agent link.
pub mod kind {
    pub const TEXT: &str = "text";
    pub const VOICE: &str = "voice";
    pub const HISTORY_END: &str = "history_end";
    pub const REPLY_INTERRUPTED: &str = "reply_interrupted";
    pub const PRESENCE: &str = "presence";
    pub const FLOOR: &str = "floor";
    pub const TURN: &str = "turn";
    pub const REFUSED: &str = "refused";
}

/// A room-log line as the agent receives it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InLine {
    /// Trimmed, never empty.
    pub text: String,
    /// Its place in the room log, when it has one.
    pub seq: Option<u64>,
    /// Replayed history: context, never answered.
    pub history: bool,
    /// `author_kind` is `agent`.
    pub agent: bool,
    /// The line was cut off before it finished (an agent's reply).
    pub interrupted: bool,
}

/// What an agent reads from its link.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Inbound {
    Line(InLine),
    HistoryEnd,
    /// The room no longer wants this reply.
    Stop(String),
    /// Not for the agent to act on.
    Ignored,
}

/// Decodes one frame of the agent link.
pub fn decode(data: &RuntimeData) -> Inbound {
    let v = match data {
        RuntimeData::Json(v) => v,
        _ => {
            return match line_text(data) {
                Some(text) => Inbound::Line(InLine {
                    text,
                    seq: None,
                    history: false,
                    agent: false,
                    interrupted: false,
                }),
                None => Inbound::Ignored,
            }
        }
    };
    match v["kind"].as_str() {
        Some(kind::TEXT | kind::VOICE) => {
            let text = v["text"].as_str().unwrap_or("").trim();
            if text.is_empty() {
                return Inbound::Ignored;
            }
            Inbound::Line(InLine {
                text: text.to_string(),
                seq: v["seq"].as_u64(),
                history: v["history"] == true,
                agent: v["author_kind"] == "agent",
                interrupted: v["interrupted"] == true,
            })
        }
        Some(kind::HISTORY_END) => Inbound::HistoryEnd,
        Some(kind::REPLY_INTERRUPTED) => match v["reply"].as_str() {
            Some(reply) => Inbound::Stop(reply.to_string()),
            None => Inbound::Ignored,
        },
        _ => Inbound::Ignored,
    }
}

/// The words of a line outside a room: a transcript (Text) or a typed
/// message (`{"kind": "text", "text"}`). A `voice` line is not read here.
pub fn line_text(data: &RuntimeData) -> Option<String> {
    match data {
        RuntimeData::Text(t) => Some(t.trim().to_string()),
        RuntimeData::Json(v) if v["kind"] == kind::TEXT => {
            v["text"].as_str().map(|t| t.trim().to_string())
        }
        _ => None,
    }
    .filter(|t| !t.is_empty())
}

/// A bare name was heard; the agent waits for `to`'s question.
pub fn presence_listening(to: &str) -> Value {
    json!({ "kind": kind::PRESENCE, "state": "listening", "to": to })
}

/// A reply started generating. `line` is the `seq` of the line it answers,
/// left out when that line had none.
pub fn presence_thinking(reply: &str, to: &str, line: Option<u64>) -> Value {
    let mut presence =
        json!({ "kind": kind::PRESENCE, "state": "thinking", "reply": reply, "to": to });
    if let Some(seq) = line {
        presence["line"] = json!(seq);
    }
    presence
}

/// The reply ended.
pub fn presence_idle(reply: &str) -> Value {
    json!({ "kind": kind::PRESENCE, "state": "idle", "reply": reply })
}

/// One sentence of a reply: the room posts `text`, text-to-speech speaks
/// `say`. Extra fields (such as `sources`) may be added to the value.
pub fn sentence_line(sentence: &str, reply: &str) -> Value {
    json!({ "kind": kind::TEXT, "text": sentence, "say": sentence, "reply": reply })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(v: Value) -> Inbound {
        decode(&RuntimeData::Json(v))
    }

    fn line(text: &str, seq: Option<u64>, history: bool, agent: bool) -> Inbound {
        Inbound::Line(InLine {
            text: text.into(),
            seq,
            history,
            agent,
            interrupted: false,
        })
    }

    #[test]
    fn typed_and_spoken_lines() {
        assert_eq!(
            line_text(&RuntimeData::Text(" hi ".into())).as_deref(),
            Some("hi")
        );
        assert_eq!(
            line_text(&RuntimeData::Json(json!({"kind": "text", "text": "yo"}))).as_deref(),
            Some("yo")
        );
        assert!(line_text(&RuntimeData::Json(json!({"kind": "turn", "state": "end"}))).is_none());
        // Outside a room a `voice` line is not read.
        assert!(line_text(&RuntimeData::Json(json!({"kind": "voice", "text": "yo"}))).is_none());
    }

    #[test]
    fn room_lines() {
        assert_eq!(
            frame(json!({"kind": "text", "text": " hi ", "seq": 3, "author_kind": "person"})),
            line("hi", Some(3), false, false)
        );
        assert_eq!(
            frame(json!({"kind": "voice", "text": "hey pixel", "seq": 4})),
            line("hey pixel", Some(4), false, false)
        );
        assert_eq!(
            frame(
                json!({"kind": "voice", "text": "yo", "seq": 1, "history": true, "author_kind": "agent"})
            ),
            line("yo", Some(1), true, true)
        );
        assert_eq!(
            frame(json!({"kind": "text", "text": "hi"})),
            line("hi", None, false, false)
        );
        assert_eq!(
            decode(&RuntimeData::Text(" hi ".into())),
            line("hi", None, false, false)
        );
        assert_eq!(
            frame(json!({"kind": "text", "text": "  "})),
            Inbound::Ignored
        );
        assert_eq!(
            frame(json!({"kind": "history_end", "seq": 4})),
            Inbound::HistoryEnd
        );
    }

    #[test]
    fn interrupted_agent_history_line() {
        assert_eq!(
            frame(
                json!({"kind": "voice", "text": "The largest ocean is", "seq": 9,
                "history": true, "author_kind": "agent", "interrupted": true})
            ),
            Inbound::Line(InLine {
                text: "The largest ocean is".into(),
                seq: Some(9),
                history: true,
                agent: true,
                interrupted: true,
            })
        );
    }

    #[test]
    fn stop_and_ignored_frames() {
        assert_eq!(
            frame(json!({"kind": "reply_interrupted", "reply": "a1-2", "reason": "barge"})),
            Inbound::Stop("a1-2".into())
        );
        // Text of a reply refused for a busy floor is still posted.
        assert_eq!(
            frame(
                json!({"kind": "floor", "event": "refused", "reason": "floor_busy", "reply": "a1-2"})
            ),
            Inbound::Ignored
        );
        for event in ["granted", "waiting", "not_held"] {
            assert_eq!(
                frame(json!({"kind": "floor", "event": event, "reply": "a1-2"})),
                Inbound::Ignored
            );
        }
        assert_eq!(
            frame(json!({"kind": "turn", "state": "start"})),
            Inbound::Ignored
        );
        assert_eq!(
            frame(json!({"kind": "presence", "state": "thinking", "reply": "b-1"})),
            Inbound::Ignored
        );
        assert_eq!(
            frame(json!({"kind": "refused", "what": "text", "reason": "rate"})),
            Inbound::Ignored
        );
        assert_eq!(
            frame(json!({"kind": "reply_interrupted"})),
            Inbound::Ignored
        );
    }

    #[test]
    fn publications() {
        assert_eq!(
            presence_listening("ana"),
            json!({"kind": "presence", "state": "listening", "to": "ana"})
        );
        assert_eq!(
            presence_thinking("a1-2", "ana", Some(7)),
            json!({"kind": "presence", "state": "thinking", "reply": "a1-2", "to": "ana", "line": 7})
        );
        // A line without a `seq` gives no `line`.
        assert_eq!(
            presence_thinking("a1-2", "ana", None),
            json!({"kind": "presence", "state": "thinking", "reply": "a1-2", "to": "ana"})
        );
        assert_eq!(
            sentence_line("It is noon.", "a1-2"),
            json!({"kind": "text", "text": "It is noon.", "say": "It is noon.", "reply": "a1-2"})
        );
        assert_eq!(
            presence_idle("a1-2"),
            json!({"kind": "presence", "state": "idle", "reply": "a1-2"})
        );
    }
}
