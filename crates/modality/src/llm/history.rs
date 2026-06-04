//! Per-session conversation history for chat-completion-shaped LLMs.
//!
//! Each entry stores a fully-shaped `serde_json::Value` chat message
//! (role, content, optional `tool_calls` / `tool_call_id` / `name`) so
//! the backend can drop entries straight into the `messages` array of
//! the next request — no re-shaping. That matters for tools-only
//! assistant turns: an assistant message carrying `tool_calls` MUST be
//! paired with one `tool` role message per call, or modern OpenAI-shape
//! servers reject the next request with a 400 over dangling
//! `tool_call_id` references.
//!
//! The window is counted in **user turns**, not raw entries, so a
//! single turn's `(assistant + N tool results)` group never gets sliced
//! apart at the front of the window.

use serde_json::Value;

/// One chat-completions message in per-session history.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    /// Pre-shaped message ready to drop into the `messages` array.
    pub message: Value,
}

impl HistoryEntry {
    pub fn user(content: &str) -> Self {
        Self {
            message: serde_json::json!({ "role": "user", "content": content }),
        }
    }

    pub fn user_message(message: Value) -> Self {
        // Already pre-shaped {role:"user", content: ...}. Used by the
        // multimodal node which builds a content-parts array upstream.
        Self { message }
    }

    pub fn assistant_text(content: &str) -> Self {
        Self {
            message: serde_json::json!({ "role": "assistant", "content": content }),
        }
    }

    /// Assistant message with `tool_calls` populated. `content` is sent
    /// as `null` when the model emitted no plain text alongside its
    /// calls, matching what the SSE stream returned. Stricter servers
    /// reject a string `content` paired with `tool_calls` — `null` is
    /// the safe shape.
    pub fn assistant_with_tool_calls(content: Option<&str>, tool_calls: Value) -> Self {
        let content_value = match content {
            Some(s) if !s.is_empty() => Value::String(s.to_string()),
            _ => Value::Null,
        };
        Self {
            message: serde_json::json!({
                "role": "assistant",
                "content": content_value,
                "tool_calls": tool_calls,
            }),
        }
    }

    /// Synthetic tool-result paired with a prior assistant `tool_calls`
    /// entry. Side-effect tools have empty `content` — the message
    /// MUST exist or the next request 400s.
    pub fn tool_result(tool_call_id: &str, name: &str, content: &str) -> Self {
        Self {
            message: serde_json::json!({
                "role": "tool",
                "tool_call_id": tool_call_id,
                "name": name,
                "content": content,
            }),
        }
    }

    pub fn role(&self) -> &str {
        self.message
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("")
    }
}

/// Compute the start index for a `history_turns`-bounded suffix of
/// `entries`. Counts **user turns** (not raw entries) so a turn's
/// assistant + tool-result group never gets sliced apart.
///
/// Returns `entries.len()` (yielding an empty suffix) when
/// `max_turns == 0` or no user role exists in `entries`.
pub fn window_start(entries: &[HistoryEntry], max_turns: usize) -> usize {
    if max_turns == 0 {
        return entries.len();
    }
    let mut user_seen = 0usize;
    let mut idx = entries.len();
    for (i, entry) in entries.iter().enumerate().rev() {
        if entry.role() == "user" {
            user_seen += 1;
            idx = i;
            if user_seen >= max_turns {
                break;
            }
        }
    }
    idx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_entry_shape() {
        let e = HistoryEntry::user("hi");
        assert_eq!(e.role(), "user");
        assert_eq!(e.message["content"], "hi");
    }

    #[test]
    fn assistant_text_shape() {
        let e = HistoryEntry::assistant_text("hello");
        assert_eq!(e.role(), "assistant");
        assert_eq!(e.message["content"], "hello");
        assert!(e.message.get("tool_calls").is_none());
    }

    #[test]
    fn assistant_with_tool_calls_uses_null_content_when_empty() {
        let calls = serde_json::json!([{"id":"c1","type":"function","function":{"name":"say","arguments":"{}"}}]);
        let e = HistoryEntry::assistant_with_tool_calls(None, calls.clone());
        assert!(e.message["content"].is_null());
        assert_eq!(e.message["tool_calls"], calls);
    }

    #[test]
    fn tool_result_shape() {
        let e = HistoryEntry::tool_result("c1", "say", "");
        assert_eq!(e.role(), "tool");
        assert_eq!(e.message["tool_call_id"], "c1");
        assert_eq!(e.message["name"], "say");
        assert_eq!(e.message["content"], "");
    }

    #[test]
    fn window_start_zero_turns_yields_empty_suffix() {
        let entries = vec![HistoryEntry::user("a"), HistoryEntry::assistant_text("b")];
        assert_eq!(window_start(&entries, 0), entries.len());
    }

    #[test]
    fn window_start_keeps_tool_results_with_their_assistant() {
        let entries = vec![
            HistoryEntry::user("hi"),
            HistoryEntry::assistant_text("hello"),
            HistoryEntry::user("again"),
            HistoryEntry::assistant_with_tool_calls(
                None,
                serde_json::json!([{"id":"c0","type":"function","function":{"name":"say","arguments":"{}"}}]),
            ),
            HistoryEntry::tool_result("c0", "say", ""),
        ];
        assert_eq!(window_start(&entries, 1), 2);
        assert_eq!(window_start(&entries, 2), 0);
        assert_eq!(window_start(&entries, 10), 0);
    }

    #[test]
    fn window_start_no_user_yields_empty() {
        let entries = vec![HistoryEntry::assistant_text("orphan")];
        assert_eq!(window_start(&entries, 5), entries.len());
    }
}
