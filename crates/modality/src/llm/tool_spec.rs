//! LLM tool-call schema + side-effect dispatch hints.
//!
//! Direct Rust port of the Python `ToolSpec` dataclass in
//! `clients/python/remotemedia/nodes/ml/qwen_text_mlx.py`. Field shape,
//! `kind` semantics, and the default `say` / `show` tool descriptions
//! are preserved verbatim so a model trained or prompted against the
//! Python descriptions behaves identically when routed through a Rust
//! LLM node (e.g. `OpenAIChatNode`).
//!
//! Layering:
//!
//! - [`ToolSpec`] is the wire-shape an LLM node uses to advertise a
//!   tool to the model and decide what to do when the model invokes
//!   it.
//! - [`ToolKind`] picks the dispatch contract:
//!   - [`ToolKind::SideEffect`] — the LLM node consumes the call inline
//!     (e.g. `say` yields its `text` argument as TTS-channel output).
//!     No tool-result is fed back to the model.
//!   - [`ToolKind::ReturnValue`] — reserved for the classic two-pass
//!     "generate → execute → feed result back → regenerate" flow. Not
//!     yet implemented in Rust streaming dispatch; declaring such a
//!     tool currently logs and is dropped at dispatch time.
//! - [`default_say_tool`] / [`default_show_tool`] are the canonical
//!   built-ins. The LLM node typically toggles them via config flags
//!   rather than asking callers to construct them.
//! - [`to_openai_tools_array`] renders a slice of specs as the
//!   `tools` field of an OpenAI chat-completions request body.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Dispatch contract for a tool call.
///
/// Mirrors the Python `Literal["side_effect", "return_value"]` field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ToolKind {
    /// Tool is consumed inline by the LLM node — the call IS the
    /// output. No result is fed back to the model.
    SideEffect,
    /// Tool's return value should be fed back to the model on a second
    /// generation pass. Not implemented in the Rust SSE pipeline yet.
    ReturnValue,
}

impl Default for ToolKind {
    fn default() -> Self {
        Self::SideEffect
    }
}

/// Schema + dispatch hint for a tool the LLM may call.
///
/// `parameters` is a JSON-Schema object that gets passed verbatim to
/// the model inside the chat-completions `tools` array.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ToolSpec {
    pub name: String,
    pub description: String,
    pub parameters: Value,
    #[serde(default)]
    pub kind: ToolKind,

    /// Whether barge-in (user starting to speak again) is allowed to
    /// cancel the in-flight LLM call once the model has committed to
    /// invoking THIS tool.
    ///
    /// `true` (default) preserves the historical behaviour: any
    /// `barge_in` envelope on the LLM node's aux port drops the
    /// dispatch future and kills the HTTP stream mid-tool-call.
    /// Right for `say` / `show`-style tools whose effect IS the
    /// spoken response — barging while the assistant speaks is the
    /// desired UX.
    ///
    /// `false` instructs the chat backend to take a `ProtectGuard`
    /// off the per-call cancel gate as soon as a streamed
    /// `delta.tool_calls` chunk names this tool. Subsequent
    /// `barge_in` envelopes are suppressed for the rest of the call,
    /// so the tool dispatch always reaches its downstream handler.
    /// Use for tools that commit work the user cannot redo by
    /// repeating themselves — physical motion, side effects on the
    /// world, expensive generations — where mid-stream cancellation
    /// loses the result.
    ///
    /// Shutdown paths force-cancel anyway.
    #[serde(default = "default_true")]
    pub cancelable: bool,
}

fn default_true() -> bool {
    true
}

impl ToolSpec {
    /// Render as one entry in an OpenAI chat-completions `tools`
    /// array: `{ "type": "function", "function": { name, description,
    /// parameters } }`.
    pub fn to_openai_function(&self) -> Value {
        json!({
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": self.parameters,
            }
        })
    }
}

/// Render a slice of specs as a JSON array suitable for the
/// chat-completions `tools` request field.
pub fn to_openai_tools_array(specs: &[ToolSpec]) -> Value {
    Value::Array(specs.iter().map(ToolSpec::to_openai_function).collect())
}

/// Built-in `say` tool. Description is kept identical to the Python
/// `_default_say_tool()` so prompt-time behaviour is the same across
/// Python and Rust LLM nodes.
pub fn default_say_tool() -> ToolSpec {
    ToolSpec {
        name: "say".to_string(),
        description: "Speak a sentence aloud to the user. The REQUIRED `text` \
parameter is the exact words to speak — if you omit it or \
leave it empty, nothing is synthesised and the user hears \
silence. Put the actual words inside the tool call; never \
write them after it.\n\n\
Correct: say(text=\"Hi Mathieu, here's your script.\")\n\
Wrong:   say()  followed by text outside the call.\n\n\
Use `say` for anything the user should HEAR: greetings, \
conversational answers, short summaries, confirmations. \
Use plain prose only — no markdown, no code, no lists."
            .to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description":
                        "The words to speak aloud. MUST be a non-empty \
        string of plain prose. Example: \"Sure thing, here's the Python script.\"",
                    "minLength": 1
                }
            },
            "required": ["text"]
        }),
        kind: ToolKind::SideEffect,
        // `say` IS the spoken response — barging while it speaks is
        // the desired UX, so leave the historical default.
        cancelable: true,
    }
}

/// Built-in `show` tool. Description matches Python `_default_show_tool()`.
pub fn default_show_tool() -> ToolSpec {
    ToolSpec {
        name: "show".to_string(),
        description: "Display written content to the user as markdown. The REQUIRED \
`content` parameter is the markdown text itself — if you omit \
it or leave it empty, nothing is rendered. Put all written \
content inside the tool call; never write it after the call.\n\n\
Correct: show(content=\"```python\\ndef hi(): ...\\n```\")\n\
Wrong:   show()  followed by markdown outside the call.\n\n\
Use `show` for anything the user should READ rather than hear: \
code blocks (triple-backtick fences with a language tag), \
tables, lists, file paths, long explanations, command output."
            .to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "content": {
                    "type": "string",
                    "description":
                        "The markdown text to render. MUST be a non-empty \
        string. Example: \"```python\\nprint('hi')\\n```\"",
                    "minLength": 1
                }
            },
            "required": ["content"]
        }),
        kind: ToolKind::SideEffect,
        // UI display — fine to barge, no irreversible side effects.
        cancelable: true,
    }
}

/// Built-in `perform_motion` tool. The `prompt` argument is a
/// natural-language description of a full-body action that
/// `KimodoMotionNode` (or any other text-to-motion engine that
/// listens for `kind=motion_intent` envelopes) diffuses into a
/// skeletal pose stream. Side-effect: `tool_dispatch::dispatch_tool_call`
/// consumes the call and emits `RuntimeData::Json({kind:"motion_intent",
/// prompt:<arg>})` on the LLM node's fan-out.
///
/// IMPORTANT: there must be a wire edge from the LLM node to the
/// motion node (or to whatever bridge owns `avatar_intent.in`)
/// or the envelope is dropped on the floor with no error.
pub fn default_motion_tool() -> ToolSpec {
    ToolSpec {
        name: "perform_motion".to_string(),
        description: "Make the avatar perform a physical full-body motion or \
gesture. The REQUIRED `prompt` parameter is a natural-language \
description of what the body should do — name the limbs and \
the action explicitly. Do NOT use this for facial expressions \
(those come from emoji in your spoken text). Do NOT narrate \
the action in *asterisks* in your speech if you call this \
tool — the user sees the motion, not the narration.\n\n\
Examples of good prompts:\n  \
- \"a person waves with the right hand at shoulder height\"\n  \
- \"a person sits down slowly on a chair\"\n  \
- \"a person points to the left with the right arm\"\n  \
- \"a person nods their head once\"\n  \
- \"a person dances briefly in place\"\n\n\
Use this whenever the user asks the avatar to move, gesture, \
greet, sit, walk, dance, point, nod, shake their head, or any \
other physical action. You may call this in parallel with \
`say` to speak while moving."
            .to_string(),
        parameters: json!({
            "type": "object",
            "properties": {
                "prompt": {
                    "type": "string",
                    "description":
                        "Natural-language description of the full-body motion. \
        Should mention the body parts involved and the action verb. \
        MUST be a non-empty string. Example: \"a person waves the \
        right hand enthusiastically\".",
                    "minLength": 1
                }
            },
            "required": ["prompt"]
        }),
        kind: ToolKind::SideEffect,
        // Motion dispatch is irreversible — committing the user's
        // request to kimodo is expensive and there's no equivalent of
        // "stop talking" once the diffuser has accepted the prompt.
        // Suppress barge-cancellation by default; callers can flip
        // this back to `true` if they want barge to abort motion.
        cancelable: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn say_tool_has_required_text_param() {
        let spec = default_say_tool();
        assert_eq!(spec.name, "say");
        assert_eq!(spec.kind, ToolKind::SideEffect);
        assert_eq!(spec.parameters["required"][0], "text");
    }

    #[test]
    fn show_tool_has_required_content_param() {
        let spec = default_show_tool();
        assert_eq!(spec.name, "show");
        assert_eq!(spec.parameters["required"][0], "content");
    }

    #[test]
    fn to_openai_function_shape() {
        let v = default_say_tool().to_openai_function();
        assert_eq!(v["type"], "function");
        assert_eq!(v["function"]["name"], "say");
        assert!(v["function"]["parameters"].is_object());
    }

    #[test]
    fn array_render_preserves_order() {
        let specs = vec![default_say_tool(), default_show_tool()];
        let arr = to_openai_tools_array(&specs);
        let arr = arr.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["function"]["name"], "say");
        assert_eq!(arr[1]["function"]["name"], "show");
    }
}
