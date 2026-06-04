//! Streaming tool-call accumulation and side-effect dispatch.
//!
//! Modality-agnostic: the LLM backend collects per-index `tool_calls`
//! deltas as the SSE stream arrives, then dispatches each accumulated
//! call here. Side-effect tools (`say`, `show`, …) emit tagged text on
//! the data path; return-value tools are not yet implemented (would
//! require a second-pass generation loop).
//!
//! The OpenAI streaming protocol splits each tool call across many SSE
//! chunks: the first chunk carries `function.name` and (optionally) an
//! `id`, subsequent chunks append characters to `function.arguments`,
//! and a final chunk has `finish_reason: "tool_calls"`. We accumulate
//! per `index`, then dispatch on stream end.

use super::tool_spec::{ToolKind, ToolSpec};
use remotemedia_types::{tag_text_str, Error, RuntimeData, TEXT_CHANNEL_DEFAULT};
use serde_json::Value;

/// Per-index accumulator for `delta.tool_calls` deltas.
#[derive(Debug, Default)]
pub struct ToolCallAccum {
    /// Streaming `tool_call.id` assigned by the server. Required when
    /// rounding the assistant message back into the next request:
    /// every `tool_calls` entry must pair with a `tool` role message
    /// carrying the same `tool_call_id`. Falls back to a synthetic
    /// `call_<index>` id at dispatch time if the server omitted it.
    pub id: String,
    pub name: String,
    /// Stringified JSON fragment that, once concatenated, parses to
    /// the tool-call argument object. Kept as `String` (not `Value`)
    /// because mid-stream content is almost always non-parseable.
    pub arguments: String,
}

/// Look up a registered tool spec by name.
fn lookup_tool<'a>(registry: &'a [ToolSpec], name: &str) -> Option<&'a ToolSpec> {
    registry.iter().find(|t| t.name == name)
}

/// Dispatch one accumulated tool call.
///
/// Routing matches the Python `_handle_tool_call` in
/// `qwen_text_mlx.py:1663`:
///
/// - `say` → emit `text` argument on `output_channel` (default `tts`)
///   with a forced trailing `\n` so the coordinator's sentencer flushes
///   it as a complete utterance. Falls through aliases
///   (`text`/`content`/`message`/`body`/`spoken`).
/// - `show` → emit `content` argument on the `ui` channel.
/// - `perform_motion` → emit `{kind:"motion_intent", prompt:<arg>}`
///   `RuntimeData::Json` so a downstream `KimodoMotionNode` (or any
///   listener of the canonical motion-intent envelope) can drive
///   full-body skeletal animation. Aliased on
///   `prompt`/`description`/`action`.
/// - any other registered `side_effect` tool → log + drop. Generic
///   dispatch surface for user-provided handlers is future work.
/// - `return_value` tools → log + drop (multi-pass not implemented).
pub fn dispatch_tool_call<F>(
    registry: &[ToolSpec],
    call: &ToolCallAccum,
    output_channel: &str,
    callback: &mut F,
) -> Result<(), Error>
where
    F: FnMut(RuntimeData) -> Result<(), Error>,
{
    if call.name.is_empty() {
        tracing::warn!("[llm] tool call with no name received; dropping");
        return Ok(());
    }

    let spec = match lookup_tool(registry, &call.name) {
        Some(s) => s,
        None => {
            tracing::warn!(
                tool = %call.name,
                "[llm] model called unregistered tool; dropping"
            );
            return Ok(());
        }
    };

    if spec.kind == ToolKind::ReturnValue {
        tracing::warn!(
            tool = %call.name,
            "[llm] return_value tools require a second generation pass \
             (not yet implemented in the streaming path); skipping"
        );
        return Ok(());
    }

    // Parse arguments JSON. On failure, fall back to empty args so
    // alias-key lookup at least tries the raw string path.
    let args: Value = serde_json::from_str(&call.arguments).unwrap_or_else(|e| {
        tracing::warn!(
            tool = %call.name,
            error = %e,
            raw = %call.arguments,
            "[llm] tool call arguments did not parse as JSON; treating as empty"
        );
        Value::Object(serde_json::Map::new())
    });

    let extract_string = |keys: &[&str]| -> Option<String> {
        for k in keys {
            if let Some(s) = args.get(*k).and_then(Value::as_str) {
                if !s.is_empty() {
                    return Some(s.to_string());
                }
            }
        }
        // Tolerate models that hand back raw text instead of a JSON
        // object: if `arguments` is itself a quoted string, use it.
        if let Value::String(s) = &args {
            if !s.is_empty() {
                return Some(s.clone());
            }
        }
        None
    };

    match call.name.as_str() {
        "say" => {
            let spoken = extract_string(&["text", "content", "message", "body", "spoken"]);
            if let Some(text) = spoken {
                let flushable = if text.ends_with('\n') {
                    text
                } else {
                    format!("{}\n", text)
                };
                callback(RuntimeData::Text(tag_text_str(&flushable, output_channel)))?;
                // Mirror to the default text channel so the frontend
                // transcript displays the spoken reply. Skip the mirror
                // if `output_channel` is already the default to avoid
                // duplicate emission.
                if output_channel != TEXT_CHANNEL_DEFAULT {
                    callback(RuntimeData::Text(tag_text_str(
                        &flushable,
                        TEXT_CHANNEL_DEFAULT,
                    )))?;
                }
            } else {
                tracing::warn!(
                    args = %call.arguments,
                    "[llm] `say` tool call had no recognisable text arg; nothing to synthesise"
                );
            }
        }
        "show" => {
            let written = extract_string(&["content", "markdown", "text", "body"]);
            if let Some(text) = written {
                callback(RuntimeData::Text(tag_text_str(&text, "ui")))?;
            } else {
                tracing::warn!(
                    args = %call.arguments,
                    "[llm] `show` tool call had no recognisable content arg"
                );
            }
        }
        "perform_motion" => {
            // Free-form motion intent for downstream Kimodo-style
            // text-to-motion engines. We emit the canonical
            // `motion_intent` envelope shape that `KimodoMotionNode`
            // consumes (`{kind, prompt}`). The LLM node forwards this
            // through its fan-out callback; the manifest must wire
            // `llm → <motion_node>` for it to land — tool dispatch
            // does not have control-bus access from this layer.
            let prompt = extract_string(&["prompt", "description", "action"]);
            if let Some(p) = prompt {
                let env = serde_json::json!({
                    "kind": "motion_intent",
                    "prompt": p,
                });
                callback(RuntimeData::Json(env))?;
            } else {
                tracing::warn!(
                    args = %call.arguments,
                    "[llm] `perform_motion` tool call had no recognisable prompt arg"
                );
            }
        }
        other => {
            tracing::debug!(
                tool = %other,
                args = %call.arguments,
                "[llm] side_effect tool dispatched; no built-in handler — dropping"
            );
        }
    }
    Ok(())
}
