//! LLM primitives: per-session conversation history, OpenAI-shaped
//! tool specs, and streaming tool-call dispatch.
//!
//! Used by `OpenAIChatNode` (in-tree), `FoundryLocalChatNode`
//! (loadable plugin), and the upcoming `CandleChatNode`. One source
//! of truth — every chat-completion-shaped provider in the SDK
//! emits the same `HistoryEntry` and parses the same `ToolSpec`.

pub mod history;
pub mod tool_dispatch;
pub mod tool_spec;

pub use history::{window_start, HistoryEntry};
pub use tool_dispatch::{dispatch_tool_call, ToolCallAccum};
pub use tool_spec::{
    default_motion_tool, default_say_tool, default_show_tool, to_openai_tools_array, ToolKind,
    ToolSpec,
};
