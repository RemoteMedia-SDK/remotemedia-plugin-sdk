# Writing a room agent

A room agent is a node that answers people in a room: it reads what is said,
decides which lines are for it, and speaks its replies one sentence at a time.
`remotemedia-room-agent` (`crates/room-agent/`) holds everything such a node
does except producing the reply. A backend (a local model, a remote service)
implements one trait, `Responder`, and wraps it in `RoomAgent`.

The crate implements the member side of the agent link. The link itself (the
frames a room member receives and publishes) is owned by
`remotemedia-scaling`'s `agent-participants` capability; this crate follows
it and does not change it.

## What `RoomAgent` does

- **Reads the link.** Room-log lines (`text`/`voice` with `seq`, `history`,
  `author_kind`, `interrupted`), `history_end` and `reply_interrupted`. Plain
  Text input is a person's live line. Everything else, `floor` events
  included, is ignored.
- **Names authors.** `participant.display_name`, then `participant.name`,
  then the origin id without a page's `-xxxx` suffix. An agent-authored line
  whose author or origin id is the agent's `name` or `member_id` is the
  agent's own line.
- **Decides what to answer.** A person's live line that names the agent
  (`wake_words`, as whole words), or every person's line with
  `respond: "all"`. A line that is only the name, plus at most one word that
  is not filler ("Hey Pixel"), gets `listening` and waits `follow_up_secs`
  for the same speaker's question. History lines and agent lines are never
  answered.
- **Keeps the room.** The last 24 lines, each with an author, a role
  (`person` or `agent`), the text, an `interrupted` flag and a `seq`. It
  renders them as `"name: text"` prompt lines and as structured
  `{author, role, text, interrupted}` records.
- **Runs one reply at a time.** It assigns `"<uuid>-<n>"` reply ids and
  publishes `thinking`, then each sentence, then `idle`. A
  `reply_interrupted` for the reply stops publication.
- **Handles cancellation.** `RoomAgent` is a `TextGenerator` with
  `CancelStrategy::ObservesToken`. A reply cut off by a cancel stays in the
  history marked as interrupted, unless the cancel's `partial_output` is
  `Discard`. A cancel whose `pending_input` is `Discard` drops the lines
  heard since the last reply.

## Writing a responder

```rust
use remotemedia_room_agent::{Outcome, ReplyOut, Responder, RoomAgent, RoomAgentConfig, Sentences, Turn};
use remotemedia_plugin_sdk::types::Error;

struct EchoResponder;

#[async_trait::async_trait]
impl Responder for EchoResponder {
    fn sentences(&self) -> Sentences {
        Sentences::spoken() // the first piece may end at a comma
    }

    async fn respond(&self, turn: Turn<'_>, out: &mut ReplyOut<'_>) -> Result<Outcome, Error> {
        for word in turn.question.split_inclusive(' ') {
            if turn.cancel.is_cancelled() || !out.text(word)? {
                return Ok(Outcome::Cancelled);
            }
        }
        Ok(Outcome::Done)
    }
}
```

A turn carries:
- `speaker` and `question`: the bare name and the follow-up line are
  joined into one question;
- `line`: the `seq` of the answered line;
- `reply` and `instance`: the reply id and this node's instance id;
- `session_id`;
- `history`: a `Snapshot`:
  - `since_reply` is the lines since the previous reply started;
  - `recent` is every line kept;
  - `records()` gives the structured records before the answered line;
- `cancel`: this token fires when the call is cancelled or dropped. Stop
  any work on threads or in other services when it fires.

Send the reply in one of two ways:
- `out.text(chunk)` takes generated text. The agent splits it with the
  responder's `Sentences` and publishes each sentence as it completes.
- `out.sentence(sentence, extra)` takes a finished sentence, and `extra`
  holds fields to add to its line, such as `sources`.

Both return `Ok(false)` once the reply is stopped. Return
`Outcome::Fallback(text)` to say a fixed sentence instead of an answer.

## Wrapping it in a plugin

```rust
use remotemedia_plugin_sdk::{params::expand_env_params, adapter::StreamingNodeFfiAdapter, /* … */};

fn create(params: &serde_json::Value) -> Result<RoomAgent<EchoResponder>, String> {
    let params = expand_env_params(params);
    let config = RoomAgentConfig::from_params(&params)?;
    Ok(RoomAgent::new("EchoAgentNode", config, EchoResponder))
}
```

Export the node with `StreamingNodeFfiAdapter` from the factory, the same as
any other `AsyncStreamingNode`. Both crates must resolve to the same
`remotemedia-traits`. Use them from the same plugin-sdk checkout or
revision.

## Parameters

| Key | Default | |
| --- | --- | --- |
| `name` | `Pixel` | what the agent is called; its own lines are kept under it |
| `wake_words` | `[name]` | words that address it (case-insensitive) |
| `respond` | `mention` | `mention` or `all` |
| `follow_up_secs` | 8 | how long a bare name waits for its question (0–120) |
| `member_id` | `name` | the agent's member id in the room |
| `publish` | unset | `room`: the link's frames; unset: the pipeline messages below |
| `sentences` | `json` | pipeline mode only: `text` emits each sentence as Text |

`RoomAgentConfig::from_params` reads only these keys. A responder parses
its own keys from the same object.

## Pipeline mode

When `publish` is not `room`, the agent works as a model node in a voice
pipeline (for example `pipelineModel` in the SDK's AI library). In this
mode:
- It reads Text and `{"kind":"text"}` lines.
- It names speakers by `participant.name` or the origin id.
- It publishes these messages:
  - `{"kind":"assistant","state":"listening","to","for_ms"}`;
  - `{"kind":"assistant","state":"thinking","reply","to"}`;
  - `{"kind":"assistant","say","reply","to"}` for each sentence, or Text
    with `sentences: "text"`;
  - `{"kind":"assistant","text","reply","to","done":true,"first_ms","total_ms"}`
    at the end.

## Utilities

- `text::Sentences` is the sentence splitter. Its options are a
  first-clause length, an abbreviation guard, long-clause `:`/`;` marks and
  a byte bound.
- `trace::Stage` is content-free stage tracing. It is on when
  `REMOTEMEDIA_STAGE_TRACE=1` (or the older `REMOTEMEDIA_RAG_TRACE=1`) is
  set, and writes `RAG_STAGE {json}` lines to stderr.
- `remotemedia_plugin_sdk::params::expand_env` and `expand_env_params`
  expand `${VAR}` in parameter values.

## Examples

- `GemmaAssistantNode` in the LiteRT plugin: a local LiteRT-LM model, in
  both modes.
- `RagAssistantNode` in scaling's `rag_assistant_room_agent` example:
  document-grounded answers from a retrieval service, with citations.
