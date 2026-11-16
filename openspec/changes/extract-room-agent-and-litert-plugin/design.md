## Context
| Location | What it shows |
| --- | --- |
| `remotemedia-sdk-base/examples/litert-nodes/src/assistant_room.rs` | Shared helpers today: `Room` (`pending`, `recent`, `attributed`, `waiting`, `stopped`, `replies`), `Partial`, `RoomFrame`, the presence and sentence builders, `addressed`, `speaker_name`, `room_speaker_name`, `line_text`. None of them uses LiteRT. |
| `.../litert-nodes/src/assistant.rs:383-517` | Gemma reply lifecycle. It records its finished reply with `note()`, so the reply never reaches `attributed`. |
| `.../litert-nodes/src/assistant.rs:548-636` | Gemma frame dispatch. |
| `.../litert-nodes/src/assistant.rs:357-363` | Reply-id instance tag: `subsec_nanos & 0xffff`, 65,536 values. |
| `remotemedia-scaling/.../rag_assistant_room_agent/native/src/rag/mod.rs:200-337, 629-706` | RAG reply lifecycle, frame dispatch and `Cancellable`, copied from Gemma with small differences: UUID instance, own-line by `member_id`, `interrupted` read from raw JSON, `note_attributed`. |
| `.../rag/mod.rs:84-119` | RAG reuses `assistant::config_from_params`, injecting a fake `model_path` in server mode and forcing `publish: room`. |
| `.../litert-nodes/src/lm.rs:474-540` | Gemma sentence splitter: first-clause option, abbreviation guard, long `:`/`;` rule. |
| `.../rag/protocol.rs:136-170` | RAG sentence splitter: plain `.`/`!`/`?`/newline, never a first clause, 32,000-byte bound. |
| `.../litert-nodes/src/stage_trace.rs` | Reads `REMOTEMEDIA_RAG_TRACE` and writes `RAG_STAGE` lines; `KokoroTtsNode` (`tts_node.rs:233`) calls it. `rag_assistant_room_agent/evaluation/stage_records.py` parses the `RAG_STAGE ` prefix. |
| `remotemedia-scaling/crates/runner/src/port_core.rs:12-37` and `openspec/specs/agent-participants` | Owner of the link-frame contract the helpers implement. |
| `remotemedia-sdk-base/examples/android-inprocess/Cargo.toml:19-20` | Points at `../../../remotemedia-litert-nodes` (feature `whisper`) and `../../../litert-lm-loadable-plugin`; neither exists. |
| `whisper/src/lib.rs:36-41` | Android export names `remotemedia_litert_nodes::whisper::{WhisperLiteRtNodeFactory, WhisperSttLiteRtNodeFactory}`. The crate has `node::WhisperLiteRtFactory`. |
| GitHub org `RemoteMedia-SDK` | One repository per loadable plugin; `remotemedia-scaling` is private, `remotemedia-sdk-base` and `remotemedia-plugin-sdk` are public. |

## Goals / Non-Goals
**Goals**
- Implement the member side of the agent link once, in a crate any plugin can depend on without LiteRT, a host or a private repository.
- Make a new room assistant a `Responder` implementation plus a factory.
- Keep the observable behavior of `GemmaAssistantNode` and `RagAssistantNode`: node types, parameters, defaults, frames and latency limits.
- Give LiteRT the same packaging as every other plugin: its own repository and release, and a runtime library other crates can use without the plugin export.
- Fix the dangling LiteRT references in `android-inprocess` and `whisper`.

**Non-Goals**
- Changing the link-frame contract or the participant port (scaling owns them).
- A common LLM-chat contract across `llama-cpp`, `foundry-local` and LiteRT. That becomes possible after this change but is not part of it.
- Moving RAG out of the scaling example, or changing its retrieval protocol.
- Publishing crates to crates.io.

## Decisions

### D1. The shared crate lives in plugin-sdk, as its own crate
`crates/room-agent/` (package `remotemedia-room-agent`) depends on `remotemedia-traits`, `remotemedia-types`, `serde_json`, `futures-util`, `async-trait` and `uuid`. It does not depend on `loadable-node-abi` or `remotemedia-plugin-sdk`, so a node built from it can be wrapped by any export path, and the crate adds nothing to the ABI.

Alternatives rejected:
- **Scaling:** private, and a heavy dependency for a plugin.
- **sdk-base:** host code, which plugins must not depend on.
- **The LiteRT crate:** this is the current state, and the source of the coupling.
- **A feature of `remotemedia-plugin-sdk`:** it would tie the release cadence to the ABI crate and grow its feature matrix.

Plugin-sdk's AGENTS.md assigns it "shared payload types, node traits", and every plugin already depends on it.

### D2. One structured history
`Room` keeps one bounded ring of `Line { author, role: Person | Agent, text, interrupted, seq }` (`MAX_LINES = 24`, as today). `pending` becomes a cursor into that ring, the index after the last reply, instead of a second copy. Prompt text (`"name: text"`) and RAG's JSON history (`{author, role, text, interrupted}`, with author ≤256 and text ≤4,000 characters) are both rendered from it.

Every write goes through one `note(Line)`, including the cut-off-reply guard. This fixes two current gaps: Gemma's finished replies were missing from structured history, and the guard skipped author truncation.

The waiting-for-question map (`speaker → (Instant, name text)`) and `stopped: Option<ReplyId>` stay as fields of `Room`.

### D3. `RoomAgent<R: Responder>` owns the node; backends implement `Responder`
```rust
pub struct Turn<'a> {
    pub speaker: &'a str,
    pub question: &'a str,      // with the bare-name prefix when it followed one
    pub line: Option<u64>,      // seq of the answered line
    pub reply: &'a ReplyId,
    pub history: History<'a>,   // snapshot: since-last-reply and recent, renderable
    pub cancel: CancelToken,    // child of the call's token
}

#[async_trait]
pub trait Responder: Send + Sync + 'static {
    /// Streams the reply. Each `Chunk::Text` goes through the splitter;
    /// `Chunk::Sentence(Value)` is published as is (e.g. RAG lines with `sources`).
    async fn respond(&self, turn: Turn<'_>, out: &mut ReplyOut<'_>) -> Outcome;
    /// Called after a cancel that discards pending input (default: nothing).
    async fn on_cancel(&self, _: &Cancellation) {}
}
```
`RoomAgent` implements the whole node. Per frame, it:
1. decodes the frame;
2. records `reply_interrupted` stops;
3. notes agent lines, marking its own lines by name or `member_id`;
4. recalls history lines;
5. hears live lines;
6. publishes `listening` for a bare name;
7. takes the turn lock;
8. assigns the reply id;
9. publishes `thinking` with `line`;
10. drives the responder through `ReplyOut`, which checks `stopped` and the token before each publish;
11. finishes the remaining sentence;
12. notes the reply, or the cut-off-reply guard notes it as interrupted;
13. publishes `idle`.

`Outcome` lets a responder add a fallback sentence (RAG's failure messages) or decide that nothing more is published (`Cancelled`). `RoomAgent` also implements `Cancellable` (`ObservesToken`, clearing pending lines on `Retention::Discard`, then `R::on_cancel`) and `TextGenerator`, so `add-generator-cancellation`'s conformance check applies to every room agent.

Gemma's model thread, its `CONTEXT_HEADROOM` reset and its fresh prompt after a cancelled conversation stay in `LiteRtLmResponder`. RAG's HTTP client, evidence validation, phone/server split, source probe and narrow conversation replies stay in `RagResponder`.

### D4. Room settings are parsed apart from model settings
`RoomAgentConfig::from_params(&Value)` reads `name` (default `Pixel`), `wake_words`, `respond` (`mention`|`all`), `follow_up_secs` (0–120) and `member_id` (default `name`). Each responder parses its own keys from the same object. RAG stops injecting `model_path` and `publish`.

`publish: "room"` stays accepted on `GemmaAssistantNode` because its manifests set it. While the non-room mode exists (D8), the LiteRT node decides between modes and only the room mode uses `RoomAgent`.

### D5. Link codec
`room_agent::link` holds:
- `Inbound::{Line{text, seq, history, agent, interrupted}, HistoryEnd, Stop(ReplyId), Ignored}`;
- builders for `presence` (listening/thinking/idle) and sentence lines;
- the frame-kind constants.

Plain Text input decodes as a person's live line, as today. `floor` frames, including `refused` with `floor_busy`, are ignored, as today.

The crate's tests use the frames listed in `port_core.rs`'s module docs. A scaling task adds a test that encodes with `port_core` and decodes with the codec, so drift breaks a build. Scaling may later use the constants in `port_core`, but this change does not require it.

### D6. Reply ids
A `ReplyId` is `"<instance>-<n>"`: `instance` is a UUID v4 made per node instance, and `n` counts replies. The format is the same; the instance part changes from four hex digits to a UUID. Every consumer (port, room log, page, tests) treats it as an opaque string, and that is checked before Gemma switches.

### D7. Utility relocation
- **`Sentences` moves to `room_agent::text`.** It is configurable, so both current splitters can be reproduced exactly: `first_clause(chars)`, `abbreviation_guard(bool)`, `long_clause_marks(bool)`, `max_bytes(n)`. When `push` exceeds the byte bound it returns `Err`.

  | Splitter | first clause | abbreviation guard | long clause marks | byte bound |
  | --- | --- | --- | --- | --- |
  | Gemma | 24 | on | on | none |
  | RAG | off | off | off | 32,000 |

  Unifying the two is not in scope; it would change how RAG cites sources.
- **`stage_trace` moves to `room_agent::trace`.** It is enabled by `REMOTEMEDIA_STAGE_TRACE=1` or the old `REMOTEMEDIA_RAG_TRACE=1`. The output prefix stays `RAG_STAGE ` until `stage_records.py` accepts a new prefix, which is a separate decision. `KokoroTtsNode` uses the crate's trace, so LiteRT's TTS no longer depends on RAG-named code.
- **`expand` (`${VAR}` in parameters) moves to `remotemedia_plugin_sdk::params::expand_env`.** It is a general authoring helper that the Whisper, Kokoro and Gemma nodes and RAG all use.

### D8. The non-room Gemma mode stays in LiteRT for now
The non-room mode (`publish` unset) emits `{"kind":"assistant", ...}`, reads `line_text`, uses `speaker_name`'s `-xxxx` suffix stripping, and supports `sentences: text`. The scaling examples do not use it. Whether the SDK's voice docs (`AI_SDK_VOICE.md`, `nextjs-tts-app`) do has not been checked.

It stays in `GemmaAssistantNode` unchanged, built on the crate's history, addressing and splitter but not on `RoomAgent`. Removing it is a separate change once its users are known.

### D9. The LiteRT plugin repository
- **Repository:** proposed `RemoteMedia-SDK/litert-nodes`, public like sdk-base. The maintainer creates it.
- **History:** imported with `git subtree split --prefix=examples/litert-nodes` from `remotemedia-sdk-base`, after the open edits on `feat/rag-assistant-room-agent` land.
- **Layout:** a Cargo workspace.
  - `crates/litert-runtime`, an rlib: `litert.rs`, `compiled.rs`, `backend.rs`, `lm.rs`, `container.rs`. It has no plugin export and no C++ build. RAG phone mode and `android-inprocess` depend on this crate.
  - The root plugin crate `litert-nodes`, a cdylib + rlib: Whisper (`node.rs`, `whisper.rs`, `mel.rs`, `tokenizer.rs`), Kokoro (`kokoro/`, `bilstm.rs`, `tts_node.rs`, `cpp/`, `build.rs`), and Gemma (`LiteRtLmResponder` and its factory), plus the `whisper-litert`, `gemma-litert` and `kokoro-litert` binaries, `plugin.toml`, `assets/`, and the cancel-conformance test.
  - The `kokoro` and `plugin` features keep their meaning.
- **Release:** the shared `build-plugin.yml` workflow publishes `liblitert-nodes-{arch}-{os}.{ext}` and `release-manifest.json`, so manifests can use `"plugins": ["litert-nodes@vX"]`. Android (`aarch64-linux-android`) needs the NDK, CMake and Ninja for Kokoro. If the shared workflow cannot provide them, the repository adds an Android job and the release manifest still lists the Android asset.
- **During development:** consumers use path dependencies on a sibling checkout (`../litert-nodes`), like the other plugins. Pinned git revisions are used for released consumers.

### D10. Order and rollback
Each step builds and passes its checks on its own, and can be reverted alone:
1. Land the pending litert-nodes edits.
2. Add `remotemedia-room-agent`, with tests ported from `assistant.rs`/`assistant_room.rs` and RAG's room tests.
3. Move Gemma onto it, inside sdk-base's example directory, then check Gemma.
4. Move RAG onto it, then check RAG.
5. Split the LiteRT repository.
6. Repoint consumers.
7. Replace sdk-base's example with a README pointer.

Until step 7, the old location still builds, so reverting any later step only means reverting consumer paths.

## Risks / Trade-offs
- **A behavior drift hides in the refactor.** Mitigation: port every existing assistant and RAG room test to the shared crate before switching either node; run Gemma's and RAG's cancel-conformance and RAG's native test suite at each step; and re-measure the phone agent against the `Assistant Room Migration` limits on the phone, because unit tests do not cover latency.
- **Two repositories must change together** (plugin-sdk, then LiteRT and RAG). Mitigation: path dependencies on sibling checkouts during development, and plugin-sdk merged first.
- **A cross-compile regression in the LiteRT CI move.** Mitigation: build `aarch64-linux-android`, `x86_64-pc-windows-msvc` and `x86_64-unknown-linux-gnu` in the new repository's CI before removing sdk-base's copy, and load each built plugin with the host.
- **A dependency pin mismatch.** `remotemedia-room-agent` must resolve to the same `remotemedia-traits` as the plugin that uses it, or trait objects will not match. It uses workspace path patches like the rest of plugin-sdk, and consumers patch it with the other plugin-sdk crates.

## Migration Plan
See D10 and `tasks.md`. Manifests, node types and runner configuration are unchanged. Operators see only that the plugin directory comes from the new repository's release instead of an sdk-base build.

## Open Questions
1. Keep, deprecate or remove the non-room Gemma mode? (D8: kept unchanged in this change.)
2. Repository name: `litert-nodes` (matches the plugin name) or `litert` (matches `llama-cpp`, `foundry-local`)?
3. Should `whisper`'s Android LiteRT export depend on `litert-nodes` with `default-features = false`, or be dropped in favour of loading the `litert-nodes` plugin directly?
4. Should scaling's `port_core` use the shared codec's constants (single source), or only test against it (D5)?
