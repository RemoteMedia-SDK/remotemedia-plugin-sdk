## Why
Two native room assistants exist today, and they share their room behavior by linking one plugin into the other:

- `GemmaAssistantNode` lives in `remotemedia-sdk-base/examples/litert-nodes/src/assistant.rs`.
- `RagAssistantNode` lives in `remotemedia-scaling/examples/scaling/rag_assistant_room_agent/native/src/rag/mod.rs`. It reaches the room helpers through a path dependency into sdk-base's example directory, with `default-features = false` so that LiteRT's plugin export and Kokoro's C++ phonemizer stay out of its build.

That layout causes four problems:

- **The room logic is not LiteRT logic, but only LiteRT can provide it.** `assistant_room.rs` holds the member side of the `agent-participants` link contract (scaling `crates/runner/src/port_core.rs:12-37`):
  - frame parsing (`RoomFrame`);
  - the presence and sentence publications;
  - speaker attribution;
  - wake-word and follow-up gating;
  - room history;
  - the cut-off-reply guard (`Partial`).

  None of it touches LiteRT. A third assistant, such as one on `llama-cpp` or `foundry-local`, would have to link the LiteRT crate to get it.
- **The shared part is incomplete, so the two assistants have already drifted apart.**
  - `process_streaming_with_ctx` and the reply lifecycle are copied between the two nodes (`assistant.rs:383-636`, `rag/mod.rs:200-337` and `629-693`). The lifecycle covers the turn lock, reply id, thinking/idle presence, the stop check, `Partial` and `on_cancel`.
  - The copies already disagree on:
    - **Reply ids:** a 16-bit nanosecond tag (`assistant.rs:357-363`) versus a UUID.
    - **Own-line detection:** origin id compared with `name` versus with `member_id`.
    - **History:** two parallel histories. `pending`/`recent` hold strings and `attributed` holds JSON, and Gemma's finished replies never reach `attributed`.
  - The config parser is LiteRT-shaped. RAG injects `model_path: "server-answer-no-local-model"` (`rag/mod.rs:86`) only to reuse it.
  - LiteRT-only modules carry general utilities: the sentence splitter (`lm.rs:474`, duplicated in RAG `protocol.rs:137`), `${VAR}` expansion (`node.rs:53`), and RAG test-harness tracing (`stage_trace.rs`, read by `KokoroTtsNode`).
- **The LiteRT plugin is an example only in name.** It has its own Cargo workspace, a `cdylib`, a `plugin.toml`, a vendored C++ phonemizer with Abseil, and per-platform builds for Android, Windows and Linux. Every other loadable plugin in the organisation has its own repository (`llama-cpp`, `foundry-local`, `whisper-cpp-plugin`, `silero-vad`, …). Two consumers already expect a standalone LiteRT crate that does not exist:
  - `remotemedia-sdk-base/examples/android-inprocess/Cargo.toml:19-20` points at `../../../remotemedia-litert-nodes` and `../../../litert-lm-loadable-plugin`;
  - `whisper/src/lib.rs:38-39` exports `remotemedia_litert_nodes::whisper::WhisperSttLiteRtNodeFactory`, which no longer exists.
- **A public plugin cannot depend on a private repository.** `remotemedia-scaling` is private and `remotemedia-sdk-base` is public, so shared room-agent code cannot live in scaling, even though scaling defines the link contract.

## What Changes
- **New crate `remotemedia-room-agent`** (`crates/room-agent/` in this repository). It is the member-side implementation of the `agent-participants` link contract that any plugin can use. It depends only on `remotemedia-traits`, `remotemedia-types`, `serde_json`, `futures-util`, `async-trait` and `uuid`, with no ABI, host or model dependencies. It provides:
  - **Link codec:** a typed codec for link frames. Inbound: lines (with `seq`, `history`, `author_kind`, `interrupted`), `history_end`, `reply_interrupted`, and ignored kinds. Outbound: presence listening, thinking and idle, and sentence lines with `text`/`say`/`reply`, plus optional extra fields such as `sources`.
  - **Attribution and addressing:** speaker attribution (`participant.display_name`, then `participant.name`, then the origin id), and one own-line check by name or `member_id`. Wake-word addressing with filler words, and the per-speaker follow-up wait.
  - **One structured history:** `Line { author, role, text, interrupted, seq }`, bounded. It renders to `"name: text"` for prompts and to the JSON history records RAG sends.
  - **The reply lifecycle:** collision-resistant reply ids; the cut-off-reply guard, which honours the cancel's `partial_output` disposition; the stop-by-`reply_interrupted` check; and clearing of pending lines when a cancel discards pending input.
  - **`RoomAgentConfig`:** parsed independently of any model (`name`, `wake_words`, `respond`, `follow_up_secs`, `member_id`).
  - **A generic `RoomAgent<R: Responder>` node.** It implements `AsyncStreamingNode`, `Cancellable` and `TextGenerator` and owns everything above. A backend implements only `Responder`: given a turn (speaker, question, line `seq`, history snapshot and cancel token), it streams reply text or finished sentences.
  - **Text helpers moved out of LiteRT:** the sentence splitter, including the first-clause option, and opt-in stage tracing (`REMOTEMEDIA_STAGE_TRACE`, which also accepts `REMOTEMEDIA_RAG_TRACE` during migration).
- **`${VAR}` parameter expansion** moves to `remotemedia-plugin-sdk` as a public helper.
- **`GemmaAssistantNode` becomes `RoomAgent<LiteRtLmResponder>`.** LiteRT keeps only the model thread, context headroom, and the fresh-prompt-after-cancel logic. The room-agent manifest parameters, defaults and wire shape stay the same.
- **`RagAssistantNode` becomes `RoomAgent<RagResponder>`** in the scaling example. It depends on `remotemedia-room-agent` for room behavior, and on the LiteRT runtime library only for phone-mode generation.
- **The LiteRT plugin moves to its own repository** (proposed `RemoteMedia-SDK/litert-nodes`), with history preserved. It becomes a Cargo workspace with two crates:
  - `litert-runtime`, an rlib: LiteRT and LiteRT-LM loading, CompiledModel, backends, the `.litertlm` container;
  - `litert-nodes`, the plugin `cdylib`: Whisper, Gemma assistant, Kokoro.

  It is released with the shared `build-plugin.yml` workflow. Consumers repoint to it: the scaling RAG example, `android-inprocess`, `whisper`, the scaling example READMEs and `run.py`, and the docs. `remotemedia-sdk-base/examples/litert-nodes/` is removed and replaced by a pointer.
- **No ABI change.** Node type names, `plugin.toml` node lists, manifest parameters and link frames are unchanged.
- **BEHAVIOR:**
  - Gemma reply ids change format: still `"<instance>-<n>"`, but the instance part becomes a UUID. Consumers treat ids as opaque, so this is not breaking.
  - Gemma's own replies now appear in structured history.
  - A reply the room stops (`reply_interrupted`) is recorded as interrupted. Both nodes recorded it as finished.
  - After the call's token fires, Gemma publishes nothing more of the reply, `idle` included, as RAG already does.
  - Stage tracing is enabled by a new variable name; the old name still works.
- **The non-room `GemmaAssistantNode` mode** (`publish` unset, `{"kind":"assistant"}` output, `sentences: text`) is kept, because the SDK's `pipelineModel`, voice example and e2e servers use it. It becomes `RoomAgent`'s pipeline publication mode, with its outputs unchanged (design D8).

## Impact
- **Plugin SDK** (`remotemedia-plugin-sdk`, owner of this change):
  - new `crates/room-agent/` (workspace member and default member);
  - `crates/plugin-sdk` gains the parameter-expansion helper;
  - docs: a short room-agent authoring guide, and a pointer from `PLUGIN_PUBLISHING.md`.
- **Base** (`remotemedia-sdk-base`):
  - `examples/litert-nodes/` is migrated, then removed;
  - `examples/android-inprocess/Cargo.toml` is repointed;
  - the open litert-nodes edits on `feat/rag-assistant-room-agent` must land first.
- **Scaling** (`remotemedia-scaling`):
  - the RAG example's native crate moves onto `remotemedia-room-agent`;
  - example READMEs, `voice_room_distributed/run.py` and the phone and RAG manifests' plugin paths are repointed;
  - optionally, `crates/runner/src/port_core.rs` tests check frame shapes against the shared codec;
  - the `add-rag-assistant-room-agent` design's "Native plugin ownership" decision (LiteRT owns the shared room helpers) is superseded and needs a note.
- **New repository:** `RemoteMedia-SDK/litert-nodes`, created by the maintainer, with a release workflow.
- **Whisper plugin** (`RemoteMedia-SDK/whisper`): fix the Android export to use the existing LiteRT factory names from the new crate.
- **SDK and docs** (`remotemedia-sdk/docs/AI_SDK_VOICE.md`, `examples/nextjs-tts-app/README.md`, `remotemedia-docs`): repoint paths, and confirm whether they use the non-room assistant mode.
- **Related changes:**
  - scaling `add-rag-assistant-room-agent`: overlapping helper extraction, superseded in location only;
  - scaling `replace-rooms-with-paired-sessions`: link frames unchanged, member terminology;
  - this repository's `add-generator-cancellation`: the `Cancellable` contract the driver implements.
