Each group ends with its check. Nothing is removed before its replacement passes. Repositories are named when the work is outside this one: (base) `remotemedia-sdk-base`, (scaling) `remotemedia-scaling`, (litert) the new LiteRT repository, (whisper) `RemoteMedia-SDK/whisper`, (sdk) `remotemedia-sdk`.

## 0. Before starting
- [x] 0.1 (base) Land or drop the 13 uncommitted edits under `examples/litert-nodes/` on `feat/rag-assistant-room-agent`; record the commit the split starts from. Landed as `48816d9` (rustfmt only); the split starts from it.
- [x] 0.2 Decide open questions 1 and 2 in `design.md` (non-room Gemma mode; repository name) and record them there. Questions 3 and 4 may wait until tasks 5.6 and 5.4.
- [x] 0.3 (sdk) Check whether `docs/AI_SDK_VOICE.md` and `examples/nextjs-tts-app` use `GemmaAssistantNode` without `publish: room`; record the result under D8. They do, as do `clients/ai` and the web e2e servers.

## 1. `remotemedia-room-agent` crate
- [x] 1.1 Add `crates/room-agent/` to the workspace members and default members, depending only on `remotemedia-traits`, `remotemedia-types`, `serde_json`, `futures-util`, `uuid` and `async-trait`.
- [x] 1.2 `link`: inbound decoding (`Line` with `interrupted`, `HistoryEnd`, `Stop`, `Ignored`; Text as a live person line), the presence and sentence-line builders, and the frame-kind constants.
- [x] 1.3 `attribution`: room speaker name (`participant.display_name`, then `participant.name`, then the origin id with any `-xxxx` hex suffix stripped), and the own-line check by name or `member_id`, case-insensitive.
- [x] 1.4 `addressing`: wake words with filler words, `respond: all`, and the per-speaker follow-up wait (live and from history).
- [x] 1.5 `history`: one bounded ring of `Line`, a since-last-reply cursor, `"name: text"` and JSON rendering with the author/text bounds.
- [x] 1.6 `reply`: `ReplyId` (UUID instance + counter), the cut-off-reply guard honouring `partial_output`, and the stop check.
- [x] 1.7 `RoomAgentConfig::from_params` with the current defaults and validation messages.
- [x] 1.8 `RoomAgent<R: Responder>`: `AsyncStreamingNode`, `Cancellable` (`ObservesToken`; `pending_input == Discard` clears the cursor, then calls `R::on_cancel`) and `TextGenerator`.
- [x] 1.9 `text::Sentences` with the options in D7; `trace` with both environment variables and the `RAG_STAGE ` prefix.
- [x] 1.10 `remotemedia_plugin_sdk::params::expand_env`.
- [x] 1.11 Port the tests from `assistant.rs`, `assistant_room.rs` and `lm.rs` (splitter), and RAG's room tests, unchanged in their expectations except reply-id format. Add tests for the two splitter configurations, own-line by `member_id`, `interrupted` agent lines, and Gemma replies appearing in structured history.
- [x] 1.12 Run `assert_generator_cancels` against `RoomAgent` with a scripted `Responder` (each disposition; no output after cancel; next turn not delayed).
- [x] 1.13 Authoring guide `docs/ROOM_AGENT.md`, and a pointer from `PLUGIN_PUBLISHING.md`.
- [x] 1.14 Check: `cargo fmt --check`, `cargo check --workspace --all-targets`, `cargo test -p remotemedia-room-agent`, `cargo test -p remotemedia-plugin-sdk`.

## 2. Gemma onto the shared crate (still in base `examples/litert-nodes/`)
- [x] 2.1 (base) Implement `LiteRtLmResponder`, keeping the model thread, `CONTEXT_HEADROOM`, the fresh prompt after a cancelled conversation, and the 20 ms cancel poll. Done (sdk-base `635b6ad`).
- [x] 2.2 (base) `GemmaAssistantNode` becomes `RoomAgent<LiteRtLmResponder>` in both publication modes (D8). Done.
- [x] 2.3 (base) `KokoroTtsNode` uses `room_agent::trace`; Whisper, Kokoro and Gemma use `expand_env`. Delete `assistant_room.rs` and `stage_trace.rs`, and `Sentences` from `lm.rs`. Done.
- [x] 2.4 (base) Confirm that no consumer parses reply ids (port, room log, room page, scaling tests) before the instance format changes. Done: nothing in scaling, the AI client or the room tools parses a reply id.
- [ ] 2.5 Check (base, litert crate): `cargo test` and `tests/cancel_conformance.rs` with a model; the plugin builds for Windows and `aarch64-linux-android`; the host loads it and lists the three node types. Partly done: `cargo test`, and the real-model `cancel_conformance` on CPU (Gemma 4 E2B: first text 1.11 s baseline, 0.80 s after an interrupt, 1.02 s after an abort); the Windows plugin loads beside the RAG plugin and lists the three node types. Not run: the `aarch64-linux-android` build.
- [ ] 2.6 Check (scaling): `phone_assistant_room_agent/ask.py` on the phone and on the PC standby within the `Assistant Room Migration` limits, including kill and drain. Record the hardware, the runs and the medians.

## 3. RAG onto the shared crate
- [x] 3.1 (scaling) `RagResponder` keeps the HTTP client, evidence/citation validation, phone/server split, source probe, conversation replies and failure messages. `RagAssistantNode` becomes `RoomAgent<RagResponder>`. Done (scaling `d31b972`).
- [x] 3.2 (scaling) Parse RAG keys apart from `RoomAgentConfig`; remove the injected `model_path`/`publish`; keep the same accepted and rejected parameters and messages. Done. `publish` defaults to `room` and any other value is rejected; `follow_up_secs` errors now come from `RoomAgentConfig`.
- [x] 3.3 (scaling) RAG's sentence splitting uses `Sentences` configured as in D7 (no first clause, no guard, 32,000-byte bound). Done.
- [x] 3.4 (scaling) Depend on `remotemedia-room-agent` for room behavior, and on the LiteRT runtime only for phone-mode generation. Done (scaling `2b7074b`).
- [ ] 3.5 Check (scaling): the native `cargo test`, `rag-assistant-check`, the plugin-load test, and `evaluation/stage_records.py` still parsing traces; then the agentic room quality suites and the takeover/kill/drain runs recorded by `add-rag-assistant-room-agent`, compared with its recorded results. Partly done: native `cargo test` (10 passed) and the plugin-load test (both plugins in one process). The trace format is unchanged (same `RAG_STAGE ` records), but `stage_records.py`, `rag-assistant-check` with a service, the quality suites and the takeover/kill/drain runs were not run.

## 4. LiteRT repository
- [ ] 4.1 Maintainer creates the repository (open question 2). Import `examples/litert-nodes` with `git subtree split` from the commit recorded in 0.1. Partly done: split locally to `D:\rm-mp\litert-nodes` from sdk-base `635b6ad` (18 commits kept). The GitHub repository is not created or pushed.
- [x] 4.2 (litert) Restructure into `crates/litert-runtime` (rlib) and the root plugin crate as in D9; keep the `kokoro` and `plugin` features, the binaries, assets, `plugin.toml` and the README. Done (litert-nodes `5e6af58`).
- [ ] 4.3 (litert) Depend on plugin-sdk and `remotemedia-room-agent` by sibling path for development, with git-revision dependencies for releases. Partly done: sibling paths. Git-revision dependencies wait until plugin-sdk's room-agent is pushed.
- [ ] 4.4 (litert) CI: build and test on Windows and Linux; build `aarch64-linux-android` with the NDK, CMake and Ninja; a release workflow producing `liblitert-nodes-{arch}-{os}.{ext}` and `release-manifest.json` for all three. Partly done: `.github/workflows/ci.yml` (Windows/Linux test, Android build), not run. No release workflow yet: the shared `build-plugin.yml` checks out only the plugin repository and has no Android target.
- [ ] 4.5 Check (litert): CI green on all three targets; the host resolves `"plugins": ["litert-nodes@<tag>"]` from the release and loads it; `cancel_conformance` with a model.

## 5. Repoint consumers
- [x] 5.1 (scaling) RAG native `Cargo.toml` → `litert-runtime` (phone mode) and `remotemedia-room-agent`; drop the path into base's example. Done.
- [x] 5.2 (scaling) Phone and RAG agent manifests and `run.py` plugin paths, `voice_room_distributed/run.py`, and the example READMEs (`phone_assistant_room`, `phone_assistant_room_agent`, `benchmark_lfm3_android`, `rag_assistant_room_agent`, `voice_room_distributed`). Done. Plugin directories were already command-line arguments; `benchmark_lfm3_android` had no reference.
- [x] 5.3 (scaling) Add a note to `add-rag-assistant-room-agent/design.md` "Native plugin ownership" that the shared helpers moved to `remotemedia-room-agent`. Done.
- [ ] 5.4 (scaling) A test that encodes frames with `port_core` and decodes them with `room_agent::link` (D5).
- [ ] 5.5 (base) `examples/android-inprocess/Cargo.toml`: replace the two dangling paths with the LiteRT repository's crates and features. Build it for Android. Partly done: the two unused dangling dependencies are removed and the manifest resolves (sdk-base `fb76ebd`). Not run: the Android build. `build-android.sh` still builds the separate `litert-lm-loadable-plugin`.
- [ ] 5.6 (whisper) Fix the Android export to the existing factory names (open question 3). Build for Android. Not done: the whisper checkout has someone else's uncommitted `Cargo.toml` change that moves Android to Candle and drops the LiteRT dependency (open question 3); `src/lib.rs`'s Android export still names the old factories.
- [ ] 5.7 (sdk, docs) Repoint `AI_SDK_VOICE.md`, `nextjs-tts-app/README.md`, the `add-ai-sdk-voice-library` tasks, and `remotemedia-docs` references. Done for `AI_SDK_VOICE.md`, `nextjs-tts-app/README.md` and `llm-litert.integration.test.ts` (sdk `2e3a1d47`). Left as history: the `add-ai-sdk-voice-library` tasks and the `remotemedia-docs` validation hash files.
- [x] 5.8 Check: grep every repository for `examples/litert-nodes`, `remotemedia_litert_nodes::assistant_room` and `stage_trace`; only history, archive and evidence files may remain. Done: only openspec history/evidence and `remotemedia-docs` validation hash files remain.

## 6. Remove base's copy
- [x] 6.1 (base) Replace `examples/litert-nodes/` with a README naming the new repository and the commit it was split from. Done (sdk-base `fb76ebd`).
- [ ] 6.2 Check (base): the CI baseline from `AGENTS.md`; nothing in the base workspace referenced the example. Partly done: the base workspace resolves (`cargo metadata`) and nothing in it referenced the example; the full CI baseline was not run.
- [ ] 6.3 `openspec validate extract-room-agent-and-litert-plugin --strict --no-interactive`.
