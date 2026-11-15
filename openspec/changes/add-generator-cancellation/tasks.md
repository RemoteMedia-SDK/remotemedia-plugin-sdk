Status as of 2026-11-15, branch `feat/generator-cancellation` in remotemedia-plugin-sdk, remotemedia-sdk-base and remotemedia-sdk. A box is checked only when the work and its stated test exist; partial items say what is left.

## 1. Shared contract (remotemedia-plugin-sdk)
- [x] 1.1 Add `crates/traits/src/cancel.rs`: `CancelToken`, `Cancellation`, `CancelReason`, `Disposition` (`INTERRUPT`, `ABORT`, serde form `{"partial_output":"retain|discard","pending_input":"retain|discard"}`), `CancelRegistration`, `child()`; unit tests for fire-once, first cancellation wins, `on_cancel` from another thread, registration drop, `cancelled()` in `select!`, and the per-reason default dispositions. Also `CancelRequest` (the cross-boundary form) and `DefaultDispositions`.
- [x] 1.2 Add `NodeRuntimeContextRead::cancel_token()` with a never-firing default.
- [ ] 1.3 Add `CancelStrategy`, `Cancellable` (no default `cancel_strategy`; defaulted `default_disposition` and `on_cancel`), and `TextGenerator`/`AudioGenerator`/`VideoGenerator`/`ImageGenerator`. Done in `crates/traits/src/generator.rs`, with `GeneratorRole` (typed constructors) and `compile_fail` doctests. Left: re-export from `remotemedia-modality`.
- [x] 1.4 Add `generates`/`cancel` to `NodeSchema` and `NodeDescription` (serde-optional, so older JSON parses).
- [ ] 1.5 `core-derive`: `#[node(generates = ..., cancel = ...)]` emits the typed `generator()` override, the schema and the plugin `describe()` declaration. Left: macro-level compile tests (the type check itself is covered by the `GeneratorRole` `compile_fail` doctests).
- [ ] 1.6 `plugin-sdk`: the SDK adapter reports the instance's role and per-reason defaults in `pacing()`. Left: a generator factory/export form with the `…Generator` bound for hand-written factories.
- [ ] 1.7 ABI 0.11: defaulted `FfiNode::cancel_call(context, cancellation_json)`; `loadable-node-abi` 0.11.0, `remotemedia-plugin-sdk` 0.13.0. Left: `docs/LOADABLE_NODE_ABI.md`, and verifying the 0.10 rejection (4.2).
- [x] 1.8 `plugin-sdk` ABI adapter: per-call token (in-flight slot, no call id needed), fired by `cancel_call` with the host's request re-resolved by the node's default, and by a drop guard or a failed `OutputSink::push` with `CallerCancelled`/`ABORT`; then `on_cancel` on the plugin runtime. 5 adapter tests.
- [ ] 1.9 Native worker: `ctx.cancel_token()` is backed by the request's `CancelSource`; `HostMessage::Cancel` carries an optional cancellation (omitted when absent); a dropped connection fires `SessionClosed`. Built; left: tests under `test-native-worker` (the crate's integration test support has pre-existing build errors unrelated to this change: `background_protocol` missing in struct literals).
- [ ] 1.10 Python workers: the iceoryx2 node loop (multiprocess, `python_plugin_export`, `python-source-plugin`), the mapped runner and the in-process template handle `__cancel_call__` and `__node_meta__` (remotemedia-sdk b742bf55, plugin-sdk 2e41a91). Left: the Rust side of `python_subprocess.rs`, `inprocess_python.rs` and `mapped_python.rs` calling into them on `cancel_call`.
- [ ] 1.11 `testing::assert_generator_cancels` (Rust, `crates/plugin-sdk/src/testing.rs`) with a passing generator and a runaway one that must fail; Python twin with three failing fixtures (keeps producing, drops pending input under `INTERRUPT`, keeps partial output under `ABORT`). Left: Rust fixtures for the two disposition failures (the Rust check measures the next call; node state is checked in each node's tests).
- [ ] 1.12 `InterruptableBackend` and `StatefulConversationBackend::finalize_turn` are deprecated. Left: the authoring guide.

## 1P. Python contract (remotemedia-sdk clients/python, plugin-sdk template)
- [x] 1P.1 JSON Schema exported from the Rust types (`crates/traits/schema/cancellation.schema.json`, checked by a Rust test); `remotemedia/core/cancel.py` held to it by `test_cancel_schema_parity.py`.
- [x] 1P.2 Thread-safe Python `CancelToken`.
- [x] 1P.3 `Cancellable` protocol, `GeneratorNode`, the four generator classes, `__init_subclass__` check.
- [x] 1P.4 `@register_node(generates=...)`; `__node_meta__` discovery frame.
- [ ] 1P.5 Worker side of the cancel frame (out of band, token, `pending_input`, `cancel()`, output gate). Left: replacing each node's `request_barge_in` latch (section 3.2).
- [x] 1P.6 Contract vendored into `templates/inprocess_python_plugin.py`, with a parity test.
- [ ] 1P.7 mypy fixture test (`test_cancel_mypy.py`). Left: mypy in CI.
- [ ] 1P.8 Python `assert_generator_cancels`, driving the worker loop function with fake ports. Left: real worker processes per mode.
- [x] 1P.9 Mixing `GeneratorNode` with the legacy `Node` base (other than through `MultiprocessNode`) raises `TypeError`.

## 2. Host (remotemedia-sdk-base)
- [ ] 2.1 The router creates a token per call and fires it inside the cancel branch, before the drop, for barge-in, supersession and session shutdown; spawns `on_cancel`; `ProtectGuard` suppresses the token. Router tests in `session_router/cancellation_tests.rs`. Left: RPC/endpoint cancel sites and the unused `cancel_force` path.
- [x] 2.2 Disposition resolution (canceller, manifest `on_cancel`, node default, reason) and `pending_input` on the router queue; undeclared nodes keep today's discard. Tests: queued input survives an interrupt; a node default, the canceller and the manifest each discard it; a protected call is not cancelled.
- [ ] 2.3 `barge_in` payload `disposition` and manifest `on_cancel` are parsed. Left: RPC/endpoint cancels.
- [ ] 2.4 Producers state only what they mean beyond an interrupt (see design): `HostedTurnNode` dropping a turn and `MemberFlushNode` send `pending_input: discard`; a retracted utterance sends `ABORT`; the coordinator sends none on purpose. Left: `RealtimeEventsNode` `response-cancel` and scaling `InterruptNode`.
- [x] 2.5 `cancel_token()` on the host `NodeRuntimeContext` (`CallCancel`, with the request behind the token).
- [ ] 2.6 The registry refuses a generator without a cancel strategy and a node whose declaration disagrees with its factory (`nodes/generator_admission_tests.rs`). Left: `src/validation/`, and the 0.10 acceptance check (4.2).
- [ ] 2.7 `PythonStreamingNode` sends `__cancel_call__` out of band when the token fires (frame-shape test). Left: `legacy_call.rs`, `loadable/source_python.rs`, `InProcessPythonNode`.
- [ ] 2.8 Audit over built-in Rust factories, a ratchet: undeclared generators are listed until migrated, reviewed transforms are listed separately. Left: `PythonNodeConfig` entries and the `@register_node` registry.
- [ ] 2.9 `PythonNodeConfig` `generates`/`cancel` and the discovery cross-check; `InProcessPythonNode` calling the template hooks.

## 3. Migrate generators
Each item: declare `generates` and a strategy, stop work that runs outside the call future (token or `on_cancel`), apply both dispositions to the node's own state (an LLM keeps or rolls back the cut-off reply; a TTS node discards its queued text by default; an S2S node retains buffered audio), and add `assert_generator_cancels`. Paths are relative to `D:\rm-mp`.

### 3.1 Built-in Rust (remotemedia-sdk-base/src)
- [x] 3.1.1 `OpenAIChatNode` and `MultimodalLLMNode`: text, `DropStopsWork`; the chat backend keeps a cut-off reply in history, marked interrupted, when partial output is retained, and rolls it back otherwise (two tests against the mock SSE provider).
- [ ] 3.1.2 `HostedTurnNode`: its barge publication carries dispositions (2.4). Left: declaring the role.
- [ ] 3.1.3 `TestToneTtsNode`: audio, `ObservesToken` in its synchronous loop.
- [ ] 3.1.4 `PythonStreamingNode` factories (Kokoro, VibeVoice, CosyVoice3, Voxtral, LFM2Audio): declare the role once each Python class is migrated (3.2), so no declared strategy is untrue.

### 3.2 Python nodes (remotemedia-sdk/clients/python/remotemedia/nodes)
- [ ] 3.2.1 Nodes with a barge latch: `KokoroTTSNode`, `KokoroOnnxTTSNode`, `QwenTTSMlxNode`, `LFM2AudioNode`, `LFM2AudioMlxNode`, `PersonaPlexAudioMlxNode`, `QwenTextMlxNode`, `MlxLmTextNode` — move to `GeneratorNode.cancel()`; retire `Interruptable`/`StatefulConversation`. `LFM2AudioNode`'s early-interrupt rollback becomes its `default_disposition`.
- [ ] 3.2.2 Nodes with no cancel today: `VibeVoiceTTSNode`, `CosyVoice3TTSNode`, `VoxtralTTSNode`, `NeuTTSNode`, the Moss nodes, `MossTTSRealtimeNode`, `LFM2TextNode`, `LFM2InstructAgentNode`, `LFM2DemoAgentNode`, `HermesLLMNode`.
- [ ] 3.2.3 Legacy `Node`-based generators `Qwen2_5OmniNode`, `UltravoxNode`, `TransformersPipelineNode`.
- [ ] 3.2.4 Motion generators `KimodoMotionNode`, `ArdyMotionNode`, `HomeLifeNode`.

### 3.3 Loadable plugins
- [x] 3.3.1 litert `GemmaAssistantNode`: text, `ObservesToken` (the model thread polls the token and calls `Conversation::cancel()` on its own thread, also before the first token); a cut-off reply is kept or rolled back by `partial_output`; `on_cancel` drops pending room lines on a discard. Real-model `assert_generator_cancels` (`tests/cancel_conformance.rs`, CPU backend): baseline first text 873 ms, after an `INTERRUPT` cancel 760 ms, after an `ABORT` cancel 912 ms.
- [ ] 3.3.2 litert `KokoroTtsNode`: audio, `ObservesToken` (stops before the next slice), discards queued text by default. Left: its real-model conformance run.
- [ ] 3.3.3 `kokoro-onnx-loadable`.
- [ ] 3.3.4 `FoundryLocalChatNode`.
- [ ] 3.3.5 `CandleChatNode`.
- [ ] 3.3.6 Python plugins (`moss-tts-realtime-loadable`, `lfm2-audio-python-source-plugin`, `lfm2-audio-mlx-python-source-plugin`, `indextts-25-python-source-plugin`).

### 3.4 Other repositories
- [ ] 3.4.1 `candle-nodes` `PhiNode`/`LlamaNode` (pinned to sdk-base rev 042a82f).
- [ ] 3.4.2 Out-of-tree generators (`qwen-s2s-avatar-plugins`, `LFM25AudioOnnxNode`): rebuild against ABI 0.11.
- [ ] 3.4.3 `remotemedia-scaling` mesh `EchoAgentNode`.

### 3.5 Avatar and video
- [ ] 3.5.1 `AvatarNode`, `IdleAnimationNode`, `Audio2FaceNode`, `MotionPlayerNode` (listed in the audit ratchet), `ActivationFaceNode`, `VadToFaceNode`, `SyntheticLipSyncNode`.

## 4. Validation
- [ ] 4.1 Run: `cargo test -p remotemedia-traits` (34 unit + doctests), `cargo test -p remotemedia-plugin-sdk --lib` (18). Left: the full workspace baseline and the feature matrix.
- [ ] 4.2 `./scripts/test-session-rpc.ps1`.
- [ ] 4.3 Run: base `--lib` tests for `transport::`, `nodes::`, `manifest` (685), plus chat backend, admission and cancellation tests; litert `--lib` (22). Left: the full CI baseline.
- [ ] 4.3P Run: the 76 Python cancellation tests (anaconda Python 3.12); the full suite shows the same 11 failures / 9 collection errors as before the change. Left: real worker modes.
- [ ] 4.4 Run: Gemma real-model conformance (3.3.1). Left: the in-process cancel and barge-in suite on rebuilt plugins, and a VAD interrupt on an S2S pipeline.
- [x] 4.5 `openspec validate add-generator-cancellation --strict --no-interactive`.
