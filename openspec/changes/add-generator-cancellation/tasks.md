## 1. Shared contract (remotemedia-plugin-sdk)
- [ ] 1.1 Add `crates/traits/src/cancel.rs`: `CancelToken`, `Cancellation`, `CancelReason`, `Disposition` (`INTERRUPT`, `ABORT`, serde form `{"partial_output":"retain|discard","pending_input":"retain|discard"}`), `CancelRegistration`, `child()`; unit tests for fire-once, first cancellation wins, `on_cancel` from another thread, registration drop, `cancelled()` in `select!`, and the per-reason default dispositions.
- [ ] 1.2 Add `NodeRuntimeContextRead::cancel_token()` with a never-firing default.
- [ ] 1.3 Add `CancelStrategy`, `Cancellable` (no default `cancel_strategy`; defaulted `default_disposition` and `on_cancel`), and `TextGenerator`/`AudioGenerator`/`VideoGenerator`/`ImageGenerator`; re-export from `remotemedia-modality`.
- [ ] 1.4 Add `generates`/`cancel` to `NodeSchema` and `NodeDescription` (serde-optional, so older JSON parses).
- [ ] 1.5 `core-derive`: `#[node(generates = ...)]` emits the bound check and fills the schema; add trybuild tests for the pass case and the missing-`Cancellable` compile error.
- [ ] 1.6 `plugin-sdk`: generator factory/export forms with the `…Generator` bound; `describe()` emits `generates`/`cancel`.
- [ ] 1.7 ABI 0.11: add defaulted `FfiNode::cancel_call(call_id, cancellation_json)`, bump `loadable-node-abi`/`plugin-sdk` versions, decide and document the 0.10 acceptance rule (open question), update `docs/LOADABLE_NODE_ABI.md`. Keep `integration/abi-v05-fixture` and the old-ABI fixture pinned.
- [ ] 1.8 `plugin-sdk` ABI adapter: per-call token keyed by call id, fired by `cancel_call` with the host's cancellation, and by a drop guard on the FFI future or a failed `OutputSink::push` with `CallerCancelled`/`ABORT`; then `on_cancel` on the plugin runtime. Tests: `cancel_call` then drop runs `on_cancel` once with the host's disposition; drop alone fires the fallback.
- [ ] 1.9 Native worker: back `ctx.cancel_token()` with the request's cancel flag and carry the cancellation on `HostMessage::Cancel`; test with `test-native-worker`.
- [ ] 1.10 Python workers (mapped, subprocess, in-process, `python_plugin_export`, `python-source-plugin`): one per-call cancel frame with reason and disposition, `GeneratorNode` base class with abstract `cancel()` and overridable `default_disposition()`, per-call `cancel_token`; tests per mode, including mapped-worker tests under `test-mapped-probe`.
- [ ] 1.11 `testing::assert_generator_cancels` and its Python twin, covering both dispositions, with fixture generators that the check must fail: one that keeps producing after a cancel, one that drops pending input under `INTERRUPT`, one that keeps partial output under `ABORT`.
- [ ] 1.12 Deprecate `InterruptableBackend` and `StatefulConversationBackend::finalize_turn`; document the contract, dispositions and precedence in the authoring guide.

## 1P. Python contract (remotemedia-sdk clients/python, plugin-sdk template)
- [ ] 1P.1 Export a JSON Schema for `Cancellation`, `CancelReason`, `Disposition`, `CancelStrategy`, `GeneratedModality` from the Rust types; generate `clients/python/remotemedia/core/cancel.py` from it; parity test in both repos fails on drift.
- [ ] 1P.2 Python `CancelToken`: thread-safe (`is_cancelled`, `wait` for blocking threads, `await cancelled()`, `on_cancel` with at-most-once callbacks via `call_soon_threadsafe`); tests from a model thread and from asyncio.
- [ ] 1P.3 `Cancellable` protocol, `GeneratorNode` (abstract `cancel`, `cancel_strategy` ClassVar, overridable `default_disposition`, per-call `cancel_token`), `TextGenerator`/`AudioGenerator`/`VideoGenerator`/`ImageGenerator`; `__init_subclass__` check at import; usable with `MultiprocessNode` and `InProcessNode`.
- [ ] 1P.4 `@register_node`: accept `generates=`, refuse it on a non-`GeneratorNode`, fill it from a `GeneratorNode` class. The worker's discovery frame reports `generates`/`cancel` next to `__rpc_meta__`.
- [ ] 1P.5 Worker runner (`remotemedia.core.multiprocessing.runner`, mapped runner, in-process template): receive the cancel frame, fire the token, apply `pending_input` to frames queued in the process, await `cancel()`; replace the per-node `request_barge_in` latch pattern.
- [ ] 1P.6 Vendor the contract into `templates/inprocess_python_plugin.py` (`InProcessNode`), with a test that the template's copy matches the generated module.
- [ ] 1P.7 Add mypy to CI over `remotemedia/nodes`, the Python plugin examples and the template; a fixture generator with a missing or mis-typed `cancel` must fail it.
- [ ] 1P.8 `remotemedia.testing.assert_generator_cancels`, run through the real worker in multiprocess, mapped, `python_plugin_export`, `python-source-plugin` and in-process modes, with the same failing fixtures as 1.11.
- [ ] 1P.9 Legacy `Node` base: refuse `GeneratorNode` mixing; deprecate it for generators.

## 2. Host (remotemedia-sdk-base)
- [ ] 2.1 Create a token per call in `session_router.rs` and fire it, with its cancellation, at the barge-in, supersession, shutdown (wire the unused `cancel_force` path) and RPC/endpoint cancel sites before the future is dropped; call `cancel_call` on plugin nodes; keep `ProtectGuard` suppression. Router tests for each reason and for a suppressed barge.
- [ ] 2.2 Resolve the disposition (envelope, manifest `on_cancel`, node default, reason) and honour `pending_input`: skip the `input_generation` bump at `session_router.rs:2804-2808` on `Retain`. Until a node declares its role, keep today's discard. Router tests: queued audio survives an interrupt; queued input is dropped on an abort; envelope and manifest overrides.
- [ ] 2.3 Add the optional `disposition` to the `barge_in` envelope (`session_control.rs`) and to RPC/endpoint cancels; parse and validate the manifest `on_cancel` field.
- [ ] 2.4 Barge producers set an explicit disposition: `speculative_vad_gate.rs` and VAD-triggered barges (`INTERRUPT`), `CancelSpeculation` retraction (`ABORT`), `ConversationCoordinatorNode::dispatch_barge`, `HostedTurnNode::cancel`, `RealtimeEventsNode` `response-cancel`, and scaling `InterruptNode` (`remotemedia-scaling/crates/mesh/src/nodes/room_nodes.rs`).
- [ ] 2.5 Implement `cancel_token()` on the host `NodeRuntimeContext`.
- [ ] 2.6 Registry and `src/validation/` refuse `generates` without `cancel`; apply the 0.10 plugin acceptance rule.
- [ ] 2.7 Python multiprocess bridge (`src/python/`, including `legacy_call.rs`, whose cancelled turns keep running today) and `loadable/source_python.rs` send the cancel frame.
- [ ] 2.8 Registry audit test over all built-in Rust factories, `PythonNodeConfig` entries (`crates/python-nodes/src/registry.rs`) and the Python `@register_node` registry: a generative category without a `generates` declaration fails, as does a generator on the legacy `Node` base.
- [ ] 2.9 `PythonNodeConfig` gains `generates`/`cancel`; at session start the host compares them with the worker's discovery frame and refuses a mismatch. `InProcessPythonNode` fires the token directly.

## 3. Migrate generators
Each item: declare `generates` and a strategy, stop work that runs outside the call future (token or `on_cancel`), apply both dispositions to the node's own state (an LLM keeps or rolls back the cut-off reply; a TTS node discards its queued text by default; an S2S node retains buffered audio), and add `assert_generator_cancels`. Paths are relative to `D:\rm-mp`; the current state is noted where it shapes the work.

### 3.1 Built-in Rust (remotemedia-sdk-base/src)
- [ ] 3.1.1 `OpenAIChatNode` (`nodes/openai_chat.rs`) and `MultimodalLLMNode` (`nodes/multimodal_llm.rs`): text, `DropStopsWork` (the reqwest stream closes on drop); move the `ChatBackend` deadline/`ProtectGuard` use from the `cancel_gate` downcast to the token.
- [ ] 3.1.2 `HostedTurnNode` (`nodes/hosted_turn.rs`): text, `OnCancel`; its existing `interrupt`/`cancel(why)` becomes `on_cancel`, keeping the downstream barge publication and the browser `interrupt` frame.
- [ ] 3.1.3 `TestToneTtsNode` (`nodes/test_tone_tts.rs`): audio, `ObservesToken` in its synchronous loop (it cannot be preempted today).
- [ ] 3.1.4 `PythonStreamingNode` factories (`streaming_registry/python_factories.rs`: Kokoro, VibeVoice, CosyVoice3, Voxtral, LFM2Audio): declare the role from the Python class; the bridge already forwards `barge_in`, which becomes the per-call cancel frame.

### 3.2 Python nodes (remotemedia-sdk/clients/python/remotemedia/nodes)
- [ ] 3.2.1 Nodes with a barge latch (`request_barge_in` + `_handle_aux_port("barge_in")`): `KokoroTTSNode`, `KokoroOnnxTTSNode`, `QwenTTSMlxNode`, `LFM2AudioNode`, `LFM2AudioMlxNode`, `PersonaPlexAudioMlxNode`, `QwenTextMlxNode`, `MlxLmTextNode` — move to `GeneratorNode.cancel()`; retire the `Interruptable` and `StatefulConversation` protocols in `nodes/ml/interfaces.py`. `LFM2AudioNode`'s early-interrupt rollback becomes its `default_disposition` (discard partial output when the user cut in early).
- [ ] 3.2.2 Nodes with no cancel today: `VibeVoiceTTSNode` (wire its existing `stop_event` to the token), `CosyVoice3TTSNode`, `VoxtralTTSNode`, `NeuTTSNode`, `MossTTSNode`/`MossVoiceGeneratorNode`/`MossSoundEffectNode` (aux handler currently ignores barge), `MossTTSRealtimeNode`, `LFM2TextNode`, `LFM2InstructAgentNode`, `LFM2DemoAgentNode`, `HermesLLMNode`.
- [ ] 3.2.3 Legacy `Node`-based generators `Qwen2_5OmniNode`, `UltravoxNode`, `TransformersPipelineNode`: migrate to `GeneratorNode` or mark deprecated and refuse admission.
- [ ] 3.2.4 Motion generators `KimodoMotionNode`, `ArdyMotionNode`, `HomeLifeNode` (video): migrate; cancel drops the cancelled turn's queued motion.

### 3.3 Loadable plugins
- [ ] 3.3.1 litert `GemmaAssistantNode` (`remotemedia-sdk-base/examples/litert-nodes/src/assistant.rs`): text, `OnCancel` calling `Conversation::cancel()` directly instead of relying on the closed pieces channel.
- [ ] 3.3.2 litert `KokoroTtsNode` (`tts_node.rs`): audio, `ObservesToken` on its synthesis thread (today the job runs to the end after a drop).
- [ ] 3.3.3 `kokoro-onnx-loadable` (`remotemedia-sdk/examples/`): audio, observe the token within a clause, not only between clauses.
- [ ] 3.3.4 `FoundryLocalChatNode` (foundry-local-loadable): text, `DropStopsWork`, confirmed by the conformance check.
- [ ] 3.3.5 `CandleChatNode` (candle-transformers-loadable): text, `ObservesToken` per token; verify whether its token loop yields at all.
- [ ] 3.3.6 Python plugins (`moss-tts-realtime-loadable`, `lfm2-audio-python-source-plugin`, `lfm2-audio-mlx-python-source-plugin`, `indextts-25-python-source-plugin`): depend on 1.10/2.7, since `python_plugin_export` and `python-source-plugin` currently forward only `shutdown`. (`RVCNode` in rvc-loadable and rvc-python-source-plugin is voice conversion, not a generator.)

### 3.4 Other repositories
- [ ] 3.4.1 `candle-nodes` `PhiNode`/`LlamaNode`: replace the handler for the removed `ControlMessageType::Cancel` with `Cancellable`; the crate is pinned to sdk-base rev 042a82f and must be moved to the new plugin-sdk first.
- [ ] 3.4.2 Out-of-tree generators referenced by `qwen-s2s-avatar-plugins` (`LlamaCppGenerationNode`, `Audio2FaceLipSyncNode`, `Live2DRenderNode`, `CcRenderNode`) and `LFM25AudioOnnxNode`: file migration issues in their repositories; until rebuilt they load as `generates: unknown`.
- [ ] 3.4.3 `remotemedia-scaling` mesh `EchoAgentNode` (test agent): text, `DropStopsWork`.

### 3.5 Avatar and video
- [ ] 3.5.1 `AvatarNode`, `IdleAnimationNode`, `Audio2FaceNode`, `ActivationFaceNode`, `MotionPlayerNode`, `VadToFaceNode`, `SyntheticLipSyncNode` (`remotemedia-sdk-base/src/nodes/avatar`, `motion`, `lip_sync`): video generators; cancel drops the cancelled turn's queued visemes/motion (as `SyntheticLipSyncNode` already does on barge) while idle animation continues. Tick-driven nodes observe the token between ticks.

## 4. Validation
- [ ] 4.1 Plugin SDK CI baseline (`cargo fmt --check`, `cargo check --workspace --all-targets`, `cargo test --workspace`, `--features python-plugin`) plus the native-worker and mapped-probe features.
- [ ] 4.2 `./scripts/test-session-rpc.ps1`: a 0.11 plugin loads and receives `cancel_call`; the pinned old-ABI fixtures follow the documented acceptance rule.
- [ ] 4.3 Base CI baseline plus the session-router cancellation tests.
- [ ] 4.3P Python: `pytest` for the contract, worker and conformance tests in each Python mode; mypy over nodes, examples and template; the schema parity test. Reported per mode, since one mode passing is not coverage of another.
- [ ] 4.4 Real-model runs, reported separately from unit tests: the five-aborted-calls probe on the Gemma assistant (each first text within slack of baseline, then a completed call); a VAD-triggered interrupt on an S2S pipeline where the interrupting utterance is answered in full (its opening words are not lost); and the in-process cancel and barge-in suite on rebuilt plugins.
- [ ] 4.5 `openspec validate add-generator-cancellation --strict --no-interactive`.
