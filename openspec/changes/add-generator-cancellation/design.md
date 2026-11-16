## Context
A call is cancelled by dropping its future. The session router does this on a `barge_in` (`CancelGate::try_cancel`, `session_router.rs` dispatch `select!`), on latest-input-wins supersession (`supersede_on_new_input`), on session shutdown, and for a cancelled RPC or endpoint call. Inside the plugin-sdk ABI adapter, dropping the FFI future aborts the spawned tokio task (`AbortOnDrop`). None of these reach work outside the future: a model thread, a native or Python worker computing a step, a GPU stream, a remote generation that is billed until the connection closes. Each node had to notice on its own, usually through a closed channel, and the litert assistant did so only in room mode.

A barge-in today also throws data away. The router bumps the node's `input_generation`, and every input queued before the barge is discarded (`session_router.rs:2804-2808`). For a speech-to-speech node fed continuous audio, that discards the start of the very utterance that caused the barge. Cancel is used for two different intents: an *interrupt* (stop talking, the conversation goes on with everything said so far, including the new speech) and an *abort* (this turn is void: retracted speculation, a reset, a closed session).

## Goals / Non-Goals
- Goals: one cancellation signal that reaches every execution mode; a generator cannot be written, exported or admitted without stating how it stops; every cancel says what happens to the cancelled call's partial output and to pending input, and an interrupt keeps both by default; a reusable check that proves a generator stops and honours the disposition.
- Non-Goals: changing when the runtime cancels (barge-in policy, `ProtectGuard`, supersession stay as they are); cancelling transforms (they may use the token, nothing requires it); preemptive killing of foreign threads.

## Decisions

### D1. `CancelToken` is the single per-call signal
`remotemedia_traits::cancel::CancelToken` is `Clone + Send + Sync`, std-only (an `Arc` holding an atomic state, the first `Cancellation`, and registered wakers/callbacks), so it works from async code, blocking model loops and FFI callbacks without a tokio dependency:

```rust
impl CancelToken {
    pub fn is_cancelled(&self) -> bool;
    pub fn cancellation(&self) -> Option<Cancellation>;
    pub fn cancelled(&self) -> impl Future<Output = Cancellation> + Send; // select!-able
    pub fn on_cancel(&self, f: impl FnOnce(&Cancellation) + Send + 'static) -> CancelRegistration; // at most once, unregisters on drop
    pub fn child(&self) -> CancelToken;
}
```

`NodeRuntimeContextRead::cancel_token(&self) -> CancelToken` returns the token of the current call. The default never fires, so existing transforms compile unchanged. The host creates one token per call and fires it at each cancel site **before** it drops the future, so `on_cancel` and the node's threads see the cancellation while the future still exists. A `ProtectGuard` that suppresses a barge suppresses the token too.

Alternatives considered: exposing `CancelGate` (tokio-only, host type, per node rather than per call, carries protection semantics plugins should not see); a tokio `CancellationToken` (adds tokio to the leaf traits crate, unusable from a plain thread).

### D2. A cancellation says what happens to data
```rust
pub struct Cancellation {
    pub reason: CancelReason,       // BargeIn, Superseded, CallerCancelled, Retracted, SessionClosed, Shutdown, Deadline
    pub disposition: Disposition,
}
pub struct Disposition {
    /// What the node does with what it generated before the cancel.
    pub partial_output: Retain | Discard,
    /// What happens to input already received or queued for this node.
    pub pending_input: Retain | Discard,
}
impl Disposition {
    pub const INTERRUPT: Self = Self { partial_output: Retain,  pending_input: Retain  };
    pub const ABORT:     Self = Self { partial_output: Discard, pending_input: Discard };
}
```

- `partial_output: Retain` — the node commits what it produced up to the cancel to its state (an LLM keeps the cut-off reply in history, marked interrupted, as the Gemma assistant already does); `Discard` rolls it back as if the call never ran. Output already emitted downstream is never recalled; this is about the node's own state.
- `pending_input: Retain` — inputs queued for the node, and any input the node has buffered internally (an S2S node's audio accumulator), are kept and feed the next call; the router does **not** bump `input_generation`. `Discard` keeps today's behavior: queued inputs from before the cancel are dropped.

Who decides, in order of precedence:
1. The canceller. The `barge_in` envelope gains an optional `disposition`; the caller of an RPC or endpoint cancel may pass one.
2. The manifest. A node's `on_cancel: { partial_output, pending_input }` overrides its defaults for that pipeline.
3. The node. `Cancellable::default_disposition(&self, &CancelContext) -> Disposition` lets a node choose per cancel — a TTS node discards its queued text (it belongs to the cancelled reply), an S2S node retains its audio, and LFM2Audio's early-interrupt rollback becomes "discard partial output when the user cut in within N ms".
4. The reason. `BargeIn` and `Superseded` → `INTERRUPT`; `Retracted` (CancelSpeculation), `CallerCancelled`, `SessionClosed`, `Shutdown` → `ABORT`; `Deadline` → `ABORT`.

So a VAD-triggered barge is an interrupt by default: generation stops, the cut-off reply stays in history, and the audio keeps flowing into the next inference. A producer states a disposition only when it means more than a plain interrupt, because an explicit choice overrides every node's default (a TTS node's default drops its queued text; an explicit `INTERRUPT` would make it speak the stale reply). As implemented: `ConversationCoordinatorNode` and VAD-driven barges send none; `HostedTurnNode` dropping a turn (and its remote half in `MemberFlushNode`) sends `pending_input: discard`; a retracted utterance sends `ABORT`. Producers publish `barge_in_payload(...)`, which puts `{"disposition": ...}` in the barge envelope's payload.

**Wire rule for a `CancelRequest`, shared by Rust and Python.** A request that carries `explicit` or `elapsed_ms` is re-resolved by the node with its own default; one with only `cancellation`, and a bare `Cancellation`, are final. Rust always serializes `explicit` (also when empty) and the router always sets `elapsed_ms`.

### D3. Typed role: generators must be `Cancellable`
```rust
pub enum CancelStrategy {
    /// All generation happens inside the call future; dropping it stops the work.
    DropStopsWork,
    /// Work outside the future polls or subscribes to `ctx.cancel_token()`.
    ObservesToken,
    /// The node stops work out of band in `on_cancel`, and may also observe the token.
    OnCancel,
}

#[async_trait]
pub trait Cancellable: Send + Sync {
    fn cancel_strategy(&self) -> CancelStrategy;                       // no default
    fn default_disposition(&self, cancel: &CancelContext) -> Disposition { cancel.reason.default_disposition() }
    /// Stop out-of-band work and apply `cancel.disposition` to the node's own state.
    async fn on_cancel(&self, call: &CallContext, cancel: &Cancellation) {}
}

pub trait TextGenerator:  Cancellable {}
pub trait AudioGenerator: Cancellable {}
pub trait VideoGenerator: Cancellable {}   // includes avatar, lip-sync and motion renderers
pub trait ImageGenerator: Cancellable {}
```

`cancel_strategy` has no default, so implementing `Cancellable` is a decision, not a blanket impl. The generator traits live in `remotemedia-traits` and are re-exported by `remotemedia-modality`.

Enforcement points, from strongest to widest coverage:
1. **Compile time (Rust nodes).** `#[node(generates = "text" | "audio" | "video" | "image")]` in `core-derive` emits a bound check on the matching generator trait; the generator factory constructor and the plugin export macro's generator form carry the same bound. Omitting `Cancellable` fails the build with an error naming the missing trait.
2. **Declaration.** `NodeSchema` and `NodeDescription` (the factory `describe()` JSON) gain `generates: Option<GeneratedModality>` and `cancel: Option<CancelStrategy>`. The derive fills both.
3. **Host admission (all modes).** The registry refuses a factory that declares `generates` without `cancel`; manifest validation refuses a pipeline containing one, and validates a node's `on_cancel` override.
4. **Audit.** A registry test fails when a node's schema category is generative (`llm`, `tts`, `t2i`, `video`, `avatar`, `motion`, `text-generation`) but it does not declare `generates`.

Not generators: speech-to-text (streaming or not) and voice conversion (`RVCNode`); they transform input and stop when it stops. They may still use the token. This applies equally to Rust and Python nodes.

### D3b. The same contract in Python
Python generators get the same types, the same declaration and the same checks, as early as Python allows. The contract lives in the SDK Python package (`remotemedia.core.cancel`, in `remotemedia-sdk/clients/python`), which `MultiprocessNode` already comes from in the multiprocess, `python_plugin_export` and `python-source-plugin` modes, and is vendored into the plugin-sdk in-process template (`templates/inprocess_python_plugin.py`, base `InProcessNode`).

```python
class CancelStrategy(enum.Enum): DROP_STOPS_WORK = "drop_stops_work"; OBSERVES_TOKEN = "observes_token"; ON_CANCEL = "on_cancel"
class CancelReason(enum.Enum): BARGE_IN = "barge_in"; SUPERSEDED = "superseded"; ...
@dataclass(frozen=True)
class Disposition: partial_output: Keep; pending_input: Keep   # Keep = Literal["retain", "discard"]; INTERRUPT / ABORT constants
@dataclass(frozen=True)
class Cancellation: reason: CancelReason; disposition: Disposition

class CancelToken:                       # thread-safe: model threads poll or wait, asyncio code awaits
    def is_cancelled(self) -> bool: ...
    def cancellation(self) -> Optional[Cancellation]: ...
    async def cancelled(self) -> Cancellation: ...
    def wait(self, timeout: Optional[float] = None) -> bool: ...   # for blocking threads
    def on_cancel(self, fn: Callable[[Cancellation], None]) -> CancelRegistration: ...

class Cancellable(Protocol):
    cancel_strategy: ClassVar[CancelStrategy]
    def default_disposition(self, ctx: CancelContext) -> Disposition: ...
    async def cancel(self, call: CallInfo, cancellation: Cancellation) -> None: ...

class GeneratorNode(abc.ABC):            # mixin for MultiprocessNode and InProcessNode
    generates: ClassVar[GeneratedModality]
    cancel_strategy: ClassVar[CancelStrategy]
    @abc.abstractmethod
    async def cancel(self, call: CallInfo, cancellation: Cancellation) -> None: ...
    def default_disposition(self, ctx: CancelContext) -> Disposition: ...   # reason default
    @property
    def cancel_token(self) -> CancelToken: ...   # the current call's token

class TextGenerator(GeneratorNode): generates = GeneratedModality.TEXT   # also Audio/Video/ImageGenerator
```

The Python and Rust types are generated from one JSON Schema (exported from the Rust types with `schemars`), and a parity test fails if either side's enums or fields differ.

### D3c. Enforcement parity
Each check exists for both languages; Python's run at the earliest point Python offers.

| Check | Rust | Python |
| --- | --- | --- |
| Type check | `#[node(generates)]`, generator factory and export macro require `…Generator: Cancellable`; build fails | `GeneratorNode`/`Cancellable` are typed; mypy (already `disallow_untyped_defs`) runs in CI over `remotemedia/nodes`, the Python plugin examples and the template, and rejects a `cancel` with the wrong signature |
| Definition | — (covered at build) | `GeneratorNode.__init_subclass__` rejects a concrete subclass without `cancel` or `cancel_strategy` at import; `@register_node(generates=...)` rejects a class that is not a `GeneratorNode`, and `@register_node` on a `GeneratorNode` fills `generates` itself |
| Instantiation | — | `abc` raises `TypeError` for a missing `cancel` |
| Declaration | `NodeSchema`/`describe()` carry `generates`/`cancel` | the worker's node discovery frame (beside `__rpc_meta__`) carries them; `PythonNodeConfig` (`crates/python-nodes`) gains `generates`/`cancel` |
| Host admission | registry and validation refuse `generates` without `cancel` | same, plus the host refuses a session when the discovery frame disagrees with the `PythonNodeConfig` declaration |
| Audit | registry test over Rust factories | the same test over `PythonNodeConfig` entries and the `@register_node` registry; a generative category without `generates`, or a generator on the legacy `Node` base, fails |
| Conformance | `assert_generator_cancels` | `assert_generator_cancels` (Python twin) run through the real worker in each Python mode |

A hand-written Rust node exported through the plain `plugin_export!` form, and a Python node registered without `generates`, escape the type and definition checks; the audit and the generative-category rule exist for those.

Alternatives considered: a required `fn role(&self) -> NodeRole` on every node (forces an edit to every transform for no behavioral gain); a runtime-only flag (not typed).

### D4. Delivery across execution boundaries
- **In-process Rust:** the router's per-call token is the context token.
- **Loadable plugins:** a new defaulted `FfiNode::cancel_call(call_id, cancellation_json)` entry. The host calls it, then drops the future. The plugin-sdk adapter maps the call id to the plugin-side token, fires it with the full `Cancellation`, and runs `on_cancel` on the plugin runtime. If the future is dropped without `cancel_call` (an older host, a panic path), the adapter's drop guard fires the token with `CallerCancelled`/`ABORT`, as does a failed `OutputSink::push`. Adding the vtable entry is a prefix extension, which this ABI has used before, but it changes the interface, so **`ABI_VERSION` moves to the next minor (0.11)** with the matching host acceptance rules and fixture updates. `describe()` JSON carries `generates`/`cancel`.
- **Native workers:** `ctx.cancel_token()` is backed by the request's existing cancel flag; the cancellation travels with the existing `HostMessage::Cancel`, extended with the reason and disposition.
- **Python (mapped, subprocess, in-process, `python_plugin_export`, `python-source-plugin`):** the bridge sends one per-call cancel frame carrying the reason and disposition when the host cancels a call. This reuses what exists (`PythonStreamingNode` already forwards `barge_in`, `MultiprocessNode` already cancels `.__cancel__` RPC streams, mapped calls expose `execution_context.is_cancelled()`); the new work is the same frame in every mode, including the plugin modes that today forward only `shutdown`. The worker builds the per-call `CancelToken` (D3b), fires it with the frame's `Cancellation`, then awaits `cancel()`; `pending_input` is applied by the worker to frames already queued in the Python process. In-process Python hosted by pyo3 (`InProcessPythonNode`) fires the token directly without a frame.
- **Router input queue:** `pending_input: Retain` skips the `input_generation` bump for that node; `Discard` keeps it.

### D5. Conformance check
`remotemedia_plugin_sdk::testing::assert_generator_cancels(node, input, opts)` asserts, for each disposition the node can receive:
- no output from the cancelled call after `opts.grace` (default 250 ms), and `on_cancel` ran or the token was observed;
- the next call reaches its first output within `opts.next_call_slack` of a baseline (the check the Gemma regression failed);
- under `INTERRUPT`, input sent during and right after the cancel is used by the next call, and a stateful node's next call sees the retained partial output; under `ABORT`, neither is.
The Python twin (`remotemedia.testing.assert_generator_cancels`) makes the same assertions, driving the node through the real worker of each Python mode rather than calling methods directly. Model-backed nodes also run it in their real-model suites.

## Risks / Trade-offs
- `DropStopsWork` can be claimed incorrectly. The conformance check runs for every generator regardless of strategy.
- Firing the token before the drop means `on_cancel` runs concurrently with the tail of the call; nodes must treat it as idempotent.
- Retaining pending input by default on barge-in changes current behavior: inputs that were dropped are now processed. A node or pipeline that relied on the drop (TTS text from the cancelled reply) must discard, which the TTS migration does through `default_disposition`. Each migrated TTS node's conformance test covers it.
- Old plugin binaries: an 0.10 plugin is handled per the ABI bump's acceptance rule (rejected, or loaded as `generates: unknown` if the host keeps accepting 0.10); decided in task 1.7.

## Migration Plan
1. Land `CancelToken`, `Cancellation`/`Disposition`, `Cancellable`, generator traits and the schema/description fields (additive).
2. Host fires tokens with dispositions at every cancel site, the router honours `pending_input`, adapters and workers deliver cancellations; barge producers set explicit dispositions. Until step 4 the router keeps today's discard for nodes that have not declared a role, so un-migrated pipelines do not change behavior.
3. Migrate each generator in the inventory and add its conformance test.
4. Turn on admission refusal and the audit test, make `INTERRUPT` the barge default for all generators, deprecate `InterruptableBackend` and `StatefulConversationBackend::finalize_turn`. Rollback: disable the admission check and the default; the additive pieces stay.

## Implementation notes
- **Token firing order.** `tokio::select!` drops the losing branches before it runs the winner's handler, so the router fires the call's token inside the cancel branch's own future; token callbacks (`cancel_call`, the Python cancel frame) run before the call's future is dropped.
- **Call identity across the ABI.** `FfiCallContext` has no call id; adding one would change a by-value struct. The router runs one media call at a time per instance and calls `cancel_call` synchronously before the drop, so "the media call in flight on this instance" names the call; the adapter registers each call before returning its future and clears it when it ends, so a late cancel is a no-op.
- **Queue decisions for plugin and Python nodes.** The router needs a node's `pending_input` default when the cancel arrives, but the node lives in another library or process. Generators declare their default per reason up front (`pacing()` JSON `default_dispositions`; Python `__node_meta__`), the router uses it for its queue, and the node re-resolves `partial_output` and its own buffers with its per-cancel default.
- **Undeclared nodes** keep today's queue behavior on a barge (queued inputs dropped) until they declare a role, so un-migrated pipelines do not change.
- **Audit.** `video` is not a generative category (most video nodes transform frames); reviewed transforms in generative categories are listed in the audit.

## Open Questions
- Old plugins: the ABI moved to 0.11, so 0.10 plugins must be rebuilt (the repository's practice for interface changes). Verifying the rejection with `test-session-rpc.ps1` is task 4.2.
- Should a pipeline be able to set a session-wide default disposition for barge-in, in addition to per-node `on_cancel`?
