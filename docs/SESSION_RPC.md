# Session-aware native processing and RPC

This worktree introduces **plugin-sdk 0.7 / loadable-node-abi 0.5**. It is an
unreleased SDK change. Build the host and plugins against the same revision.
ABI 0.4 binaries require rebuilding; retaining default Rust methods preserves
source compatibility, not binary compatibility.

## Processing identity

Hosts call `FfiNode::process_streaming_with_context(input, context, media_sink)`.
`FfiCallContext` owns an optional session ID and a node ID. The SDK reconstructs
a `rpc::CallContext` and calls `AsyncStreamingNode::process_streaming_with_ctx`.
Its existing default delegates to `process_streaming` with the session ID.
An override can also read `ctx.node_id()`.

Identity belongs to each invocation, never a mutable global or a JSON payload.
Applications multiplexing substreams should key their state by host session,
node and an optional application stream ID. The context carries identity only;
host capabilities, snapshots and arbitrary session-state objects are not copied
across the DLL boundary. Legacy context-free entry points remain available.

## Native methods

Implement `rpc_methods()` and `call_rpc()` on `AsyncStreamingNode`. The example
at `examples/session-rpc` builds a real DLL with unary, streaming and failing
methods. The author-facing types live in `remotemedia_plugin_sdk::rpc`:

- `RpcMethod` declares a name, `RpcMode` and optional argument/result schemas.
- `RpcRequest::positional::<(String, usize)>()` decodes positional arguments;
  `named::<MyParams>()` decodes keyword arguments using serde.
- `RpcResponder::value(result).await` emits a typed result with backpressure.
  Return `Ok(())` when finished or `Err(error)` to report a failure.

Unary values terminate their call. Streams emit values followed by exactly one
`end`; failures before termination emit `error`. A unary handler returning
without emitting a value returns JSON null. Fire-and-forget handlers emit no
reply. Sending after termination or into a closed sink fails. Handlers should
propagate emitter errors with `?` and perform any fallible work before emitting
a unary result.

The ABI uses MessagePack `RpcRequest`/`RpcReply` payloads and a separate
asynchronous `RpcReplySink`; media continues to use `OutputSink`. The host binds
the reply destination. Plugins cannot choose a different session or node by
changing payload fields. A request contains `method`, `corr_id`, `args` and
`kwargs`; a reply contains `corr_id`, `kind` and optional `data`.

## Host integration

The companion `remotemedia-sdk-base` worktree discovers RPC method descriptors
and registers a dedicated per-node ingress queue. It handles `__rpc_meta__`
using the existing `{method: "ff" | "reply" | "stream"}` discovery response.
Existing Python/JS proxies therefore use their existing call syntax:

```js
const identity = await ctrl.manager.identity();
for await (const value of ctrl.manager.watch(3)) console.log(value);
```

Replies go only to `<node>.out.__reply__`, outside media fanout and intercepts.
Nodes declaring no RPC methods retain their existing aux-port behavior. The
native RPC lane admits at most 32 concurrent calls with a 32-command input
queue; overload returns an explicit error. RPC handlers run concurrently with
media and other RPCs, so native mutable handles still require synchronization.
Calls require an initialized node and its registered control endpoint, as with
other session control operations.

Stream cancellation uses `<method>.__cancel__` with the original correlation
ID. It aborts the invocation, not an independently managed download job.
Session shutdown aborts all invocation tasks. Dropping an SDK FFI future aborts
its plugin-runtime task. Blocking native kernels still require cooperative
cancellation; this does not make native code force-interruptible. Caller-side
unary timeouts retain the existing protocol semantics and do not imply a server
job cancellation.

The companion gRPC/WebRTC host now binds calls to the control attachment that
published them. Closing that attachment cancels its calls even when another
subscriber remains on the reply tap; explicit cancellation cannot cancel another
attachment's call. Direct host callers can opt into this with `RpcCallScope` and
`SessionControl::publish_scoped`, or retain the existing session-owned `publish`.

The ABI reply channel is bounded and awaits capacity. Existing control-bus taps
are broadcasts: reply subscribers that lag must fail explicitly rather than
skip frames. The companion Python, Node.js, gRPC and WebRTC adapters report RPC
reply lag; ordinary media tap semantics are unchanged. This is not durable or
replayable RPC delivery.

## Verification

On Windows, run `scripts/test-session-rpc.ps1` in this SDK worktree. It runs the
focused suites, builds current and pinned ABI 0.4 DLLs, checks both incompatible
version directions, and runs the companion host contract tests including the
actual DLL round trip. Cargo must be available, with its normal dependency
cache/network access. The companion host worktree must be adjacent and named
`remotemedia-sdk-base`. The script also runs the JavaScript regression and, when
the adjacent `remotemedia-sdk` client worktree is present, its Python RPC tests
(requires Node.js, Python, pytest and the Python client dependencies).

The script runs Cargo outside parent checkouts to avoid inheriting unrelated
`.cargo/config.toml` files. It restores its temporary environment settings.
See [validation evidence](SESSION_RPC_VALIDATION.md) for the exercised matrix
and platform limits.
