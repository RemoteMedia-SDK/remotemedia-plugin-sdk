## Decisions
Use owned ABI-safe session/node strings, with no shared mutable current-session field. Plugins receive the existing read-only context interface; host-only state/capabilities are not transferred. A missing session remains absent through legacy delegation.

Append defaulted FfiNode methods for context-aware processing, RPC method metadata and RPC invocation. The new asynchronous RpcReplySink accepts MessagePack-encoded typed replies and is separate from the synchronous media OutputSink. The host binds sinks to a session/node; request payloads cannot change their destination.

Keep the established aux-port calls and corr_id/value/end/error reply protocol. Rust method descriptors declare ff/reply/stream modes. A per-call responder enforces terminal behavior and provides serde-based typed argument and result helpers. Dropping an FFI invocation aborts its plugin-runtime task; blocking native work remains cooperatively cancellable only.

The host routes RPC only for nodes advertising methods, preserving legacy aux handling for other nodes. Run bounded concurrent RPC tasks separately from media. Handle stream cancellation by method/correlation ID, abort all tasks on shutdown, and keep replies out of media fanout. A slow RPC reply subscriber must receive an explicit lag failure rather than silently skipping terminal frames.

## Compatibility
Existing Rust implementors retain defaults and compile after rebuilding. The repository's ABI policy requires a minor version bump for vtable additions; binary compatibility is tested, not assumed. No new RuntimeData variant or transport is introduced. Audio.cpp migration and generated client stubs are follow-on consumers, not required SDK implementation changes.

## Validation
Test session and node propagation through generated ABI trait objects and actual loaded DLLs; legacy defaults; unary/stream/error/metadata behavior; independent RPC during media; cancellation, sink failure and shutdown; and old/new ABI version behavior. Run focused Rust suites and formatting checks. Record actual commands and limitations.
