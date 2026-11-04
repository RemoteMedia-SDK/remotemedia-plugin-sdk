# Session/RPC validation

Environment: Windows x64, MSVC, 2026-11-02. These are local worktree results;
they do not represent a published SDK release or an audio.cpp inference run.

## Worktrees

The four checkouts are adjacent under `D:/remotemedia-sdk/.worktrees/session-rpc`:

| Checkout | Branch | Changes |
| --- | --- | --- |
| `remotemedia-plugin-sdk` | `feat/session-aware-rpc` | Types, ABI, adapter, example DLL, compatibility fixtures and documentation |
| `remotemedia-sdk-base` | `feat/loadable-session-rpc` | Host identity forwarding, concurrent RPC dispatch, caller-scoped cancellation and transport error reporting |
| `remotemedia-sdk` | `fix/rpc-subscription-errors` | Python/JS pending-call failure handling and Python cancellation addresses |
| `remotemedia-scaling` | `test/loadable-session-rpc` | Two-process network gate, admission version and reproducible native builds |

## Reproduce

Run `scripts/test-session-rpc.ps1` from the SDK checkout. It invokes:

```text
cargo test -p remotemedia-plugin-sdk -p remotemedia-traits
cargo build --manifest-path examples/session-rpc/Cargo.toml
cargo build --manifest-path crates/plugin-sdk/tests/fixtures/abi-v04/Cargo.toml
probe.exe <ABI-0.4-DLL> accept
probe.exe <ABI-0.5-DLL> reject
cargo test -p remotemedia-plugin-sdk --test binary_compatibility -- --ignored
cargo test --manifest-path ../remotemedia-sdk-base/integration/session-rpc/Cargo.toml -- --include-ignored
node ../remotemedia-sdk-base/crates/transports/ffi/nodejs/scripts/test-rpc-subscription.cjs
python -m pytest <client>/control/tests/test_rpc_proxy.py <client>/control/tests/test_transport_split.py -q
```

The script supplies absolute manifest paths, target directories, `ABI_V04_DLL`,
`SESSION_RPC_DLL` and `PYTHONPATH`. It requires the adjacent host checkout;
Python regressions run when the adjacent client checkout is available.

Then run `../remotemedia-scaling/scripts/test-session-rpc.ps1`. It builds the
same example DLL and runs two real runner processes. See the scaling checkout's
`docs/SESSION_RPC_COMPATIBILITY.md` for the network cases and rollout contract.

## Evidence

- SDK and traits: 24 passing tests (5 SDK unit, 6 existing adapter, 7 new
  session/RPC, 6 trait unit). Existing documentation examples remain ignored.
- The compatibility test is initially ignored without a fixture, then explicitly
  run with the ABI 0.4 DLL: one passing test.
- ABI matrix: old host/old DLL accepted; old host/new DLL rejected; new host/old
  DLL rejected with a version diagnostic; new host/new DLL exercised successfully.
- Companion host: five passing integration tests, including the explicitly
  enabled real DLL test. They cover concurrent session identity, blocked media,
  saturated media ingress, metadata, unary and streaming results, failures,
  fire-and-forget, malformed arguments, call limits, cancellation, session
  shutdown, reply tap overflow and absence of RPC output in media fanout.
- Python: 18 passing proxy and transport tests, including propagation of
  subscription errors and valid stream cancellation addresses.
- JavaScript: one passing regression verifies failure of pending unary and
  streaming calls and refusal of new registration after subscription failure.
- Scaling: one passing multi-case integration test uses gRPC registration and
  real WebRTC control channels across two runner processes. It verifies local
  session identity (including identical local IDs on separate runners), isolated
  state and correlation IDs, blocked media, typed replies, cancellation,
  controlled relocation, session recreation and reconnect. Disconnecting an idle
  RPC caller cancels its invocation while another attachment's active RPC survives.
- gRPC control bus: six passing network end-to-end tests cover attachment,
  missing sessions, publish, tap, intercept, node state and session shutdown.
  These suites total 56 passing tests, plus the standalone binary probes.
- `cargo check --features loadable,loadable-export` for the companion host and
  `cargo check -p remotemedia-ffi --no-default-features --features python,napi`
  passed. The duplicated FFI modules in the client checkout match the checked
  host modules.
- gRPC and WebRTC compilation passed with
  `cargo check -p remotemedia-grpc -p remotemedia-webrtc --no-default-features --features remotemedia-grpc/multiprocess,remotemedia-webrtc/grpc-signaling`.
  Use `--manifest-path <host>/Cargo.toml` from outside parent checkouts. The
  transports currently require `multiprocess` for their Python node imports.
- Changed Rust files pass `rustfmt --check --edition 2021 --config skip_children=true`;
  all four checkouts pass `git diff --check`.
- `openspec validate add-session-rpc --strict` passed.

## Limits

The first network transport build failed inside vendored protobuf because its
MSVC runtime setting differed from Abseil's. The scaling script and CI now use
a CMake toolchain setting `protobuf_MSVC_STATIC_RUNTIME=OFF`; the script also
repairs existing protobuf caches. Runner protocol builds select vendored protoc
unless `PROTOC` is explicitly provided. Generated protobuf source changes from
verification were restored to the checkout baseline.

The loaded-DLL RPC network gate uses WebRTC; its gRPC traffic is runner
registration, diagnostics and lifecycle management. Native Python/Node DLL
loading is not covered; client regressions use actual proxy implementations with
controlled transports. There is no verified Linux/macOS binary matrix yet, and
remote CI has not run. Relocation uses explicit register/terminate operations;
automatic scheduler migration and replay of in-flight RPCs are not covered.

ABI 0.5 requires host/plugin rebuilds. Default methods preserve Rust source
compatibility only. Identity context does not transfer host capabilities or
arbitrary state objects. Native blocking kernels need cooperative cancellation.
The bounded ABI reply sink provides backpressure, while broadcast control taps
report lag; replies are not durable or replayable.

Scaling entry and runners must roll out together at admission generation 0.7.0.
The authoritative routing identity is `(runner, local SDK session, node)`;
local session IDs are not globally unique. gRPC/WebRTC control attachments own
their RPC lifetimes; direct host publishers opt in with `publish_scoped`.

Ripwire's Windows/WSL worktree lookup could not resolve a Git baseline, so its
quality-delta check produced no verdict. Review used source diffs, compiler
checks and the regression suites above.
