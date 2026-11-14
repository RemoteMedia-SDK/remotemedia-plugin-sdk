# RemoteMedia Plugin SDK

## Scope and structure

This workspace owns contracts shared by hosts and plugins, plus plugin authoring and worker support. Host-side plugin discovery, loading policy and pipeline execution live in `../remotemedia-sdk-base/src/loadable/` and `../remotemedia-sdk-base/src/transport/`. The SDK facade, clients and examples live in `../remotemedia-sdk/`; distributed hosts live in `../remotemedia-scaling/`.

| Crate directory | Responsibility |
| --- | --- |
| `crates/loadable-node-abi/` | ABI-stable host/plugin interface and runtime bridge |
| `crates/types/` | Runtime payloads, media ownership and shared data types |
| `crates/traits/` | Streaming node, RPC, capability and buffer contracts |
| `crates/modality/` | Modality-specific interfaces |
| `crates/core-derive/` | Node/config procedural macros |
| `crates/plugin-sdk/` | Authoring adapters, exports, native and Python workers |
| `crates/py-env-manager/` | Managed Python environments and uv provisioning |
| `crates/shared-memory/` | Shared-memory ownership and worker protocols |
| `crates/python-buffers/` | Python buffer bridge |
| `crates/device-cuda/` | CUDA device-buffer support |
| `examples/`, `integration/` | Real plugins and boundary fixtures |

## Contract rules

- Read [the ABI contract](docs/LOADABLE_NODE_ABI.md) and [session RPC guide](docs/SESSION_RPC.md) for the affected boundary, then verify the definitions and manifests. Some prose still quotes earlier release lines. Package versions come from each crate's `Cargo.toml`; `ABI_VERSION` in `crates/loadable-node-abi/src/lib.rs` comes from that crate's package version.
- Use ABI-safe types across dynamic-library boundaries. Do not pass ordinary Rust trait objects or native `RuntimeData` layouts across the DLL boundary. Preserve owning media buffers, allocator/lifetime contracts, snapshots and copy accounting.
- A Rust default method does not guarantee compatibility with an already-built plugin. Interface/layout changes require the matching versioning decision, host adapter changes, fixture updates and real binary compatibility checks. Rebuild both sides when required by the contract.
- Invocation identity comes from host call context. RPC requests/replies have a dedicated channel separate from media outputs; preserve correlation, backpressure, terminal replies, cancellation and session isolation.
- Keep `native-worker`, `mapped-python`, `python-plugin` and `inprocess-python` behavior distinct. Changes to provisioning must preserve the declared Python requirements and environment/cache identity.
- Read each sibling repository's `AGENTS.md` when changing its consumer. Avoid introducing a dependency on the full host runtime merely to implement a shared contract or authoring helper.

## Build and validation

The workspace declares Rust 1.88 and edition 2021; `rust-toolchain.toml` selects stable, rustfmt and clippy. Default members omit some buffer/device and integration crates. Inspect `Cargo.toml` and `cargo metadata --no-deps --format-version 1` before assuming a plain build covers them.

From this repository, the CI baseline in `.github/workflows/rust.yml` is:

```sh
cargo fmt --check
cargo check --workspace --all-targets
cargo test --workspace
cargo test -p remotemedia-plugin-sdk --features python-plugin
cargo test -p remotemedia-py-env --features bundled-uv
```

CI uses Python 3.12 with NumPy for tests embedding Python. Configure a compatible interpreter and native toolchain before running those tests. uv provisioning tests may need cached artifacts or network access.

- For native-worker changes, include the `test-native-worker` feature and the relevant tests in `crates/plugin-sdk/tests/`. For mapped Python, inspect the `test-mapped-probe` feature and mapped-worker tests. Linux supervisor and namespace tests require Linux; another platform's successful build is not coverage of those paths.
- For ABI/RPC changes on Windows, run `./scripts/test-session-rpc.ps1`. It builds a current example and the pinned old-ABI fixture, checks acceptance/rejection, and invokes the adjacent base host's integration tests. It requires `../remotemedia-sdk-base/`, Node.js, and Python/pytest/client dependencies when the adjacent SDK client is present.
- `examples/session-rpc/` and the old-ABI fixture have separate manifests. `integration/shared-buffer-plugin/` and `integration/abi-v05-fixture/` are workspace members. Keep historical compatibility fixtures pinned to the contract they test; do not update them indiscriminately to the newest ABI.
- Test buffer changes through both the owner and consumer, including disposal and failure paths. Compilation alone does not establish zero-copy behavior or a working dynamic-library round trip.

Plugin packaging conventions are in [PLUGIN_PUBLISHING.md](docs/PLUGIN_PUBLISHING.md), `templates/`, and `.github/workflows/build-plugin.yml`. Preparing a plugin or updating guidance does not itself request a release.

## Specifications

Check local `openspec/changes/` before changing ABI, wire formats, lifecycle or authoring APIs. Link the corresponding host/transport work and specify the compatibility and validation consequences. Use [the SDK OpenSpec workflow](../remotemedia-sdk/openspec/AGENTS.md) when that checkout is available, running commands from the repository containing the change. Implement an already-approved scope without requesting the same approval again. Documentation corrections and fixes restoring intended behavior do not need a new proposal.
