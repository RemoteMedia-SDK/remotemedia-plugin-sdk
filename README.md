# RemoteMedia Plugin SDK

Author-facing Rust crates for building RemoteMedia loadable plugins.

This repository owns the small compatibility surface shared by
RemoteMedia hosts and dynamically loaded plugin cdylibs:

- `loadable-node-abi` — the `abi_stable` boundary loaded by hosts.
- `remotemedia-types` — wire-format payload types.
- `remotemedia-traits` — streaming node traits and schema metadata.
- `remotemedia-py-env` — Python environment provisioning for Python-backed cdylib plugins.
- `remotemedia-plugin-sdk` — plugin authoring adapter and export macros.

Host-side plugin resolution and pipeline execution live in
`RemoteMedia-SDK/remotemedia-sdk`.

## Rust Plugin Dependency

Published plugins should depend on the released SDK line:

```toml
remotemedia-plugin-sdk = "0.5"
```

When testing an unreleased SDK line, pin a git revision instead of a
branch:

```toml
remotemedia-plugin-sdk = { git = "https://github.com/RemoteMedia-SDK/remotemedia-plugin-sdk", rev = "<sha>" }
```

## Verification

```bash
cargo fmt --check
cargo check --workspace --all-targets
cargo test --workspace
cargo test -p remotemedia-plugin-sdk --features python-plugin
```

## ABI Contract

See [docs/LOADABLE_NODE_ABI.md](docs/LOADABLE_NODE_ABI.md).
