# Loadable Node ABI Contract

The `loadable-node-abi` crate defines everything that crosses the
`dlopen` boundary between a host (built into a binary) and a loadable
node plugin (built as a `cdylib` and loaded at runtime). This document
is the canonical spec — it explains what's in the contract, what's
deliberately not, and the rules both sides must follow to keep
plugins forward-compatible.

The contract is enforced at three layers:

| Layer | Where | Catches |
|---|---|---|
| Release workflow dependency gate | `.github/workflows/build-plugin.yml` | a plugin resolving an old `remotemedia-plugin-sdk` / `loadable-node-abi` before binaries are uploaded |
| Cargo resolver — plugin pins or versions SDK intentionally | each plugin's `Cargo.toml` | the wrong SDK snapshot being used to build the plugin |
| `loadable-node-abi` semver | `cargo` version resolution | plugin sources that haven't been rebuilt for a newer ABI |
| `abi_stable` runtime check | `NodePluginRef::load_from_file` at host startup | a built .so that doesn't match the host's expected layout |

If a plugin makes it past these checks, the host can call into it
safely.

## 1. The ABI surface

Everything in this section lives in [`crates/loadable-node-abi/src/lib.rs`](../crates/loadable-node-abi/src/lib.rs).

### Root module

Every plugin exports a single `abi_stable` root module under the
symbol `node_plugin`:

```rust
#[repr(C)]
#[derive(StableAbi)]
#[sabi(kind(Prefix(prefix_ref = NodePluginRef)))]
#[sabi(missing_field(panic))]
pub struct NodePlugin {
    #[sabi(last_prefix_field)]
    pub list_factories: extern "C" fn() -> RVec<FfiNodeFactoryBox>,
}
```

Hosts load it via `NodePluginRef::load_from_file(path)`. abi_stable
validates layout, abi_stable version, and prefix-type compatibility
at that call.

### Factory trait

```rust
#[sabi_trait]
pub trait FfiNodeFactory: Send + Sync + 'static {
    fn node_type(&self) -> RString;
    fn create(&self, params: RString) -> RResult<FfiNodeBox, RString>;
}
```

`params` is the manifest's `params` object serialized as a JSON string
(not msgpack — params are small, configuration-shaped, and
human-debuggable; the wire-format split is intentional). The factory
returns a node instance owned via `RBox`.

### Node trait

```rust
#[sabi_trait]
pub trait FfiNode: Send + Sync + 'static {
    fn node_type(&self) -> RString;

    #[sabi(last_prefix_field)]                                            // ABI v0.1 cut
    fn process(&self, input: RVec<u8>) -> FfiFuture<RResult<RVec<u8>, RString>>;

    fn process_multi(&self, input: RVec<u8>)                              // added 0.2.0
        -> FfiFuture<RResult<RVec<RVec<u8>>, RString>> { /* default */ }

    fn initialize(&self, session_id: RString, node_id: RString)           // added 0.2.0
        -> FfiFuture<RResult<(), RString>> { /* default: no-op */ }
}
```

Three methods, three semantics:

- **`process`** — single-input, single-output. The original surface.
- **`process_multi`** — single-input, N-outputs. Required by nodes
  whose `process_streaming` callback fires multiple times per input
  (e.g. Silero VAD emitting both a speech-state JSON event and the
  audio passthrough). Default impl wraps `process` as a 1-element
  vec, so older plugins keep working but are limited to single-output.
- **`initialize`** — one-time per-session init hook for lazy-load
  plugins (e.g. llama-cpp spawning a multi-GB GGUF loader thread).
  Default is a no-op; plugins that do all their setup eagerly in
  `FfiNodeFactory::create` don't override it.

## 2. What is **not** in the ABI

Anything that would force the plugin to link `remotemedia-core` or
match the host's exact dep graph is kept out:

| Not in ABI | Why | What plugins do instead |
|---|---|---|
| `RuntimeData` (native enum) | dragging it across `dlopen` would require both sides to agree on `remotemedia-types` byte-for-byte | serialize/deserialize via `rmp-serde::to_vec_named` ↔ `from_slice` at every FFI hop (see [`crates/plugin-sdk/src/adapter.rs:125,146`](../crates/plugin-sdk/src/adapter.rs)) |
| `ControlBus` / `SessionContext` | bus is host-owned, threads through async tasks the plugin can't see | plugins use a polled-state pattern from inside their `FfiFuture` if they need host context |
| `StreamingNodeFactory`, `MediaCapabilities`, `CapabilityBehavior` | live in `remotemedia-traits`, would couple ABI to trait shape | the plugin-sdk crate ships an `adapter::wrap_ffi_factory` that bridges in-process; loadable plugins declare capabilities only via static manifest metadata |
| `tokio::Runtime`, `tokio::Handle` | runtime versions can differ | `async_ffi::FfiFuture` carries its own poll-state; plugin and host can each have their own runtime |
| Progress events from inside `initialize()` | no FFI for the host's `emit_progress` | wrap heavy init in `WarmSessionPool::prewarm` on the host side, which fires its own progress before delegating |
| Pacing domains | host-only scheduler concern | manifest pacing hints route only to in-tree nodes |
| Anything in `remotemedia-core` | by construction — that's the whole point | depend on `remotemedia-plugin-sdk` and `remotemedia-traits` / `remotemedia-types` instead |

If you find yourself wanting to add a host-side concept to the ABI,
the first question is "can it be expressed as opaque bytes the plugin
treats as a token?" Usually the answer is yes.

## 3. Wire format

All `RVec<u8>` payloads across the boundary are **msgpack** encoded
with `rmp-serde::to_vec_named`. This applies to:

- `FfiNode::process` input and output
- `FfiNode::process_multi` input and each `RVec<u8>` inside the output

The encoder is `to_vec_named` (string field keys, not integer indices)
so the wire stays self-describing — a host can decode payloads it
didn't compile against and at worst drops unknown fields, never
mis-decodes them.

`params` (in `FfiNodeFactory::create`) is the exception: it's a JSON
string (`RString`). Params are small, human-readable, and have already
gone through serde once when the manifest was parsed. JSON is cheaper
to debug than msgpack here.

## 4. Forward compatibility

Two mechanisms work together:

### 4.1 `#[sabi(last_prefix_field)]`

Marks the *most-recently-stable* field/method below which everything
must have a default impl. Two markers exist today:

- `NodePlugin::list_factories` — the original v0.1 surface
- `FfiNode::process` — the original v0.1 surface

Anything added after these markers (in declaration order) must carry a
default. Plugins built against the old surface keep loading; the host
silently uses the default whenever the plugin doesn't override.

### 4.2 Default impls

Every method added after the prefix cut **must** have a default that
preserves the v0.1 behavior. Examples:

```rust
// process_multi default: delegate to process, wrap as 1-element vec.
// Plugins that only know about process stay correct, just lossy when
// the underlying node was actually multi-output.
fn process_multi(&self, input: RVec<u8>) -> FfiFuture<RResult<RVec<RVec<u8>>, RString>> {
    let fut = self.process(input);
    async move {
        match fut.await {
            ROk(out) => ROk(RVec::from(vec![out])),
            RErr(e) => RErr(e),
        }
    }.into_ffi()
}

// initialize default: no-op.
fn initialize(&self, _: RString, _: RString) -> FfiFuture<RResult<(), RString>> {
    async { ROk(()) }.into_ffi()
}
```

### 4.3 What you can't do without a MAJOR bump

- Change the signature of a method that's *at or above* a
  `last_prefix_field` cut.
- Reorder fields of `NodePlugin` above the cut.
- Change a sabi-trait's required (non-defaulted) method count above the
  cut.
- Remove an `abi_stable` derive from any type that crosses the
  boundary.
- Switch the wire format (e.g. msgpack → CBOR) without simultaneously
  bumping a discriminator on the payload — but at that point you're in
  MAJOR territory anyway.

## 5. Versioning policy

`loadable-node-abi`'s crate version is the source of truth. Bump it
with this rule:

| Change | Bump | Examples |
|---|---|---|
| Add a sabi-trait method WITH a default impl, below the prefix cut | MINOR (0.x → 0.x+1) | `process_multi`, `initialize` |
| Add a prefix-type field WITH a default value (`#[sabi(missing_field(...))]`) | MINOR | a hypothetical `plugin_metadata: extern "C" fn() -> RString` |
| Change a method signature, remove a method, move the prefix cut | MAJOR (0.x → 0.x+1.0 pre-1.0, or x → x+1 post-1.0) | nothing yet |
| Add an example, fix a doc comment, fix the rmp-serde call site | PATCH | doc-only changes |

Currently we're pre-1.0, so MINOR is the practical unit of breakage —
`abi_stable` will reject a plugin built against a different MINOR
even though cargo's semver normally allows `^0.x` resolution. That's
intentional: pre-1.0 is the right shape while we're still discovering
what the contract should be.

**Change history** lives in the crate's
[`Cargo.toml`](../crates/loadable-node-abi/Cargo.toml) as comments
above the `version =` line. Update it on every bump:

```toml
# History:
#   0.1.0  initial release
#   0.2.0  + FfiNode::process_multi, + FfiNode::initialize
#                 (commit f4ba6b4e — multi-output FFI path)
```

After bumping either `loadable-node-abi` or `remotemedia-plugin-sdk`,
run:

```bash
python3 scripts/sync-loadable-abi-versions.py
```

That refreshes the reusable plugin release workflow defaults and the
docs examples. CI runs the same script with `--check` so stale workflow
expectations cannot merge silently.

## 6. Plugin author checklist

When authoring a Path 3 hand-rolled plugin (or a dual-emit `#[node]`
plugin built with the `loadable-export` feature), every plugin repo
must:

- [ ] Depend only on `remotemedia-plugin-sdk`, **not** on
      `remotemedia-core`. Linking core drags in the host's whole dep
      graph — including version-pinned crates like `ort` that conflict
      across plugins. See memory note [[loadable_plugin_conflict_isolation]].
- [ ] Depend on an intentional `remotemedia-plugin-sdk` line. Prefer
      crates.io once the SDK line is published:

      ```toml
      remotemedia-plugin-sdk = "0.5"
      ```

      If the plugin must consume an unpublished SDK snapshot, pin by
      `rev = "<sha>"`, not by `branch = "main"`. Branch pins resolve at
      build time against whatever the remote HEAD happens to be —
      every plugin release becomes a roll of the dice. Rev pins are
      deterministic. Example:

      ```toml
      remotemedia-plugin-sdk = { git = "https://github.com/RemoteMedia-SDK/remotemedia-sdk.git", rev = "5dd1bc9980eafa51bac188279bb0d29a2bdaba34" }
      ```

- [ ] Declare its own `[workspace]` in `Cargo.toml` to break out of
      any parent workspace's `[patch]` table. The
      [`examples/pyannote-rs-conflict/`](../examples/pyannote-rs-conflict)
      template documents the full pattern.
- [ ] Use `crate-type = ["cdylib", "rlib"]` and gate the abi_stable
      root-module export behind a feature (canonically named
      `plugin-export`, default-on). Hosts that want to link the plugin
      as an rlib (in-process registration) opt out of the feature to
      avoid symbol collision.
- [ ] Use msgpack (`rmp-serde::to_vec_named` / `from_slice`) for
      every `RVec<u8>` payload. JSON-encoded payloads from older
      plugins will decode as garbage on a current host.
- [ ] Provide a CI matrix build (see
      [`docs/PLUGIN_PUBLISHING.md`](PLUGIN_PUBLISHING.md)) that
      produces .so / .dll / .dylib artifacts under predictable names
      so hosts can resolve from `~/.config/remotemedia/nodes/` or
      `$REMOTEMEDIA_NODES_DIR`.
- [ ] Keep the reusable release workflow's ABI gate enabled. It runs
      `cargo metadata` before building and fails the release if the
      plugin resolves an older `remotemedia-plugin-sdk` or
      `loadable-node-abi` than the SDK workflow expects. When this
      fails, bump the plugin dependency (and lockfile, if committed)
      before cutting a new release.

## 7. Host author rules

When wiring a loadable plugin into a host binary:

- **Bubble `AbiInstability` panics up as a structured error.**
  abi_stable's panic on layout mismatch is the only signal you get
  that someone shipped a binary built against the wrong ABI snapshot.
  The host's plugin-loader wraps `NodePluginRef::load_from_file` in a
  catch and translates it to a `Manifest error: loadable plugin "..."
  failed to load:` log line. Don't swallow it.
- **Missing trait methods are NOT errors.** When a plugin doesn't
  override `process_multi` or `initialize`, the host gets the default
  impl. This is by design — older plugins are still valid against a
  newer host. Don't add runtime checks like "does this plugin
  implement initialize?" — just call it; the default returns
  `ROk(())`.
- **Don't reach into the plugin's own deps.** If a plugin links a
  different version of `ort`, that's its problem to manage in its own
  `[workspace]`. The host MUST NOT assume the plugin's transitive deps
  match its own.
- **One node-type per plugin is preferred, multiple are allowed.**
  `list_factories` returns a vec for a reason. But if two plugins
  expose the same `node_type` string, the registry will reject the
  second load. Plugins should namespace their node types
  (`SileroVADNode`, not `VADNode`).

## 8. Change history

| Version | Date | Change | SDK commit |
|---|---|---|---|
| 0.1.0 | initial | `NodePlugin`, `FfiNodeFactory`, `FfiNode::process` | — |
| 0.2.0 | 2026-05 | Added `FfiNode::process_multi` and `FfiNode::initialize` (both with default impls). Documented msgpack wire format. | `f4ba6b4e` |

When you bump:

1. Update the table above.
2. Update the history block in
   [`crates/loadable-node-abi/Cargo.toml`](../crates/loadable-node-abi/Cargo.toml).
3. Bump every official plugin's `rev = ` pin and cut a new patch
   release per plugin so the published .so artifacts are built against
   the new ABI.

## 9. Compatibility matrix

Plugins maintained under the `RemoteMedia-SDK` GitHub org, and what
they're currently pinned to:

| Plugin | Latest release | SDK rev | `loadable-node-abi` | Notes |
|---|---|---|---|---|
| [`silero-vad`](https://github.com/RemoteMedia-SDK/silero-vad) | `v0.3.0` | `5dd1bc9980ea` | `0.2.0` | bumped from branch-pin to rev-pin in this cycle; needs re-release |
| [`audio2face`](https://github.com/RemoteMedia-SDK/audio2face) | (in-tree only) | `5dd1bc9980ea` | `0.2.0` | mid-rlib-migration; needs re-release once landed |
| [`llama-cpp`](https://github.com/RemoteMedia-SDK/llama-cpp) | (in-tree only) | `5dd1bc9980ea` | `0.2.0` | already had rlib refactor; needs re-release for rev-pin |
| [`live2d-render`](https://github.com/RemoteMedia-SDK/live2d-render) | (in-tree only) | `5dd1bc9980ea` | `0.2.0` | mid-rlib-migration; needs re-release once landed |
| [`pyannote-rs`](https://github.com/RemoteMedia-SDK/pyannote-rs) | — | `1e2083be` | `0.1.0` | intentionally rev-pinned older for the [`ort` matrix](../examples/pyannote-rs-conflict) — see memory [[pyannote_ort_matrix]] |
| [`lfm25-audio-onnx`](https://github.com/RemoteMedia-SDK/lfm25-audio-onnx) | `v0.2.0` | (verify) | (verify) | primary audio backend for [s2s-tool-orchestrator](S2S_TOOL_ORCHESTRATION.md); see memory [[lfm25_audio_onnx_plugin]] |

Cells marked `(verify)` are out-of-date in this draft. Update them
when you re-cut the corresponding plugin.

## 10. Related docs

- [`docs/CUSTOM_NODE_REGISTRATION.md`](CUSTOM_NODE_REGISTRATION.md) —
  author-side guide to the four paths to ship a node (in-tree,
  dual-emit, hand-rolled cdylib, Python cdylib). Covers the *how*;
  this doc covers the *what*.
- [`docs/PLUGIN_PUBLISHING.md`](PLUGIN_PUBLISHING.md) — CI workflow
  for cutting plugin releases.
- [`docs/plans/2026-05-14-skinny-traits-dual-emit-plugins.md`](plans/2026-05-14-skinny-traits-dual-emit-plugins.md) —
  historical design plan that produced the `remotemedia-traits` /
  `remotemedia-types` / `remotemedia-plugin-sdk` split.
- [`crates/loadable-node-abi/src/lib.rs`](../crates/loadable-node-abi/src/lib.rs) —
  the actual source of truth. If this doc and that file disagree, the
  source wins; please file a doc fix.
