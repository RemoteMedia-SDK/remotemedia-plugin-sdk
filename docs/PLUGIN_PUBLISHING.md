# Publishing a RemoteMedia SDK plugin

This guide is for plugin authors who want their cdylib (`.so` / `.dylib` /
`.dll`) plugin to be consumable via the manifest's `plugins` field with
nothing more than:

```json
{ "plugins": ["my-plugin@v0.1.0"] }
```

You publish the matrix-built binaries + a `release-manifest.json` to a
GitHub Release. The SDK's resolver fetches the manifest, picks the
entry for the consumer's platform, downloads the binary, verifies its
SHA256, and dlopens it. See
[`crates/core/src/loadable/resolver.rs`](../crates/core/src/loadable/resolver.rs)
for the full dispatch state machine.

The SDK ships a reusable GitHub Actions workflow that handles the
entire build → SHA256 → release-manifest → upload flow.

## TL;DR — three files in your plugin repo

1. **`plugin.toml`** at the repo root, with at least:

   ```toml
   [plugin]
   name        = "my-plugin"
   version     = "0.1.0"
   language    = "rust"

   [rust]
   node_types    = ["MyNode"]
   asset_pattern = "lib{name}-{arch}-{os}.{ext}"
   ```

2. **`Cargo.toml`** — your plugin crate as usual, exporting the
   `plugin_export!()` factory (Path 2, 3, or 4).

3. **`.github/workflows/release.yml`** — wires the reusable workflow:

   ```yaml
   name: Release
   on:
     push:
       tags: ['v*']
     workflow_dispatch:
       inputs:
         tag:
           description: 'Tag to release (e.g. v0.2.0)'
           required: true

   jobs:
     release:
       uses: matbeedotcom/remotemedia-sdk/.github/workflows/build-plugin.yml@main
       with:
         crate-name:   my-plugin-cdylib   # cargo `name = ...` field
         display-name: my-plugin          # plugin.toml [plugin].name
         tag:          ${{ inputs.tag }}  # optional — defaults to ref_name
       permissions:
         contents: write   # required so the inner workflow can upload
   ```

Then `git tag v0.1.0 && git push origin v0.1.0` and the workflow runs.

## What the workflow does

Matrix-builds your cdylib for 4 platforms in parallel:

| Platform key       | Runner             | Rust target                | Asset file                     |
|--------------------|--------------------|----------------------------|--------------------------------|
| `x86_64-linux`     | `ubuntu-latest`    | `x86_64-unknown-linux-gnu` | `lib{name}-x86_64-linux.so`    |
| `aarch64-linux`    | `ubuntu-24.04-arm` | `aarch64-unknown-linux-gnu`| `lib{name}-aarch64-linux.so`   |
| `aarch64-darwin`   | `macos-latest`     | `aarch64-apple-darwin`     | `lib{name}-aarch64-darwin.dylib`|
| `x86_64-windows`   | `windows-latest`   | `x86_64-pc-windows-msvc`   | `lib{name}-x86_64-windows.dll` |

> **Intel macOS (`x86_64-darwin`) is intentionally NOT in the matrix.** The
> free-tier `macos-13` runner queue often starves indefinitely (jobs sit
> `queued` for >30 minutes, blocking the release upload). Apple Silicon
> consumers are well-covered by `aarch64-darwin`. Intel-Mac consumers hit
> a precise `PlatformNotPublished` from the resolver and can build from
> source locally (`cargo build --release` against this repo).

After each build it:

1. Resolves the plugin's Cargo dependency graph and fails fast if
   `remotemedia-plugin-sdk` or `loadable-node-abi` do not match the
   ABI versions expected by this SDK workflow.
2. Renames the cargo output (`lib{crate_name_underscored}.{ext}`) to
   `lib{display-name}-{platform}.{ext}` so the resolver's
   `current_platform()` lookup matches.
3. Computes SHA256 of the renamed binary.
4. Uploads as a per-platform artifact.

The downstream `release` job:

1. Downloads every artifact.
2. Assembles `release-manifest.json`:
   ```json
   {
     "name": "my-plugin",
     "version": "v0.1.0",
     "loadable_node_abi": "0.3.0",
     "remotemedia_plugin_sdk": "0.5.0",
     "platforms": {
       "x86_64-linux":  { "file": "libmy-plugin-x86_64-linux.so",   "sha256": "..." },
       "aarch64-linux": { "file": "libmy-plugin-aarch64-linux.so",  "sha256": "..." },
       ...
     }
   }
   ```
3. Uploads every binary + the manifest to the GitHub Release at the
   triggering tag (creating the release if it doesn't exist).

## Inputs reference

The reusable workflow (`build-plugin.yml`) accepts:

| Input                        | Required | Default               | Purpose |
|------------------------------|----------|-----------------------|---------|
| `crate-name`                 | yes      | —                     | Your `Cargo.toml` `name = ...` field. Determines the build-output filename. Hyphens get converted to underscores (cargo convention). |
| `display-name`               | yes      | —                     | Public name. Matches `[plugin].name` in `plugin.toml` and is used in the release asset filenames + release-manifest. Typically equals the GitHub repo name for canonical-org shorthand (`my-plugin@v0.1.0` → `RemoteMedia-SDK/my-plugin`). |
| `tag`                        | no       | `${{ github.ref_name }}` | Git tag to release against. Override when triggering via `workflow_dispatch`. |
| `cargo-features`             | no       | empty                 | Extra `--features <list>` to pass to `cargo build`. Most plugins don't need this — feature wiring lives in `Cargo.toml`. |
| `cargo-no-default-features`  | no       | `false`               | Pass `--no-default-features` to cargo. |
| `enforce-loadable-abi`       | no       | `true`                | Fails the release when the plugin resolves a `remotemedia-plugin-sdk` or `loadable-node-abi` version that does not match this SDK workflow. Keep enabled for published loadable plugins. |
| `expected-remotemedia-plugin-sdk` | no   | workflow-defined      | Expected `remotemedia-plugin-sdk` crate version. Override only when intentionally building against an older host line. |
| `expected-loadable-node-abi` | no       | workflow-defined      | Expected `loadable-node-abi` crate version. Override only when intentionally building against an older host line. |

## Canonical-org vs. third-party

The reusable workflow has no special knowledge of the
`RemoteMedia-SDK` GitHub org. A plugin author outside that org can use
it the same way — just reference the repo by `{owner}/{repo}` in
their consumers' manifest entries:

```json
{ "plugins": ["myorg/my-plugin@v0.1.0"] }
```

The canonical-org shorthand `"my-plugin@v0.1.0"` (no slash) is only
for plugins published at `github.com/RemoteMedia-SDK/{name}`.

## Pinning the workflow version

The example above uses `@main`. For reproducible builds, pin to a
specific SHA or tag of the SDK monorepo:

```yaml
uses: matbeedotcom/remotemedia-sdk/.github/workflows/build-plugin.yml@v1.2.3
```

`@main` is fine while the workflow is stabilizing (semver-major changes
will be called out in the SDK changelog).

## Automating ABI bumps

The reusable workflow's expected ABI versions are generated from the
SDK crates:

```bash
python3 scripts/sync-loadable-abi-versions.py
```

Run this after bumping `crates/plugin-sdk/Cargo.toml` or
`crates/loadable-node-abi/Cargo.toml`. CI runs the same command with
`--check`, so a PR that changes the ABI crates without refreshing the
workflow/docs fails before merge.

Plugin repos still need their own dependency bump when the SDK ABI
line changes. The release workflow enforces that at publish time by
checking the plugin's resolved Cargo graph before uploading artifacts.

## What gets skipped

- **aarch64-windows** — niche; add it back if there's user demand. The
  matrix is trivial to extend (one more block in `build-plugin.yml`).
- **Cross-compiling** for unusual targets (musl, ARMv7, etc.) — the
  workflow uses native runners per platform. For exotic targets, fork
  the workflow and add `cross` or `cargo-zigbuild`.
- **Signing** (Apple notarization, Authenticode) — not yet. Plugins
  ship unsigned; SHA256 in the release-manifest provides integrity
  verification.

## Troubleshooting

**Build artifact not found** — the workflow looks for
`target/{rust-target}/release/lib{crate_name_underscored}.{ext}`. If
your crate uses a different `[lib]` name than the package name, set
`crate-name` to match the `[lib].name` field (not `[package].name`).

**`release.yml` failed at upload** — make sure your caller workflow has
`permissions: contents: write` (the inner workflow inherits this from
its caller).

**Empty release-manifest.json** — at least one platform build must
succeed for the manifest to be generated. The `release` job depends on
the `build` job; if all platform builds fail, the release step never
runs.

**Resolver still hits 404** — confirm the release-manifest.json is at
the release's _assets_ (not the source archive). Browse to
`https://github.com/{owner}/{repo}/releases/tag/{tag}` and check the
"Assets" section.

**Release fails at "Validate RemoteMedia loadable ABI"** — the plugin
is resolving an SDK/ABI version that will not load in hosts built
against this workflow's SDK line. Bump the plugin's
`remotemedia-plugin-sdk` dependency to the expected version, refresh
`Cargo.lock` if the plugin commits one, and rerun the release. Do not
fix this by disabling `enforce-loadable-abi` for public releases unless
the release is intentionally targeting an older host line.

## Reference

- [Reusable workflow source](../.github/workflows/build-plugin.yml)
- [Resolver source](../crates/core/src/loadable/resolver.rs) — what
  consumes the published artifacts
- [`plugin.toml` schema](../crates/core/src/loadable/plugin_toml.rs)
