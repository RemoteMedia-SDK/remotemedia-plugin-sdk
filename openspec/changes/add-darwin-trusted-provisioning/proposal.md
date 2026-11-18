## Why

The [macOS MLX Space runner](../../../../huggingface-spaces-runner/openspec/changes/add-macos-mlx-space-runner/proposal.md) needs managed Python environments on Darwin. The existing manager rejects all installer and interpreter commands on that platform.

## What Changes

- Allow a trusted host process to opt into captured Darwin provisioning commands. Keep leases and cache transaction ownership until process group termination is verified.
- Reuse captured-command lifetime handling for the new backend. Continue to require the existing Linux supervisor and Windows job paths on those platforms.

## Capabilities

### New Capabilities

- `darwin-trusted-provisioning` covers opt-in lifetime ownership for trusted managed Python operations on Darwin.

### Modified Capabilities

None.

## Impact

- The application explicitly enables this mode only for its pinned macOS Space profile. `remotemedia-sdk-base` continues calling the existing source-node environment manager interface.
- Process groups track the non-detaching descendants of a trusted command. Use the existing stronger platform contracts for general untrusted provisioning.
