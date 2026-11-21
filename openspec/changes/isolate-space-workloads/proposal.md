# Isolate Space workloads in the Plugin SDK

## Why

Managed environments can currently install repository-directed dependencies. The existing OS-specific process supervision provides process ownership and teardown. Protected workloads also need a provider that enforces their filesystem, process, device, and network boundaries during both installation and execution.

## What Changes

- Add reusable contracts for locked Python installation, explicit child launch inputs, and enforceable workload isolation profiles.
- Keep existing trusted-plugin APIs and lifecycle semantics. Require the protected path to fail closed if it cannot enforce a requested policy.

## Impact

- Adds `managed-workload-isolation` in `crates/py-env-manager` and the Plugin SDK worker support. SDK Base consumes the contract. See the [shared design](../../../../huggingface-spaces-runner/openspec/changes/isolate-space-workloads/design.md).
- This extends the managed Python environment surface and composes with `add-darwin-trusted-provisioning`. Existing native plugin ABI and trusted provisioning remain usable.
