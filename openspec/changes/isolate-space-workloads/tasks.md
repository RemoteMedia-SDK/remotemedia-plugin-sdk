## 1. Provider and managed installation

- [ ] 1.1 Define the provider profile, bounded lifecycle operations, and supported-policy reporting at the SDK Base boundary. Implement and document the first Linux provider and capability probing for CPU and CUDA separately.
- [ ] 1.2 Extend `crates/py-env-manager/src/uv_backend.rs`, `process.rs`, and cache handling with verified offline wheel installation, explicit child inputs, and immutable or run-private environment ownership. Include lock digest, target, interpreter and policy identity in cache selection.
- [x] 1.3 Provide a separate disposable build context for approved source distributions. Verify build outputs before publication and clean up build state on failure or cancellation.

## 2. Validation

- [ ] 2.1 Run lock and cache tests for wrong targets, altered bytes, missing wheels, concurrent reuse, and no ambient installer config or network access.
- [ ] 2.2 Exercise the real Linux provider in installation and serving with synthetic access-denial fixtures, limits, cancellation, and crash teardown. Verify device visibility and scheduling constraints on physical CUDA for the CUDA profile.
- [ ] 2.3 Run the existing managed Python and lifecycle tests on supported trusted paths, including trusted Darwin and Windows process ownership.
