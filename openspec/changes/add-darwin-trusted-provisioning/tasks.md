## 1. Provide and validate Darwin provisioning

- [x] 1.1 Implement trusted, opt-in command-group capture and preserve process and resource ownership across exit and cancellation.
- [x] 1.2 Run Darwin process-owner and environment-manager tests on Apple Silicon and verify the existing Linux and Windows test paths.

The M1 Pro passed all three focused command-owner tests and the explicit opt-in test. Its environment-manager suite passed 42 active tests before the opt-in regression was added. Linux passed 42 active environment-manager tests and Windows passed 45 through `cargo-dev.ps1`. Each manager suite excluded six installer fixtures that require separate setup.
