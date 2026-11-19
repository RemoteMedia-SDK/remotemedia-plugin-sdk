## ADDED Requirements

### Requirement: Enforceable workload isolation profiles

The Plugin SDK SHALL provide a workload provider contract with a versioned capability result and explicit launch policy for install or build and serve. The provider SHALL enforce a per-workload OS identity or equivalent boundary, filesystem and IPC restrictions, allowed environments and handles, default-deny network policy, device access, and finite resource and lifetime limits. It SHALL report an unsupported requirement before starting workload code. Process supervision and installer configuration alone SHALL confer no isolation capability.

#### Scenario: A provider lacks a required control

- **WHEN** a protected workload asks for network, device, or resource controls the selected profile cannot enforce
- **THEN** the provider rejects the launch before user code or a build backend runs and reports the missing control

#### Scenario: An installer and app run in protected contexts

- **WHEN** each phase attempts to access a synthetic host credential, another run's files, shared writable caches, or a disallowed network service
- **THEN** the OS boundary denies each access and the trusted worker remains available

### Requirement: Locked managed workload installation

Protected installation SHALL use a trusted interpreter and a complete target-compatible, hash-verified wheel set offline. The installer SHALL use explicit arguments, configuration, environment, and resource handles. It SHALL consume only its verified inputs and SHALL fail on missing, extra, or modified required artifacts. Any approved source build SHALL run in a separate disposable context using policy-approved inputs, with only verified outputs eligible for use. Shared environments SHALL be immutable to workloads and keyed by artifact or lock identity, target, interpreter and ABI, and applicable policy.

#### Scenario: A verified wheel disappears during provisioning

- **WHEN** a wheel is unavailable or differs from its expected digest at installation
- **THEN** provisioning fails without resolving a substitute or publishing an environment for later opens

#### Scenario: Two runs use the same dependency lock

- **WHEN** two workloads select the same verified target environment
- **THEN** any reused environment is read-only to both, and each run has isolated writable state

### Requirement: Provider-owned termination and resource limits

The provider SHALL enforce configured CPU, memory, process, scratch storage, and wall-time limits on the workload and its descendants, and expose confirmed termination to its caller. GPU profiles SHALL expose only assigned hardware and enforce device isolation and configured concurrency through provider and scheduler controls. A profile with unenforceable per-run GPU memory limits SHALL require exclusive device access or an enforceable hardware partition.

#### Scenario: Stop or owner death interrupts a workload

- **WHEN** a run is cancelled or its trusted owner exits while workload descendants are active
- **THEN** the isolation unit is reaped and its route and writable state are removed, or the provider reports unresolved cleanup so resources remain quarantined
