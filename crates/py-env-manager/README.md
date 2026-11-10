# Python environment process ownership

Environment creation, dependency installation, interpreter checks, and the
post-install diagnostic probe run with contained process ownership. Cancelling
their future joins command shutdown before releasing cache protection. The owner
drains stdout and stderr concurrently and closes remaining descendants even when
the main process exits successfully. Shutdown may block the cancelling thread;
these operations belong on the control plane.

Windows uses a hidden private job, assigned before the command's primary thread
resumes. Linux requires explicitly configured, trusted, matching supervisor and
unshare executables:

```json
{
  "process_namespace": {
    "supervisor": "/opt/remotemedia/remotemedia-worker-supervisor",
    "unshare": "/usr/bin/unshare"
  }
}
```

Set `PythonEnvConfig.process_namespace` directly, or provide the inner object as
JSON in `REMOTEMEDIA_PYTHON_PROCESS_NAMESPACE`. Explicit config takes precedence.
Invalid environment JSON fails configuration. The matching supervisor binary is
built from `remotemedia-shared-memory` with `linux-worker-supervisor`; namespace
creation must be permitted on the host. Missing or unavailable containment fails
explicitly. Other platforms currently reject these contained launches.

Mapped factories configured with `with_linux_namespace` use those paths for
provisioning too. A Linux worker using another placement still needs explicit
provisioning containment. Selecting provisioning containment does not change the
worker's placement. Mapped-wheel verification uses this same containment and
retains its environment lease until the probe and descendants stop. Automatic
packaging/selection of the supervisor remains follow-up integration work.

The current Linux supervisor uses the `supervise-v2`/`init-v2` startup protocol.
Rebuild the matching helper when updating the host: version 2 reports the actual
namespace-local worker PID for probe identity checks. The guardian PID remains a
separate host identity. Neither numeric PID is a substitute for owned process
handles/pidfds when signalling or proving process death.

## Backend migration

`EnvBackend::ensure_python`, `create_venv`, and `install_deps` now accept a
`&ProvisioningContext`. Custom implementations must use `context.command(program)`
and its `command_mut()` and `output().await` methods, or provide equivalent joined
containment. Configure arguments, environment and working directory on this
builder from the outset; reconstructing an existing command can lose `env_clear`.
Do not replace the supervisor executable or its leading arguments.

The context retains transaction and environment locks. A command that cannot
confirm termination quarantines its retained owners until host exit. A custom
backend retaining a context after returning prevents success metadata publication
and lock release. Detached tasks do not inherit the context: retain it explicitly
and join their complete lifetime. Implementations that ignore this contract are
not cancellation safe merely because they implement the trait.

`install_additional_deps` requires an idle managed environment. It rejects leased
environments and invalidates metadata before mutation; failed or cancelled
installation therefore requires rebuilding on the next ensure operation. Legacy
metadata-only consumers still need migration to leases to be protected.

## Host-crash cache protection

Each provisioning operation and leased environment owns a persistent record in
`<cache>/.locks/<key>.owners/`. The record stays locked while its owner lives and
is removed on normal final release. Contained command owners retain it through
confirmed shutdown; unknown termination retains it. Abrupt host death leaves an
unowned record, because host death does not prove that descendants have stopped.

Updated cache managers reject reuse, replacement and additional installation for
such an entry, and eviction skips every entry with retained records. An unreadable
record store also fails closed. Normal concurrent read leases and final-owner
release continue to work. These are cooperating-manager guarantees: older cache
managers and external filesystem tools do not enforce the record protocol.

Automatic recovery of unconfirmed entries is not implemented yet. They remain
quarantined even after descendants later exit; free OS locks, timestamps and
numeric PIDs are insufficient recovery evidence. Do not remove records merely
because a host disappeared. This protects access during a crash but does not
complete bounded crash recovery or the remaining legacy-launcher migration.

## Validation

The library's deterministic cancellation regression is enabled normally on
Windows. On Linux enable `test-linux-namespace` and set
`RM_TEST_PROVISION_SUPERVISOR` to the separately built matching supervisor.
Real offline system/uv tests in `tests/provisioning.rs` require a Python with venv
and pip, and uv for the uv case; run them with `--include-ignored`.

These checks cover cancellation of a controlled installer, cache replacement,
retained context locks, and real offline provisioning. Probe fault tests use a
bounded fake interpreter with descendants: build `python-probe-fixture` with the
`test-probe` feature and set `RM_TEST_PROBE_EXE` to it. Enable `test-probe` for the
environment library tests and `test-mapped-probe` for the plugin library tests;
Linux also needs `RM_TEST_PROVISION_SUPERVISOR`. These exercise both timeout and
task abort, successful parent exit with surviving descendants, and PID mismatch.
The offline PEP 517 installer matrix additionally needs `bundled-uv`, an absolute
`RM_TEST_INSTALLER_PYTHON` selecting Python 3.12, pip and uv. Run the ignored
`cache_tests::installers` cases explicitly; they cover requirements, metadata and
wheel hooks during both creation and additional installation. The Linux
host-crash regression deliberately pauses its guardian and proves cache access
is refused while the installer still writes. Cross-process crash quarantine and
graceful release run on Windows and Linux. These checks do not establish every
installer phase, automatic crash recovery, or the full pipeline acceptance scope.

The strengthened Windows installer fixture retains original process handles and
requires them to be signaled immediately when cancellation returns. Earlier pip
cases failed while Job Object active accounting had reached zero. The owner now
seals the job against new assignments, verifies that retained original identities
cover its lifetime process count, and waits on every handle before releasing
resources. The pip/uv matrix passes this stronger check. After handle-confirmed
exit, the fixture separately allows bounded asynchronous file-lock cleanup.

Windows observation polls every 5 ms to collect identities, but polling does not
prove completeness. A missing identity, more than 4096 distinct lifetime members,
failed query or unsignaled handle causes shutdown failure and quarantine. Rapid
short-lived helpers can therefore cause explicit failure rather than unsafe
success. Guaranteed registration, large-tree/stress behavior and automatic
quarantine recovery remain follow-up work. Identity comparison uses
`CompareObjectHandles` (Windows 10 / Server 2016 or later).
