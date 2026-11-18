## Darwin command ownership

Enable process-group ownership only when trusted host configuration sets `REMOTEMEDIA_PYTHON_DARWIN_TRUSTED_PROVISIONING=1`. Reject a Linux namespace in this mode. Spawn each command in its own group with null stdin and captured stdout and stderr. Observe leader termination with `waitid` without reaping it until the group has been signalled. After reaping, only probe group presence, since a group identifier may be reused. Join capture threads and release retained owners after confirmed termination. Quarantine retained owners when termination cannot be confirmed. The application owns cross-process recovery if the provisioning host itself is killed.

Reuse the shared capture mechanism independently of the Linux and Windows mapped-worker channel. The new backend applies to captured interpreter and installer commands. It assumes commands and descendants stay in their assigned process group.
