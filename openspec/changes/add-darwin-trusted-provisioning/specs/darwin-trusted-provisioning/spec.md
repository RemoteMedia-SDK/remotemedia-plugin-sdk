## ADDED Requirements

### Requirement: Trusted Darwin command ownership

The managed Python environment runtime SHALL require an explicit trusted host opt-in to run provisioning commands on Darwin. It SHALL retain related cache resources until the command and its owned descendants have terminated or quarantine the resources when termination cannot be confirmed.

#### Scenario: Darwin command owner is cancelled

- **WHEN** a trusted Darwin provisioning command with group-owned children is cancelled
- **THEN** the capture owner terminates and joins the group before releasing retained cache resources
- **AND** a timeout keeps the retained resources unavailable for reuse

#### Scenario: Darwin has no provisioning opt-in

- **WHEN** the host has not enabled trusted Darwin provisioning
- **THEN** the manager rejects the provisioning command before launching it
