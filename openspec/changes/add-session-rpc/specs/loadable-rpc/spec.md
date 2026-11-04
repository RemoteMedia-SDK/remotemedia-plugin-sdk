## ADDED Requirements
### Requirement: Invocation identity
The SDK SHALL deliver host-supplied session and node identity independently for each media and RPC invocation.
#### Scenario: Concurrent sessions
- **WHEN** two sessions invoke the same adapter concurrently
- **THEN** each invocation observes its own session and node identity.

### Requirement: Dedicated typed replies
The SDK SHALL support discoverable ff, reply and stream RPC methods using the established control-bus protocol and a dedicated asynchronous reply sink.
#### Scenario: Streaming response
- **WHEN** a method emits several values and completes
- **THEN** values arrive in order followed by one end frame, without media output.
#### Scenario: Handler failure
- **WHEN** a handler fails before completing
- **THEN** the caller receives a correlated error frame.

### Requirement: Responsive control and lifetime
The host SHALL process opted-in RPC independently of media, bound concurrent work, and cancel invocation tasks on stream cancellation or shutdown.
#### Scenario: Media in progress
- **WHEN** media processing is waiting and a status RPC arrives
- **THEN** the RPC completes without waiting for media completion.
#### Scenario: Consumer disappears
- **WHEN** the reply sink closes or the session shuts down
- **THEN** reply delivery fails and associated asynchronous invocation work is released.

### Requirement: Explicit compatibility
New methods SHALL retain source-compatible defaults and the ABI version SHALL reflect its changed layout.
#### Scenario: Legacy implementation
- **WHEN** a node implements only the existing methods and is rebuilt
- **THEN** it still processes media and declares no dedicated RPC methods.

### Requirement: Network caller lifetime
The host SHALL bind network RPC cancellation to the publishing control attachment.
#### Scenario: Idle call outlives a disconnected client
- **WHEN** a client disconnects while its RPC is waiting and another reply subscriber remains
- **THEN** its invocation is cancelled without closing the session or cancelling another caller's invocation.
