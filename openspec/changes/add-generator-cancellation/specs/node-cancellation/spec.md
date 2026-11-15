## ADDED Requirements
### Requirement: Per-call cancellation signal
Every node call SHALL carry a cancellation token that the host fires, with a reason, before it drops a cancelled call. The token SHALL be observable from async code, from blocking threads and from callbacks, and SHALL fire at most once per call.

#### Scenario: Barge-in cancels an in-flight reply
- **WHEN** a `barge_in` envelope cancels a node's in-flight call and no `ProtectGuard` is held
- **THEN** the call's token reports cancelled with reason `BargeIn` before the call future is dropped

#### Scenario: Protected call is not cancelled
- **WHEN** a `barge_in` arrives while the node holds a `ProtectGuard`
- **THEN** the call's token does not fire and the call continues

#### Scenario: Superseded and closed calls
- **WHEN** a newer input supersedes a call on a latest-input-wins node, or the session closes during a call
- **THEN** the call's token fires with reason `Superseded` or `SessionClosed` respectively

#### Scenario: Transforms are unaffected
- **WHEN** a node that does not generate content ignores the token
- **THEN** it compiles and runs exactly as before

### Requirement: Cancellation disposition
Every cancellation SHALL carry a disposition stating whether the node retains or discards the partial output of the cancelled call in its own state, and whether input already received or queued for the node is retained for the next call or discarded. The disposition SHALL be taken from the canceller if it sets one, else from the manifest node's `on_cancel`, else from the node's `default_disposition`, else from the reason (`BargeIn` and `Superseded`: retain both; `Retracted`, `CallerCancelled`, `SessionClosed`, `Shutdown`, `Deadline`: discard both). Output already emitted downstream SHALL NOT be recalled under either disposition.

#### Scenario: VAD interrupts a speech-to-speech reply
- **WHEN** a VAD-triggered `barge_in` with no explicit disposition cancels a speech-to-speech generator while the user's audio is streaming in
- **THEN** generation stops, the audio queued for the node before and after the barge is delivered to the next call, and the cut-off reply remains in the node's conversation state marked interrupted

#### Scenario: Retracted speculation aborts
- **WHEN** a `CancelSpeculation` retraction cancels a call
- **THEN** the router discards the inputs queued for the node and the node rolls back the partial output

#### Scenario: Node chooses to discard stale input
- **WHEN** a barge-in with no explicit disposition cancels a TTS node whose `default_disposition` discards pending input
- **THEN** the queued text of the cancelled reply is not spoken by the next call

#### Scenario: Canceller overrides the node
- **WHEN** a `barge_in` envelope carries `{"pending_input":"discard"}` for a node whose default retains input
- **THEN** the queued inputs are discarded

#### Scenario: Manifest override
- **WHEN** a manifest node sets `on_cancel: { partial_output: discard }` and a barge without an explicit disposition cancels it
- **THEN** the node rolls back the partial output and retains pending input

### Requirement: Generators must declare and implement cancellation
A node that generates text, audio, video or images, including avatar, lip-sync and motion renderers, SHALL be declared a generator. Speech-to-text and voice-conversion nodes are not generators. A generator and SHALL implement `Cancellable`, stating its `CancelStrategy`. For Rust nodes the derive attribute, the generator factory and the plugin export macro SHALL reject a generator type that does not implement `Cancellable` at compile time.

#### Scenario: Rust generator without Cancellable
- **WHEN** a type marked `#[node(generates = "audio")]` does not implement `Cancellable`
- **THEN** compilation fails with an error naming the missing `AudioGenerator`/`Cancellable` bound

#### Scenario: Generator declaration is visible to the host
- **WHEN** the host reads a generator's schema or a plugin factory's `describe()` JSON
- **THEN** it contains `generates` with the produced modality and `cancel` with the declared strategy

### Requirement: Rust and Python parity
Every requirement of this capability SHALL apply to Rust and Python nodes alike, in every execution mode. Python SHALL provide the same types (`Cancellation`, `CancelReason`, `Disposition`, `CancelStrategy`, `CancelToken`) and the same generator classes as Rust, generated from one schema, and SHALL enforce the generator contract at type-check time, at class definition and at instantiation, for both the `MultiprocessNode` and `InProcessNode` bases.

#### Scenario: Python generator without cancel fails before any session
- **WHEN** a module defines a concrete `AudioGenerator` subclass of `MultiprocessNode` without `cancel` or without `cancel_strategy`
- **THEN** mypy in CI reports the missing member, and importing the module raises `TypeError` naming the class

#### Scenario: Python registration requires the generator base
- **WHEN** `@register_node(generates="text")` decorates a class that does not derive from `GeneratorNode`
- **THEN** the decorator raises at import with an error naming the class

#### Scenario: Legacy base cannot host a generator
- **WHEN** a generator is defined on the legacy `Node` base
- **THEN** the audit test fails and names the node, and the host refuses to admit it

#### Scenario: Python declaration disagrees with host registration
- **WHEN** a Python node's discovery frame declares `generates` differently from its `PythonNodeConfig` entry
- **THEN** the host refuses the session and names the node and both declarations

#### Scenario: Types stay in step
- **WHEN** a `CancelReason`, disposition field or `CancelStrategy` is added on one side only
- **THEN** the schema parity test fails

#### Scenario: Same disposition behavior in both languages
- **WHEN** the same interrupt is delivered to a Rust and a Python speech-to-speech generator
- **THEN** both stop generating, retain pending audio for the next call and retain the cut-off reply

### Requirement: Host admission of generators
The host SHALL refuse to register a node factory, and pipeline validation SHALL refuse a manifest node, that declares `generates` without a `cancel` strategy. A plugin built against an ABI before 0.11 SHALL be handled by the documented acceptance rule for that version, and if accepted SHALL be reported once as `generates: unknown`.

#### Scenario: Plugin declares generates without cancel
- **WHEN** a loadable plugin factory's `describe()` returns `{"generates":"text"}` with no `cancel`
- **THEN** registration fails with an error naming the node type and the missing declaration

#### Scenario: Old plugin binary
- **WHEN** a plugin built against ABI 0.10 is loaded
- **THEN** the host applies the documented 0.10 acceptance rule, and an accepted plugin is logged once as `generates: unknown`

### Requirement: Cancellation reaches every execution mode
A cancelled call SHALL fire the node's token and run its `on_cancel` with the reason and disposition in every execution mode that can host a generator: in-process Rust, loadable plugin, native worker, mapped Python, subprocess Python, in-process Python, `python_plugin_export` and `python-source-plugin`. Python generator base classes SHALL make the cancel hook abstract so a generator without one cannot be instantiated.

#### Scenario: Host cancels a loadable plugin call
- **WHEN** the host cancels a call to a plugin built against ABI 0.11
- **THEN** it calls `cancel_call` before dropping the future, and the plugin-side token fires with the host's reason and disposition and the node's `on_cancel` runs once

#### Scenario: Plugin call dropped without cancel_call
- **WHEN** a plugin call's future is dropped without a preceding `cancel_call`
- **THEN** the plugin-side token fires with reason `CallerCancelled` and the abort disposition

#### Scenario: Python generator without a cancel hook
- **WHEN** a Python class derived from `GeneratorNode` does not implement `cancel`
- **THEN** instantiating it raises `TypeError`

#### Scenario: Python worker receives a cancel
- **WHEN** the host drops a call to a Python generator in any Python execution mode
- **THEN** the worker delivers a cancel for that call id and the node's `cancel` runs

### Requirement: Generator cancellation conformance
Every generator SHALL pass the shared conformance check: after its call is cancelled it emits no further output for that call within the grace period, its cancel hook or token observation is recorded, and the next call's time to first output is within the configured slack of an uncancelled baseline. Unit-level passes SHALL be reported separately from real-model and real-plugin passes.

#### Scenario: Repeated aborted replies
- **WHEN** five calls to a generator are each cancelled after their first output and a sixth call runs to completion
- **THEN** each call reaches first output within the slack of the baseline and the sixth completes

#### Scenario: Interrupt keeps input for the next call
- **WHEN** the check cancels a generator with the interrupt disposition and sends input during and right after the cancel
- **THEN** the next call uses that input, and a stateful generator's next call sees the retained partial output

#### Scenario: Generator keeps producing after cancel
- **WHEN** a generator emits output for a cancelled call after the grace period
- **THEN** the conformance check fails and reports the node type and the late output count
