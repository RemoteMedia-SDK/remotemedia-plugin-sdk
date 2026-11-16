## ADDED Requirements

### Requirement: Standalone LiteRT Plugin Repository
The LiteRT nodes (`WhisperLiteRtNode`, `GemmaAssistantNode`, `KokoroTtsNode`) SHALL be built, versioned and released from their own repository, with the history imported from `remotemedia-sdk-base/examples/litert-nodes`, and SHALL NOT be built from `remotemedia-sdk-base`. The repository's release SHALL publish `liblitert-nodes-{arch}-{os}.{ext}` for Android (aarch64), Windows (x86_64) and Linux (x86_64), together with a release manifest, so that a manifest can name the plugin by version.

#### Scenario: Plugin resolved by version
- **WHEN** a manifest lists `"plugins": ["litert-nodes@<tag>"]` and names `GemmaAssistantNode`
- **THEN** the host downloads the asset for its platform, verifies it, loads it, and creates the node

#### Scenario: Base no longer builds LiteRT
- **WHEN** the base repository's CI baseline runs
- **THEN** no LiteRT, Kokoro C++ or Abseil code is compiled, and `examples/litert-nodes/` contains only a pointer to the new repository

### Requirement: LiteRT Runtime Library Without Plugin Export
The LiteRT repository SHALL provide its LiteRT and LiteRT-LM loading, CompiledModel, backend and `.litertlm` container code as a library crate. That crate SHALL have no `plugin_export!` and no C++ build, so that other plugins and applications can link it without exporting LiteRT's factories.

#### Scenario: RAG phone mode links the runtime only
- **WHEN** the RAG assistant plugin is built with phone-mode generation
- **THEN** it links the LiteRT runtime library, its shared library exports only the RAG plugin's entry point, and its build does not compile the Kokoro phonemizer

### Requirement: Unchanged LiteRT Node Contract Across the Move
After the move, the LiteRT nodes SHALL accept the same node types, parameters and defaults, and SHALL produce the same outputs, as before it. The room mode of `GemmaAssistantNode` SHALL be a room agent built on `remotemedia-room-agent`. The phone assistant agent SHALL still meet the `Assistant Room Migration` latency, kill and drain limits on the same hardware.

#### Scenario: Existing manifest runs unchanged
- **WHEN** `phone_assistant_room_agent/pixel.json` is run with the plugin directory taken from the new repository's release
- **THEN** it runs without edits other than the plugin path, and the agent answers, speaks and presents as before

#### Scenario: Latency unchanged on the phone
- **WHEN** `ask.py` runs with the agent on the phone after the move
- **THEN** the medians are within the limits recorded in `agent-participants` (0.90 s, 2.23 s, 4.04 s)

### Requirement: Consumers Reference the LiteRT Repository
Every build that uses LiteRT code SHALL depend on the LiteRT repository's crates, either by sibling path during development or by pinned revision, and SHALL NOT depend on a path inside `remotemedia-sdk-base/examples/` or on a crate path that does not exist.

#### Scenario: Android in-process example builds
- **WHEN** `remotemedia-sdk-base/examples/android-inprocess` is built for `aarch64-linux-android` with the LiteRT repository checked out beside it
- **THEN** its LiteRT dependencies resolve and the build succeeds

#### Scenario: Whisper plugin's Android export compiles
- **WHEN** the `whisper` plugin is built for Android
- **THEN** every LiteRT factory it exports exists in the LiteRT crate it depends on
