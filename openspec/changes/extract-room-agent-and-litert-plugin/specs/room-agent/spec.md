## ADDED Requirements

### Requirement: Shared Room-Agent Crate
The plugin SDK SHALL provide a `remotemedia-room-agent` crate implementing the member side of the agent link defined by scaling's `agent-participants` capability. The crate SHALL depend on no model runtime, no host crate, no loadable ABI crate and no private repository, so that any plugin can build a room agent from it.

#### Scenario: A plugin builds a room agent without LiteRT
- **WHEN** a plugin crate depends on `remotemedia-room-agent` and `remotemedia-plugin-sdk` but not on any LiteRT crate
- **THEN** it can implement a room agent node and export it with `plugin_export!`, and its dependency tree contains no LiteRT, Abseil or host crate

#### Scenario: Same trait instance as the plugin
- **WHEN** a plugin depends on `remotemedia-room-agent` and on `remotemedia-plugin-sdk` from the same checkout or revision
- **THEN** both resolve to one `remotemedia-traits` crate, and the room agent node is accepted wherever an `AsyncStreamingNode` is expected

### Requirement: Link Frame Codec
The crate SHALL decode the frames an agent receives on its link: room-log lines (`kind` `text` or `voice`) with their `seq`, `history`, `author_kind` and `interrupted` fields; `history_end`; and `reply_interrupted` with its reply id. It SHALL treat every other kind, including all `floor` events, as ignored, and a plain Text payload as a live line from a person. It SHALL encode the agent's publications in the contract's shape: `presence` `listening` (with `to`), `thinking` (with `reply`, `to`, and `line` only when the answered line had a `seq`), `idle` (with `reply`), and sentence lines carrying `text`, `say` and `reply`, plus any extra fields the agent adds.

#### Scenario: Interrupted agent history line
- **WHEN** the agent receives `{"kind":"voice","text":"The largest ocean is","seq":9,"history":true,"author_kind":"agent","interrupted":true}`
- **THEN** it decodes to a line with that text, `seq` 9, history, an agent author and `interrupted` set

#### Scenario: Busy-floor refusal is ignored
- **WHEN** the agent receives `{"kind":"floor","event":"refused","reason":"floor_busy","reply":"x-1"}`
- **THEN** it decodes as ignored, and the reply is not treated as stopped

#### Scenario: Thinking without a line
- **WHEN** a reply starts for a line that had no `seq`
- **THEN** the published `thinking` presence has no `line` field

#### Scenario: Shapes match the participant port
- **WHEN** scaling's participant port produces each kind of frame it delivers to an agent
- **THEN** the codec decodes each one as the frame the port intended (checked by a scaling test)

### Requirement: Addressing and Follow-Up
A person's line SHALL be addressed to the agent when it contains a wake word (case-insensitive, whole word) with more than one other word that is neither a wake word nor a filler word, or when the agent responds to all lines. A line consisting of a wake word with at most one such other word SHALL NOT be answered on its own. Instead, the speaker's next line within `follow_up_secs` SHALL be answered as the bare line followed by that line, and the agent SHALL publish `listening` for the speaker while it waits. History lines SHALL never be answered: a speaker whose last history line was a bare name waits for their question for `follow_up_secs` from when that history line arrived, and a speaker with any later history line does not wait.

#### Scenario: Bare name, then the question
- **WHEN** Ana says "Hey Pixel" and, 3 s later with `follow_up_secs` 8, says "what time is it?"
- **THEN** the agent publishes `listening` to Ana after the first line, and answers "Hey Pixel what time is it?" after the second

#### Scenario: Someone else's line is not the question
- **WHEN** Ana says "Hey Pixel" and Ben then says "lunch?"
- **THEN** no reply starts for Ben's line, and Ana is still waiting

#### Scenario: Wait expired
- **WHEN** Ana's question arrives 9 s after her bare name with `follow_up_secs` 8
- **THEN** no reply starts

### Requirement: Attribution and Own Lines
The agent SHALL name a line's author by `participant.display_name`, else `participant.name`, else the origin id with any trailing `-` plus four hex digits removed. An agent-authored line SHALL be recorded as the agent's own, under the agent's `name`, when the author's name equals `name` or the origin id equals `name` or `member_id`, compared case-insensitively. Otherwise it SHALL be recorded under the other agent's name. Agent-authored lines SHALL never be answered.

#### Scenario: Own line after a takeover
- **WHEN** an agent with `name` "Pixel" and `member_id` "pixel-agent" receives a history line authored by origin `pixel-agent` with display name "pixel"
- **THEN** the line is recorded as Pixel's own, and no reply starts

### Requirement: Structured Room History
The agent SHALL keep one bounded history of lines (author, person or agent, text, interrupted, seq). Prompt text and structured history records SHALL both be derived from it. Its own finished replies, its cut-off replies, other agents' lines and people's lines SHALL all be recorded in it. A structured record SHALL bound the author to 256 characters and the text to 4,000 characters.

#### Scenario: A finished reply is in structured history
- **WHEN** a reply finishes with text "It is noon."
- **THEN** the next turn's structured history ends with a record authored by the agent, role agent, that text, and `interrupted` false

#### Scenario: History is bounded
- **WHEN** 29 lines are recorded with a bound of 24
- **THEN** the history holds the last 24 lines

### Requirement: Reply Lifecycle
For each answered line the agent SHALL assign a reply id `"<instance>-<n>"`, where `instance` is a UUID v4 unique to the node instance and `n` counts its replies, then publish `thinking`, each finished sentence, and `idle` when the reply ends. Replies SHALL be produced one at a time, in order. After a `reply_interrupted` for a reply id, nothing more of that reply SHALL be published. When a reply ends before completing, its generated text SHALL be recorded as an interrupted line, unless the cancellation's `partial_output` is `Discard`, in which case nothing of it SHALL be recorded.

#### Scenario: Room stops a reply
- **WHEN** `reply_interrupted` arrives for the reply being generated
- **THEN** no further sentence of that reply is published, and its text so far is recorded as interrupted

#### Scenario: Abort discards the partial reply
- **WHEN** a reply is cancelled with an abort disposition after generating "The largest ocean is"
- **THEN** no line is recorded for it

#### Scenario: Reply ids differ across instances
- **WHEN** an agent unit moves to another runner and answers again
- **THEN** the new instance's reply ids share no instance part with the old instance's ids

### Requirement: Responder Contract
A backend SHALL provide a room agent by implementing a `Responder`, which receives one turn at a time and streams its reply. A turn contains the speaker, the question, the answered line's `seq`, the reply id, a history snapshot and the call's cancel token. All link decoding, addressing, attribution, history, reply ids, presence, stopping and cancellation SHALL be done by the crate's `RoomAgent`, not by the responder. A `RoomAgent` SHALL be a text generator that observes the cancel token, and SHALL pass the generator cancellation conformance check. When a cancellation discards pending input, the lines heard since the last reply SHALL be dropped from the next prompt.

#### Scenario: Conformance with a scripted responder
- **WHEN** `assert_generator_cancels` runs against a `RoomAgent` whose responder streams text slowly
- **THEN** no output arrives after the cancel within the grace period, each disposition is honoured, and the next turn's first output is not delayed by the abandoned one

#### Scenario: Responder sees only its turn
- **WHEN** a person's live line addresses the agent
- **THEN** the responder is called once with that speaker, question and line `seq`, after `thinking` has been published

### Requirement: Room Agent Configuration
The crate SHALL parse the room-agent settings from a node's parameters independently of any model settings: `name` (default `Pixel`), `wake_words` (default the lowercased name), `respond` (`mention` default, or `all`), `follow_up_secs` (default 8, finite, 0–120) and `member_id` (default `name`). It SHALL reject other `respond` values and out-of-range `follow_up_secs` with an error naming the key. It SHALL NOT require any model parameter.

#### Scenario: Server-mode agent without a model path
- **WHEN** a node is created with room settings and no `model_path`
- **THEN** the room settings parse successfully

### Requirement: Sentence Splitting
The crate SHALL split streamed text into sentences as they complete, at `.`, `!` or `?` followed by whitespace, or at a newline. It SHALL also support these options: a first piece that may end at a comma after a configured length; not splitting after a short capitalised word or a digit ("Dr.", "U.S.", "3."); ending a long piece at `:` or `;`; and an output byte bound that returns an error when exceeded. The remaining text SHALL be returned when the reply finishes.

#### Scenario: Gemma configuration
- **WHEN** "The tallest mountain on Earth, by height above sea level, is Everest. " is pushed with a 24-character first clause and the abbreviation guard on
- **THEN** the first sentence emitted ends at the first comma after 24 characters

#### Scenario: Bounded configuration
- **WHEN** more than 32,000 bytes are pushed with a 32,000-byte bound
- **THEN** `push` returns an error

### Requirement: Opt-In Stage Tracing
The crate SHALL provide content-free stage tracing that writes one `RAG_STAGE ` JSON line per event to stderr, only when `REMOTEMEDIA_STAGE_TRACE=1` or `REMOTEMEDIA_RAG_TRACE=1` is set. A stage dropped before it finishes SHALL record its end with outcome `unfinished`.

#### Scenario: Off by default
- **WHEN** neither variable is set
- **THEN** no trace line is written

#### Scenario: Old variable still works
- **WHEN** only `REMOTEMEDIA_RAG_TRACE=1` is set
- **THEN** trace lines are written in the same format the RAG evaluation parser reads today
