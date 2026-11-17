//! `RoomAgent`: the node. It reads the link, keeps the room, takes one
//! reply at a time and publishes it; a [`Responder`] only produces the text.

use std::sync::Mutex;
use std::time::Instant;

use async_trait::async_trait;
use remotemedia_traits::{
    rpc::CallContext, AsyncStreamingNode, CancelReason, CancelSource, CancelStrategy, CancelToken,
    Cancellable, Cancellation, GeneratorRole, NodeRuntimeContextRead, Retention, TextGenerator,
};
use remotemedia_types::{Error, RuntimeData};
use serde_json::{json, Map, Value};

use crate::addressing::{addressed, Addressed};
use crate::attribution::{is_own, room_speaker_name, speaker_name};
use crate::config::{Publish, RoomAgentConfig};
use crate::history::{Line, Room, Snapshot};
use crate::link::{self, Inbound};
use crate::text::Sentences;

/// One reply to produce.
pub struct Turn<'a> {
    /// Who the reply is to.
    pub speaker: &'a str,
    /// What they said to the agent: the answered line, or their bare name and
    /// the line that followed it.
    pub question: &'a str,
    /// The `seq` of the answered line, when it has one.
    pub line: Option<u64>,
    /// `"<instance>-<n>"`.
    pub reply: &'a str,
    /// This node instance's id (the reply id's first part).
    pub instance: &'a str,
    pub session_id: &'a str,
    /// The history when the reply started; the answered line is last.
    pub history: &'a Snapshot,
    pub config: &'a RoomAgentConfig,
    /// Fires when the call is cancelled, and when it is dropped.
    pub cancel: CancelToken,
}

/// How a reply ended, as the responder saw it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Finished.
    Done,
    /// Finished without an answer: say this sentence instead.
    Fallback(String),
    /// Stopped early: publish nothing more.
    Cancelled,
}

/// A backend that answers one turn at a time.
#[async_trait]
pub trait Responder: Send + Sync + 'static {
    /// How [`ReplyOut::text`] splits the reply into sentences.
    fn sentences(&self) -> Sentences {
        Sentences::new()
    }

    /// Produces the reply through `out`: generated text with
    /// [`ReplyOut::text`], or finished sentences with
    /// [`ReplyOut::sentence`]. Stop when either returns `Ok(false)` or
    /// `turn.cancel` fires.
    async fn respond(&self, turn: Turn<'_>, out: &mut ReplyOut<'_>) -> Result<Outcome, Error>;

    /// Called for each cancelled call, after the agent applied the
    /// disposition to its own state.
    async fn on_cancel(&self, _cancellation: &Cancellation) {}
}

type Publisher<'a> = dyn FnMut(RuntimeData) -> Result<(), Error> + Send + 'a;

/// Where a responder sends its reply. Dropped before the reply ended (the
/// runtime dropped the call), it records what was generated as an
/// interrupted line, unless the cancel discards partial output.
pub struct ReplyOut<'a> {
    publish: &'a mut Publisher<'a>,
    agent: &'a RoomAgentConfig,
    node_type: &'a str,
    room: &'a Mutex<Room>,
    reply: &'a str,
    to: &'a str,
    /// The call's token, for the cancel's disposition.
    call: CancelToken,
    sentences: Sentences,
    /// What the reply has said or generated so far.
    said: String,
    published: usize,
    started: Instant,
    first_ms: Option<f64>,
    ended: bool,
}

impl<'a> ReplyOut<'a> {
    /// Whether the reply may still publish: the call is not cancelled and the
    /// room has not stopped it.
    pub fn open(&self) -> bool {
        !self.call.is_cancelled()
            && self.room.lock().unwrap().stopped.as_deref() != Some(self.reply)
    }

    /// Generated text: kept, and each sentence it completes is published.
    /// `Ok(false)`: the reply is stopped; nothing was kept.
    pub fn text(&mut self, text: &str) -> Result<bool, Error> {
        if !self.open() {
            return Ok(false);
        }
        self.first_ms
            .get_or_insert_with(|| self.started.elapsed().as_secs_f64() * 1000.0);
        self.said.push_str(text);
        let ready = self
            .sentences
            .push(text)
            .map_err(|e| Error::InvalidData(e.to_string()))?;
        for sentence in ready {
            self.say(&sentence, Map::new())?;
        }
        Ok(true)
    }

    /// A finished sentence, published as it is; `extra` fields (such as
    /// `sources`) are added to its line. `Ok(false)`: the reply is stopped;
    /// nothing was published.
    pub fn sentence(&mut self, sentence: &str, extra: Map<String, Value>) -> Result<bool, Error> {
        if !self.open() {
            return Ok(false);
        }
        self.first_ms
            .get_or_insert_with(|| self.started.elapsed().as_secs_f64() * 1000.0);
        if !self.said.is_empty() {
            self.said.push(' ');
        }
        self.said.push_str(sentence);
        self.say(sentence, extra)?;
        Ok(true)
    }

    /// Sentences published so far.
    pub fn published(&self) -> usize {
        self.published
    }

    pub fn reply(&self) -> &str {
        self.reply
    }

    fn say(&mut self, sentence: &str, extra: Map<String, Value>) -> Result<(), Error> {
        eprintln!(
            "[{}] reply {} +{:.0} ms: {sentence}",
            self.node_type,
            self.reply,
            self.started.elapsed().as_secs_f64() * 1000.0
        );
        let mut value = match self.agent.publish {
            Publish::Room => link::sentence_line(sentence, self.reply),
            Publish::Pipeline {
                sentences_as_text: true,
            } => {
                self.published += 1;
                return (self.publish)(RuntimeData::Text(sentence.to_string()));
            }
            Publish::Pipeline { .. } => json!({
                "kind": "assistant", "say": sentence, "reply": self.reply, "to": self.to,
            }),
        };
        for (key, field) in extra {
            value[key] = field;
        }
        self.published += 1;
        (self.publish)(RuntimeData::Json(value))
    }

    /// Records the reply as it ended.
    fn record(&mut self, interrupted: bool) {
        self.ended = true;
        let text = self.said.trim();
        if !text.is_empty() {
            self.room
                .lock()
                .unwrap()
                .note(Line::agent(&self.agent.name, text, interrupted, None));
        }
    }
}

impl Drop for ReplyOut<'_> {
    fn drop(&mut self) {
        if self.ended {
            return;
        }
        if self
            .call
            .cancellation()
            .is_some_and(|c| c.disposition.partial_output == Retention::Discard)
        {
            eprintln!(
                "[{}] reply {} cut off; rolled back (the cancel discards it)",
                self.node_type, self.reply
            );
            self.ended = true;
            return;
        }
        if !self.said.trim().is_empty() {
            eprintln!(
                "[{}] reply {} cut off after {} characters; kept in the context, marked",
                self.node_type,
                self.reply,
                self.said.trim().chars().count()
            );
        }
        if let Ok(mut room) = self.room.lock() {
            let text = self.said.trim();
            if !text.is_empty() {
                room.note(Line::agent(&self.agent.name, text, true, None));
            }
        }
        self.ended = true;
    }
}

/// Cancels a reply's sub-work when the call ends, also when its future is
/// dropped by a host whose token never fires.
struct Work(CancelSource);

impl Drop for Work {
    fn drop(&mut self) {
        self.0
            .cancel(Cancellation::new(CancelReason::CallerCancelled));
    }
}

/// A room agent node: everything but producing the reply's text.
pub struct RoomAgent<R: Responder> {
    node_type: &'static str,
    config: RoomAgentConfig,
    responder: R,
    room: Mutex<Room>,
    /// One reply at a time, in order.
    turn: futures_util::lock::Mutex<()>,
    /// Tags this instance's reply ids.
    instance: String,
}

impl<R: Responder> RoomAgent<R> {
    pub fn new(node_type: &'static str, config: RoomAgentConfig, responder: R) -> Self {
        Self {
            node_type,
            config,
            responder,
            room: Mutex::new(Room::default()),
            turn: futures_util::lock::Mutex::new(()),
            instance: uuid::Uuid::new_v4().to_string(),
        }
    }

    pub fn config(&self) -> &RoomAgentConfig {
        &self.config
    }

    pub fn responder(&self) -> &R {
        &self.responder
    }

    /// This instance's reply-id prefix.
    pub fn instance(&self) -> &str {
        &self.instance
    }

    /// The room's state (for tests and diagnostics).
    pub fn room(&self) -> std::sync::MutexGuard<'_, Room> {
        self.room.lock().unwrap()
    }

    fn room_mode(&self) -> bool {
        self.config.is_room()
    }

    /// Replies to what `speaker` said; `line` is the `seq` of that line.
    async fn reply<F>(
        &self,
        speaker: &str,
        question: &str,
        line: Option<u64>,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let _turn = self.turn.lock().await;
        let call = ctx.cancel_token();
        if call.is_cancelled() {
            return Ok(0);
        }
        let work = Work(call.child());
        let (n, history) = self.room.lock().unwrap().start_reply();
        // Unique across instances, so a reply from the runner a unit moved
        // to is not mistaken for one from before the move.
        let reply = format!("{}-{n}", self.instance);
        callback(RuntimeData::Json(if self.room_mode() {
            link::presence_thinking(&reply, speaker, line)
        } else {
            json!({"kind": "assistant", "state": "thinking", "reply": reply, "to": speaker})
        }))?;

        let publish: &mut Publisher<'_> = &mut callback;
        let mut out = ReplyOut {
            publish,
            agent: &self.config,
            node_type: self.node_type,
            room: &self.room,
            reply: &reply,
            to: speaker,
            call: call.clone(),
            sentences: self.responder.sentences(),
            said: String::new(),
            published: 0,
            started: Instant::now(),
            first_ms: None,
            ended: false,
        };
        let turn = Turn {
            speaker,
            question,
            line,
            reply: &reply,
            instance: &self.instance,
            session_id: ctx.session_id(),
            history: &history,
            config: &self.config,
            cancel: work.0.token(),
        };
        let outcome = self.responder.respond(turn, &mut out).await?;
        let total_ms = out.started.elapsed().as_secs_f64() * 1000.0;
        // A stopped reply (the responder saw `Ok(false)` and may say
        // `Cancelled`) still ends with `idle`.
        let stopped = self.room.lock().unwrap().stopped.as_deref() == Some(reply.as_str());
        // Cancelled: publish nothing more; dropping `out` records the reply as
        // the cancel's disposition says.
        if call.is_cancelled() || (outcome == Outcome::Cancelled && !stopped) {
            return Ok(1 + out.published);
        }
        if stopped {
            eprintln!("[{}] reply {reply} stopped by the room", self.node_type);
        } else {
            if let Some(rest) = out.sentences.finish() {
                out.say(&rest, Map::new())?;
            }
            if let Outcome::Fallback(sentence) = &outcome {
                out.sentence(sentence, Map::new())?;
            }
        }
        out.record(stopped);
        let published = 1 + out.published;
        let first_ms = out.first_ms;
        let said = out.said.trim().to_string();
        drop(out);
        if self.room_mode() {
            eprintln!(
                "[{}] reply {reply} ended: first text {:.0} ms, total {total_ms:.0} ms",
                self.node_type,
                first_ms.unwrap_or(total_ms)
            );
            callback(RuntimeData::Json(link::presence_idle(&reply)))?;
        } else {
            callback(RuntimeData::Json(json!({
                "kind": "assistant", "text": said, "reply": reply, "to": speaker, "done": true,
                "first_ms": first_ms.map(f64::round), "total_ms": total_ms.round(),
            })))?;
        }
        Ok(published + 1)
    }
}

#[async_trait]
impl<R: Responder> AsyncStreamingNode for RoomAgent<R> {
    fn node_type(&self) -> &str {
        self.node_type
    }

    fn generator(&self) -> Option<GeneratorRole<'_>> {
        GeneratorRole::text(self)
    }

    async fn process(&self, _data: RuntimeData) -> Result<RuntimeData, Error> {
        Err(Error::InvalidData(format!(
            "{} streams its replies",
            self.node_type
        )))
    }

    async fn process_streaming<F>(
        &self,
        _data: RuntimeData,
        _session_id: Option<String>,
        _callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        Ok(0)
    }

    async fn process_streaming_with_ctx<F>(
        &self,
        data: RuntimeData,
        ctx: &dyn NodeRuntimeContextRead,
        mut callback: F,
    ) -> Result<usize, Error>
    where
        F: FnMut(RuntimeData) -> Result<(), Error> + Send,
    {
        let room_mode = self.room_mode();
        let line = if room_mode {
            match link::decode(&data) {
                Inbound::Line(line) => line,
                Inbound::Stop(reply) => {
                    self.room.lock().unwrap().stopped = Some(reply);
                    return Ok(0);
                }
                Inbound::HistoryEnd => {
                    let room = self.room.lock().unwrap();
                    eprintln!(
                        "[{}] history: {} lines, {} waiting for their question",
                        self.node_type,
                        room.history.len(),
                        room.waiting.len()
                    );
                    return Ok(0);
                }
                Inbound::Ignored => return Ok(0),
            }
        } else {
            match link::line_text(&data) {
                Some(text) => link::InLine {
                    text,
                    seq: None,
                    history: false,
                    agent: false,
                    interrupted: false,
                },
                None => return Ok(0),
            }
        };
        let origin = ctx.origin();
        let speaker = if room_mode {
            room_speaker_name(origin.as_deref())
        } else {
            speaker_name(origin.as_deref())
        };
        if line.agent {
            // An agent's line is context: its own under its name, another's
            // under that agent's.
            let own = is_own(
                origin.as_deref(),
                &speaker,
                &self.config.name,
                &self.config.member_id,
            );
            let author = if own { &self.config.name } else { &speaker };
            self.room.lock().unwrap().note(Line::agent(
                author,
                &line.text,
                line.interrupted,
                line.seq,
            ));
            return Ok(0);
        }
        let addressed = addressed(&self.config.wake_words, self.config.respond_all, &line.text);
        let now = Instant::now();
        if line.history {
            self.room
                .lock()
                .unwrap()
                .recall(&speaker, &line.text, line.seq, addressed, now);
            return Ok(0);
        }
        let question = self.room.lock().unwrap().hear(
            &speaker,
            &line.text,
            line.seq,
            addressed,
            self.config.follow_up,
            now,
        );
        match question {
            Some(question) => {
                self.reply(&speaker, &question, line.seq, ctx, callback)
                    .await
            }
            None if addressed == Addressed::NameOnly => {
                // Shown while it waits for the question.
                callback(RuntimeData::Json(if room_mode {
                    link::presence_listening(&speaker)
                } else {
                    json!({
                        "kind": "assistant", "state": "listening", "to": speaker,
                        "for_ms": self.config.follow_up.as_millis() as u64,
                    })
                }))?;
                Ok(1)
            }
            None => Ok(0),
        }
    }
}

/// A text generator (openspec: add-generator-cancellation). The responder
/// observes the turn's token; a cut-off reply stays in the history, marked,
/// unless the cancel discards partial output; a cancel that discards pending
/// input also drops the lines heard since the last reply.
#[async_trait]
impl<R: Responder> Cancellable for RoomAgent<R> {
    fn cancel_strategy(&self) -> CancelStrategy {
        CancelStrategy::ObservesToken
    }

    async fn on_cancel(&self, _call: &CallContext, cancellation: &Cancellation) {
        if cancellation.disposition.pending_input == Retention::Discard {
            let dropped = self.room.lock().unwrap().discard_pending();
            if dropped > 0 {
                eprintln!(
                    "[{}] cancel discards {dropped} pending lines",
                    self.node_type
                );
            }
        }
        self.responder.on_cancel(cancellation).await;
    }
}

impl<R: Responder> TextGenerator for RoomAgent<R> {}
