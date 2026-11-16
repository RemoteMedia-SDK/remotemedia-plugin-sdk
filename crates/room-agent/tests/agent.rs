//! `RoomAgent` driven the way the runtime drives it, with a scripted
//! responder.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use remotemedia_plugin_sdk::testing::{assert_generator_cancels, ConformanceOptions};
use remotemedia_room_agent::{
    Outcome, ReplyOut, Responder, Role, RoomAgent, RoomAgentConfig, Sentences, Turn,
};
use remotemedia_traits::{
    emit::Origin, rpc::CallContext, AsyncStreamingNode, CancelReason, CancelSource, CancelToken,
    Cancellable, Cancellation, Disposition, MediaCapabilities, NodeRuntimeContextRead,
};
use remotemedia_types::{Error, RuntimeData};
use serde_json::{json, Value};

/// What each turn saw.
#[derive(Clone, Debug)]
struct Seen {
    speaker: String,
    question: String,
    line: Option<u64>,
    records: Vec<Value>,
    since_reply: usize,
}

/// Streams `pieces`, `delay` apart, and records each turn.
struct Script {
    pieces: Vec<&'static str>,
    delay: Duration,
    seen: Arc<Mutex<Vec<Seen>>>,
    /// Waited for after the first piece, when set.
    gate: Option<Arc<tokio::sync::Notify>>,
    outcome: Outcome,
}

impl Script {
    fn new(pieces: &[&'static str]) -> Self {
        Self {
            pieces: pieces.to_vec(),
            delay: Duration::ZERO,
            seen: Arc::default(),
            gate: None,
            outcome: Outcome::Done,
        }
    }
}

#[async_trait]
impl Responder for Script {
    fn sentences(&self) -> Sentences {
        Sentences::spoken()
    }

    async fn respond(&self, turn: Turn<'_>, out: &mut ReplyOut<'_>) -> Result<Outcome, Error> {
        self.seen.lock().unwrap().push(Seen {
            speaker: turn.speaker.into(),
            question: turn.question.into(),
            line: turn.line,
            records: turn.history.records(),
            since_reply: turn.history.since_reply.len(),
        });
        for (i, piece) in self.pieces.iter().enumerate() {
            if !self.delay.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep(self.delay) => {}
                    _ = turn.cancel.cancelled() => return Ok(Outcome::Cancelled),
                }
            }
            if !out.text(piece)? {
                return Ok(Outcome::Cancelled);
            }
            if i == 0 {
                if let Some(gate) = &self.gate {
                    gate.notified().await;
                }
            }
        }
        Ok(self.outcome.clone())
    }
}

struct Ctx {
    cancel: CancelToken,
    origin: Option<Arc<Origin>>,
}

impl Ctx {
    fn from(fields: &[(&str, &str)]) -> Self {
        let metadata: HashMap<String, String> = fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Self {
            cancel: CancelToken::never(),
            origin: Origin::from_metadata(&metadata).map(Arc::new),
        }
    }

    fn person(name: &str) -> Self {
        Self::from(&[
            ("participant.id", &format!("{}-1a2b", name.to_lowercase())),
            ("participant.display_name", name),
        ])
    }
}

impl NodeRuntimeContextRead for Ctx {
    fn origin(&self) -> Option<Arc<Origin>> {
        self.origin.clone()
    }
    fn session_id(&self) -> &str {
        "room-1"
    }
    fn node_id(&self) -> &str {
        "assistant"
    }
    fn cancel_token(&self) -> CancelToken {
        self.cancel.clone()
    }
    fn capability(&self, _: &str) -> Option<MediaCapabilities> {
        None
    }
    fn insert_capability(&self, _: &str, _: MediaCapabilities) {}
    fn snapshot_any(&self, _: &str) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
    fn state_any(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
    fn try_state_any(&self) -> Option<Arc<dyn std::any::Any + Send + Sync>> {
        None
    }
}

fn agent(params: Value, script: Script) -> RoomAgent<Script> {
    RoomAgent::new(
        "TestAgent",
        RoomAgentConfig::from_params(&params).unwrap(),
        script,
    )
}

fn room_agent(script: Script) -> RoomAgent<Script> {
    agent(
        json!({"publish": "room", "name": "Pixel", "member_id": "pixel-agent"}),
        script,
    )
}

async fn run(agent: &RoomAgent<Script>, ctx: &Ctx, data: Value) -> Vec<RuntimeData> {
    let mut out = Vec::new();
    agent
        .process_streaming_with_ctx(RuntimeData::Json(data), ctx, |v| {
            out.push(v);
            Ok(())
        })
        .await
        .unwrap();
    out
}

fn json_of(out: &[RuntimeData]) -> Vec<Value> {
    out.iter()
        .map(|d| match d {
            RuntimeData::Json(v) => v.clone(),
            RuntimeData::Text(t) => json!({"text_output": t}),
            _ => json!(null),
        })
        .collect()
}

#[tokio::test]
async fn room_reply_lifecycle() {
    let script = Script::new(&["It is ", "noon. Enjoy ", "lunch"]);
    let seen = script.seen.clone();
    let agent = room_agent(script);
    let ana = Ctx::person("Ana");

    let out = json_of(
        &run(
            &agent,
            &ana,
            json!({"kind": "voice", "text": "Hey Pixel", "seq": 4}),
        )
        .await,
    );
    assert_eq!(
        out,
        [json!({"kind": "presence", "state": "listening", "to": "Ana"})]
    );
    let out = json_of(
        &run(
            &agent,
            &ana,
            json!({"kind": "voice", "text": "what time is it?", "seq": 5}),
        )
        .await,
    );
    let reply = out[0]["reply"].as_str().unwrap().to_string();
    assert!(reply.starts_with(agent.instance()) && reply.ends_with("-1"));
    assert_eq!(
        out,
        [
            json!({"kind": "presence", "state": "thinking", "reply": reply, "to": "Ana", "line": 5}),
            json!({"kind": "text", "text": "It is noon.", "say": "It is noon.", "reply": reply}),
            json!({"kind": "text", "text": "Enjoy lunch", "say": "Enjoy lunch", "reply": reply}),
            json!({"kind": "presence", "state": "idle", "reply": reply}),
        ]
    );
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen[0].speaker, "Ana");
        assert_eq!(seen[0].question, "Hey Pixel what time is it?");
        assert_eq!(seen[0].line, Some(5));
        // The answered line is not among the records.
        assert_eq!(seen[0].records.len(), 1);
    }

    // The finished reply is in the next turn's structured history.
    run(
        &agent,
        &ana,
        json!({"kind": "text", "text": "Pixel, and tomorrow?", "seq": 7}),
    )
    .await;
    let seen = seen.lock().unwrap();
    assert_eq!(
        seen[1].records.last().unwrap(),
        &json!({"author": "Pixel", "role": "agent", "text": "It is noon. Enjoy lunch", "interrupted": false})
    );
    // Since the last reply: the reply itself and the new question.
    assert_eq!(seen[1].since_reply, 2);
}

#[tokio::test]
async fn agent_and_history_lines_are_never_answered() {
    let script = Script::new(&["Hi."]);
    let seen = script.seen.clone();
    let agent = room_agent(script);
    // Its own line after a takeover, recognised by member id.
    let me = Ctx::from(&[
        ("participant.id", "pixel-agent"),
        ("participant.display_name", "Assistant"),
    ]);
    let other = Ctx::from(&[
        ("participant.id", "sage-agent"),
        ("participant.display_name", "Sage"),
    ]);
    let ana = Ctx::person("Ana");
    for (ctx, frame) in [
        (
            &me,
            json!({"kind": "voice", "text": "The largest ocean is", "seq": 1,
            "history": true, "author_kind": "agent", "interrupted": true}),
        ),
        (
            &other,
            json!({"kind": "text", "text": "Pixel, what do you think?", "seq": 2, "author_kind": "agent"}),
        ),
        (
            &ana,
            json!({"kind": "text", "text": "Pixel, old question?", "seq": 3, "history": true}),
        ),
        (&ana, json!({"kind": "history_end"})),
        (
            &ana,
            json!({"kind": "floor", "event": "refused", "reason": "floor_busy", "reply": "x-1"}),
        ),
    ] {
        assert!(run(&agent, ctx, frame).await.is_empty());
    }
    assert!(seen.lock().unwrap().is_empty());
    let room = agent.room();
    let lines: Vec<_> = room.history.recent().cloned().collect();
    assert_eq!(lines[0].author, "Pixel");
    assert!(lines[0].interrupted && lines[0].role == Role::Agent);
    assert_eq!(lines[1].author, "Sage");
    assert_eq!(lines[2].role, Role::Person);
}

#[tokio::test]
async fn the_room_stops_a_reply() {
    let mut script = Script::new(&["The largest ", "ocean is the Pacific. ", "It is huge."]);
    let gate = Arc::new(tokio::sync::Notify::new());
    script.gate = Some(gate.clone());
    let agent = Arc::new(room_agent(script));
    let ana = Ctx::person("Ana");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let a = agent.clone();
    let task = tokio::spawn(async move {
        let ana = Ctx::person("Ana");
        a.process_streaming_with_ctx(
            RuntimeData::Json(
                json!({"kind": "text", "text": "Pixel, the largest ocean?", "seq": 1}),
            ),
            &ana,
            move |v| {
                let _ = tx.send(v);
                Ok(())
            },
        )
        .await
    });
    let thinking = match rx.recv().await.unwrap() {
        RuntimeData::Json(v) => v,
        _ => unreachable!(),
    };
    let reply = thinking["reply"].as_str().unwrap().to_string();
    run(
        &agent,
        &ana,
        json!({"kind": "reply_interrupted", "reply": reply}),
    )
    .await;
    gate.notify_one();
    task.await.unwrap().unwrap();
    let rest = json_of(&std::iter::from_fn(|| rx.try_recv().ok()).collect::<Vec<_>>());
    // Nothing more of it but `idle`.
    assert_eq!(
        rest,
        [json!({"kind": "presence", "state": "idle", "reply": reply})]
    );
    let room = agent.room();
    let last = room.history.last().unwrap();
    assert_eq!(last.text, "The largest");
    assert!(last.interrupted);
}

/// Runs a reply, cancels it with `disposition` after its first sentence, and
/// drops it, as the runtime does on a barge.
async fn cancelled(disposition: Disposition) -> RoomAgent<Script> {
    let mut script = Script::new(&["The largest ", "ocean is ", "the Pacific. ", "It is huge."]);
    script.delay = Duration::from_millis(30);
    let agent = Arc::new(room_agent(script));
    let source = CancelSource::new();
    let mut ctx = Ctx::person("Ana");
    ctx.cancel = source.token();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let a = agent.clone();
    let task = tokio::spawn(async move {
        a.process_streaming_with_ctx(
            RuntimeData::Json(
                json!({"kind": "text", "text": "Pixel, the largest ocean?", "seq": 1}),
            ),
            &ctx,
            move |v| {
                let _ = tx.send(v);
                Ok(())
            },
        )
        .await
    });
    loop {
        if matches!(rx.recv().await, Some(RuntimeData::Json(v)) if v["kind"] == "text") {
            break;
        }
    }
    let cancellation = Cancellation::with_disposition(CancelReason::BargeIn, disposition);
    source.cancel(cancellation);
    let call = CallContext {
        session_id: Some("room-1".into()),
        node_id: "assistant".into(),
    };
    Cancellable::on_cancel(&*agent, &call, &cancellation).await;
    task.abort();
    let _ = task.await;
    // Nothing is published after the cancel.
    assert!(std::iter::from_fn(|| rx.try_recv().ok())
        .all(|v| !matches!(v, RuntimeData::Json(ref j) if j["state"] == "idle")));
    Arc::try_unwrap(agent).ok().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_interrupted_reply_stays_marked() {
    let agent = cancelled(Disposition::INTERRUPT).await;
    let room = agent.room();
    let last = room.history.last().unwrap();
    assert_eq!(last.author, "Pixel");
    assert_eq!(last.text, "The largest ocean is the Pacific.");
    assert!(last.interrupted);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_aborted_reply_is_discarded() {
    let agent = cancelled(Disposition::ABORT).await;
    let room = agent.room();
    assert!(room.history.recent().all(|line| line.role == Role::Person));
    // The abort also drops the lines heard since the last reply.
    assert_eq!(room.history.since_reply().count(), 0);
}

#[tokio::test]
async fn pipeline_mode_publishes_assistant_messages() {
    let agent = agent(
        json!({"respond": "all", "name": "Pixel"}),
        Script::new(&["It is noon."]),
    );
    let ana = Ctx::from(&[("participant.id", "ana-1f3a")]);
    let out = json_of(
        &run(
            &agent,
            &ana,
            json!({"kind": "text", "text": "What time is it?"}),
        )
        .await,
    );
    let reply = out[0]["reply"].clone();
    assert_eq!(
        out[0],
        json!({"kind": "assistant", "state": "thinking", "reply": reply, "to": "ana"})
    );
    assert_eq!(
        out[1],
        json!({"kind": "assistant", "say": "It is noon.", "reply": reply, "to": "ana"})
    );
    assert_eq!(out[2]["kind"], "assistant");
    assert_eq!(out[2]["done"], true);
    assert_eq!(out[2]["text"], "It is noon.");
    assert!(out[2]["total_ms"].is_number());
    // A room frame is not read in the pipeline mode.
    assert!(
        run(&agent, &ana, json!({"kind": "voice", "text": "Hello?"}))
            .await
            .is_empty()
    );

    let mention = agent_with_text();
    let out = json_of(&run(&mention, &ana, json!({"kind": "text", "text": "Hey Pixel"})).await);
    assert_eq!(
        out,
        [json!({"kind": "assistant", "state": "listening", "to": "ana", "for_ms": 8000})]
    );
    let out = json_of(&run(&mention, &ana, json!({"kind": "text", "text": "the time?"})).await);
    assert_eq!(out[1], json!({"text_output": "It is noon."}));
}

fn agent_with_text() -> RoomAgent<Script> {
    agent(json!({"sentences": "text"}), Script::new(&["It is noon."]))
}

#[tokio::test]
async fn fallback_sentence_is_said() {
    let mut script = Script::new(&[]);
    script.outcome = Outcome::Fallback("I couldn't find that.".into());
    let agent = room_agent(script);
    let out = json_of(
        &run(
            &agent,
            &Ctx::person("Ana"),
            json!({"kind": "text", "text": "Pixel, the gate?"}),
        )
        .await,
    );
    assert_eq!(out[1]["text"], "I couldn't find that.");
    assert_eq!(out[2]["state"], "idle");
}

#[tokio::test]
async fn reply_ids_differ_across_instances() {
    let a = room_agent(Script::new(&["Hi."]));
    let b = room_agent(Script::new(&["Hi."]));
    assert_ne!(a.instance(), b.instance());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn passes_generator_conformance() {
    let mut script = Script::new(&["One. ", "Two. ", "Three. ", "Four. ", "Five."]);
    script.delay = Duration::from_millis(40);
    let agent = Arc::new(agent(json!({"respond": "all", "publish": "room"}), script));
    let report = assert_generator_cancels(
        agent,
        || RuntimeData::Json(json!({"kind": "text", "text": "Count to five", "seq": 1})),
        ConformanceOptions {
            counts: |out| matches!(out, RuntimeData::Json(v) if v["kind"] == "text"),
            first_output_timeout: Duration::from_secs(5),
            ..Default::default()
        },
    )
    .await
    .unwrap_or_else(|e| panic!("{e}"));
    assert_eq!(report.cancelled.len(), 2);
}
