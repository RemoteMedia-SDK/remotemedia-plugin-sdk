//! What was said in the room: one bounded history of lines, from which both
//! prompt text and structured history records are rendered.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::addressing::Addressed;

/// Lines kept.
pub const MAX_LINES: usize = 24;
/// Bounds of a structured history record.
pub const MAX_AUTHOR_CHARS: usize = 256;
pub const MAX_TEXT_CHARS: usize = 4000;

/// Who said a line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Person,
    Agent,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Person => "person",
            Role::Agent => "agent",
        }
    }
}

/// One line of the room.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Line {
    pub author: String,
    pub role: Role,
    pub text: String,
    /// Cut off before it finished.
    pub interrupted: bool,
    pub seq: Option<u64>,
}

impl Line {
    pub fn person(author: &str, text: &str, seq: Option<u64>) -> Self {
        Self {
            author: author.into(),
            role: Role::Person,
            text: text.into(),
            interrupted: false,
            seq,
        }
    }

    pub fn agent(author: &str, text: &str, interrupted: bool, seq: Option<u64>) -> Self {
        Self {
            author: author.into(),
            role: Role::Agent,
            text: text.into(),
            interrupted,
            seq,
        }
    }

    /// `"author: text"`, marked when it was cut off.
    pub fn prompt_line(&self) -> String {
        if self.interrupted {
            format!(
                "{}: {} [interrupted before finishing]",
                self.author, self.text
            )
        } else {
            format!("{}: {}", self.author, self.text)
        }
    }

    /// `{author, role, text, interrupted}`, with the author and text bounded.
    pub fn record(&self) -> Value {
        json!({
            "author": self.author.chars().take(MAX_AUTHOR_CHARS).collect::<String>(),
            "role": self.role.as_str(),
            "text": self.text.chars().take(MAX_TEXT_CHARS).collect::<String>(),
            "interrupted": self.interrupted,
        })
    }
}

/// The last [`MAX_LINES`] lines, and where the last reply started.
#[derive(Clone, Debug, Default)]
pub struct History {
    lines: VecDeque<Line>,
    /// Lines ever noted.
    noted: u64,
    /// `noted` when the last reply started (or a cancel dropped the lines
    /// since).
    cursor: u64,
}

impl History {
    pub fn note(&mut self, line: Line) {
        self.lines.push_back(line);
        self.noted += 1;
        while self.lines.len() > MAX_LINES {
            self.lines.pop_front();
        }
    }

    /// Every line kept, oldest first.
    pub fn recent(&self) -> impl ExactSizeIterator<Item = &Line> + '_ {
        self.lines.iter()
    }

    /// The lines noted since the last reply started.
    pub fn since_reply(&self) -> impl Iterator<Item = &Line> + '_ {
        let first = self.noted - self.lines.len() as u64;
        self.lines
            .iter()
            .enumerate()
            .filter(move |(i, _)| first + *i as u64 >= self.cursor)
            .map(|(_, line)| line)
    }

    /// Starts a reply: lines noted from now on are "since the reply".
    pub fn mark_reply(&mut self) {
        self.cursor = self.noted;
    }

    pub fn len(&self) -> usize {
        self.lines.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    pub fn last(&self) -> Option<&Line> {
        self.lines.back()
    }
}

/// A copy of the history when a reply starts. The answered line is the last
/// of both lists.
#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    /// The lines since the previous reply started.
    pub since_reply: Vec<Line>,
    /// Every line kept.
    pub recent: Vec<Line>,
}

impl Snapshot {
    /// The recent lines before the answered one.
    pub fn before_question(&self) -> &[Line] {
        &self.recent[..self.recent.len().saturating_sub(1)]
    }

    /// Structured records of the lines before the answered one.
    pub fn records(&self) -> Vec<Value> {
        self.before_question().iter().map(Line::record).collect()
    }
}

/// A room agent's state.
#[derive(Debug, Default)]
pub struct Room {
    pub history: History,
    /// Replies started.
    pub replies: u64,
    /// Speakers who named the agent without a question yet: when, and what
    /// they said.
    pub waiting: HashMap<String, (Instant, String)>,
    /// The reply the room last said to stop.
    pub stopped: Option<String>,
}

impl Room {
    pub fn note(&mut self, line: Line) {
        self.history.note(line);
    }

    /// A person's live line. Returns what they said to the agent, if the line
    /// is for it: the line itself, or the bare name and this line when it
    /// follows their bare name within `follow_up`.
    pub fn hear(
        &mut self,
        speaker: &str,
        text: &str,
        seq: Option<u64>,
        addressed: Addressed,
        follow_up: Duration,
        now: Instant,
    ) -> Option<String> {
        self.note(Line::person(speaker, text, seq));
        let follow_up = self
            .waiting
            .remove(speaker)
            .filter(|(at, _)| now.duration_since(*at) <= follow_up)
            .map(|(_, name)| format!("{name} {text}"));
        match addressed {
            Addressed::Yes => Some(text.to_string()),
            Addressed::NameOnly => {
                self.waiting
                    .insert(speaker.to_string(), (now, text.to_string()));
                None
            }
            Addressed::No => follow_up,
        }
    }

    /// A person's history line: context. The speaker is waiting for their
    /// question from `now` if the line is only the name, and not waiting
    /// after any other line.
    pub fn recall(
        &mut self,
        speaker: &str,
        text: &str,
        seq: Option<u64>,
        addressed: Addressed,
        now: Instant,
    ) {
        self.note(Line::person(speaker, text, seq));
        if addressed == Addressed::NameOnly {
            self.waiting
                .insert(speaker.to_string(), (now, text.to_string()));
        } else {
            self.waiting.remove(speaker);
        }
    }

    /// Starts reply number `replies + 1` and copies the history for it.
    pub fn start_reply(&mut self) -> (u64, Snapshot) {
        self.replies += 1;
        let snapshot = Snapshot {
            since_reply: self.history.since_reply().cloned().collect(),
            recent: self.history.recent().cloned().collect(),
        };
        self.history.mark_reply();
        (self.replies, snapshot)
    }

    /// Drops the lines heard since the last reply from the next prompt.
    /// Returns how many there were.
    pub fn discard_pending(&mut self) -> usize {
        let n = self.history.since_reply().count();
        self.history.mark_reply();
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::addressing::addressed;

    fn named(text: &str) -> Addressed {
        addressed(&["pixel".to_string()], false, text)
    }

    const FOLLOW_UP: Duration = Duration::from_secs(8);

    fn hear(room: &mut Room, speaker: &str, text: &str, now: Instant) -> Option<String> {
        room.hear(speaker, text, None, named(text), FOLLOW_UP, now)
    }

    fn recall(room: &mut Room, speaker: &str, text: &str, now: Instant) {
        room.recall(speaker, text, None, named(text), now)
    }

    fn since(room: &Room) -> Vec<String> {
        room.history.since_reply().map(Line::prompt_line).collect()
    }

    #[test]
    fn live_follow_up() {
        let mut room = Room::default();
        let t0 = Instant::now();
        assert_eq!(hear(&mut room, "ana", "Hey Pixel", t0), None);
        // Someone else's line is not the question.
        assert_eq!(hear(&mut room, "ben", "lunch?", t0), None);
        assert_eq!(
            hear(
                &mut room,
                "ana",
                "what time is it?",
                t0 + Duration::from_secs(3)
            )
            .as_deref(),
            Some("Hey Pixel what time is it?")
        );
        // The wait is used up.
        assert_eq!(
            hear(&mut room, "ana", "and then?", t0 + Duration::from_secs(4)),
            None
        );
        assert_eq!(room.history.since_reply().count(), 4);
    }

    #[test]
    fn wait_expired() {
        let mut room = Room::default();
        let t0 = Instant::now();
        assert_eq!(hear(&mut room, "ana", "Hey Pixel", t0), None);
        assert_eq!(
            hear(
                &mut room,
                "ana",
                "what time is it?",
                t0 + Duration::from_secs(9)
            ),
            None
        );
    }

    #[test]
    fn history_restores_waiting() {
        let mut room = Room::default();
        let t0 = Instant::now();
        // Ana's last history line is the bare name; Ben's bare name was
        // followed by another line of his.
        recall(&mut room, "ben", "Hey Pixel", t0);
        recall(&mut room, "ana", "Hey Pixel", t0);
        recall(&mut room, "ben", "never mind", t0);
        // A question in the history leaves nobody waiting.
        recall(&mut room, "cy", "Pixel, what time is it?", t0);
        assert_eq!(room.waiting.len(), 1);
        assert_eq!(
            since(&room),
            [
                "ben: Hey Pixel",
                "ana: Hey Pixel",
                "ben: never mind",
                "cy: Pixel, what time is it?"
            ]
        );

        let later = t0 + Duration::from_secs(2);
        assert_eq!(hear(&mut room, "ben", "what time is it?", later), None);
        assert_eq!(
            hear(&mut room, "ana", "what time is it?", later).as_deref(),
            Some("Hey Pixel what time is it?")
        );
    }

    #[test]
    fn history_wait_runs_from_receipt() {
        let mut room = Room::default();
        let t0 = Instant::now();
        recall(&mut room, "ana", "Hey Pixel", t0);
        let late = t0 + FOLLOW_UP + Duration::from_secs(1);
        assert_eq!(hear(&mut room, "ana", "what time is it?", late), None);
    }

    #[test]
    fn history_is_bounded() {
        let mut room = Room::default();
        for i in 0..MAX_LINES + 5 {
            room.note(Line::person("ana", &i.to_string(), None));
        }
        assert_eq!(room.history.len(), MAX_LINES);
        assert_eq!(room.history.since_reply().count(), MAX_LINES);
        assert_eq!(
            room.history
                .recent()
                .next()
                .map(Line::prompt_line)
                .as_deref(),
            Some("ana: 5")
        );
    }

    #[test]
    fn a_reply_starts_a_new_since_list() {
        let mut room = Room::default();
        room.note(Line::person("ana", "hi", Some(1)));
        room.note(Line::person("ben", "Pixel, time?", Some(2)));
        let (n, snapshot) = room.start_reply();
        assert_eq!(n, 1);
        assert_eq!(snapshot.since_reply.len(), 2);
        assert_eq!(snapshot.before_question().len(), 1);
        room.note(Line::agent("Pixel", "It is noon.", false, None));
        room.note(Line::person("ana", "thanks", Some(4)));
        assert_eq!(since(&room), ["Pixel: It is noon.", "ana: thanks"]);
        let (_, snapshot) = room.start_reply();
        assert_eq!(snapshot.recent.len(), 4);
        assert_eq!(snapshot.since_reply.len(), 2);
        // A finished reply is in the structured history.
        assert_eq!(
            snapshot.records()[2],
            json!({"author": "Pixel", "role": "agent", "text": "It is noon.", "interrupted": false})
        );
    }

    #[test]
    fn discard_drops_pending_only() {
        let mut room = Room::default();
        room.note(Line::person("ana", "one", None));
        room.note(Line::person("ana", "two", None));
        assert_eq!(room.discard_pending(), 2);
        assert_eq!(room.history.since_reply().count(), 0);
        assert_eq!(room.history.len(), 2);
    }

    #[test]
    fn records_are_bounded_and_marked() {
        let long = Line::agent(&"a".repeat(300), &"t".repeat(5000), true, None);
        let record = long.record();
        assert_eq!(record["author"].as_str().unwrap().len(), MAX_AUTHOR_CHARS);
        assert_eq!(record["text"].as_str().unwrap().len(), MAX_TEXT_CHARS);
        assert_eq!(record["interrupted"], true);
        assert_eq!(
            Line::agent("Pixel", "The largest ocean is", true, None).prompt_line(),
            "Pixel: The largest ocean is [interrupted before finishing]"
        );
    }
}
