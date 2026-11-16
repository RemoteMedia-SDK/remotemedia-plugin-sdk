//! The member side of the RemoteMedia agent link, for any assistant that
//! answers people in a room.
//!
//! A backend implements [`Responder`]: given a [`Turn`] (who asked, what,
//! the room's history and a cancel token), it streams its reply into a
//! [`ReplyOut`]. [`RoomAgent`] does the rest:
//!
//! * decodes the link's frames ([`link`]) and names each line's author
//!   ([`attribution`]);
//! * decides which lines are for the agent, including a bare name followed
//!   by the question ([`addressing`]);
//! * keeps one bounded history of the room ([`history`]), rendered as prompt
//!   lines or structured records;
//! * takes one reply at a time, tags it with a reply id unique to the
//!   instance, publishes `thinking`, each sentence ([`text`]) and `idle`, and
//!   stops when the room says so;
//! * implements the generator cancellation contract: a cut-off reply stays in
//!   the history, marked as interrupted, unless the cancel discards it.
//!
//! Settings come from the node's parameters ([`RoomAgentConfig`]).
//! Without `publish: "room"` the agent publishes a pipeline model node's
//! `{"kind": "assistant"}` messages instead of the link's frames.
//!
//! The crate has no loadable ABI, host or model dependency: a plugin wraps a
//! `RoomAgent` with its own factory and export. See `docs/ROOM_AGENT.md`.

pub mod addressing;
pub mod agent;
pub mod attribution;
pub mod config;
pub mod history;
pub mod link;
pub mod text;
pub mod trace;

pub use addressing::Addressed;
pub use agent::{Outcome, ReplyOut, Responder, RoomAgent, Turn};
pub use config::{Publish, RoomAgentConfig};
pub use history::{Line, Role, Room, Snapshot};
pub use text::Sentences;
