use remotemedia_shared_memory::{
    BufferDescriptor, MessageDescriptor, PeerId, SessionId, WriteDescriptor,
};
use remotemedia_types::RuntimeMetadata;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    Hello(SessionId, PeerId),
    Input(MessageDescriptor),
    Produce(WriteDescriptor),
    ReleaseInput,
    ReleaseOutput,
    ReleaseOutputAsync,
    ForgedGrant(u64),
    Echo(Vec<u8>),
    Stop,
    Crash,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum Event {
    Hello(SessionId, PeerId),
    Accepted,
    Published(WriteDescriptor, RuntimeMetadata),
    InputReleased(BufferDescriptor),
    OutputReleased(WriteDescriptor),
    Echo(Vec<u8>),
}
