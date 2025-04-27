use bytes::{Bytes};
use crate::message::Message;
use crate::error::ProtocolError;

#[derive(Debug)]
pub struct ConsumeResponse {
    pub offset: u64,
    pub message: Message,
}

impl ConsumeResponse {
    pub fn deserialize(buf: Bytes) -> Result<Self, ProtocolError> {
        let (offset, message) = Message::deserialize(&buf)?;
        Ok(ConsumeResponse { offset, message })
    }
}
