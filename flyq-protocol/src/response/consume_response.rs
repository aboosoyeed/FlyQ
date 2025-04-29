use bytes::{Bytes};
use crate::message::Message;
use crate::errors::ProtocolError;

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

    pub fn serialize(&self) -> Bytes {
        let raw = self.message.serialize(self.offset);
        let len = u32::from_be_bytes(raw[0..4].try_into().unwrap()) as usize;
        Bytes::copy_from_slice(&raw[4..4 + len]) // ✅ Skip msg_len prefix
    }
}
