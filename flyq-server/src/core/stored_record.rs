use flyq_protocol::errors::DeserializeError;
use flyq_protocol::{read_bytes, Message};

/// Represents a message stored in a segment file, including its offset.
#[derive(Debug, Clone)]
pub struct StoredRecord {
    pub offset: u64,
    pub message: Message,
}

impl StoredRecord {
    /// Serializes the record for writing to disk.
    ///
    /// Layout:
    /// [ record_len: u32 ]
    /// [ offset      : u64 ]
    /// [ message     : bytes from Message::serialize_for_disk() ]
    pub fn serialize(&self) -> Vec<u8> {
        let message_bytes = self.message.serialize_for_wire();
        let total_len = 8 + message_bytes.len(); // offset (8) + message content

        let mut buf = Vec::with_capacity(4 + total_len);
        buf.extend_from_slice(&(total_len as u32).to_be_bytes()); // record length prefix
        buf.extend_from_slice(&self.offset.to_be_bytes());        // offset
        buf.extend_from_slice(&message_bytes);                    // message
        buf
    }

    /// Deserializes a StoredRecord from a buffer (excluding the initial length prefix).
    ///
    /// Expects buffer to start with:
    /// [ offset: u64 ]
    /// [ message bytes... ]
    pub fn deserialize(mut buf: &[u8]) -> Result<Self, DeserializeError> {
        // offset
        let offset_bytes = read_bytes(&mut buf, 8)?;
        let offset = u64::from_be_bytes(offset_bytes.try_into().unwrap());

        // message
        let message = Message::deserialize_body(buf)?;

        Ok(Self { offset, message })
    }
}
