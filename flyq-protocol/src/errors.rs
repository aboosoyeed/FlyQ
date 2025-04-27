use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("Unknown opcode: {0}")]
    UnknownOpCode(u8),

    #[error("Incomplete frame")]
    IncompleteFrame,

    #[error("Payload decode error: {0}")]
    PayloadError(String),

    #[error("Unknown frame type: {0}")]
    UnknownFrameType(u8),

    #[error("Checksum Mismatch expected: {expected} found: {found} ")]
    ChecksumMismatch { expected: u32, found: u32 },
}

#[derive(Debug, Error)]
pub enum DeserializeError {
    #[error("Unexpected end of input")]
    UnexpectedEOF,

    #[error("Invalid UTF-8 in message")]
    InvalidUtf8,

    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    #[error("Offset {0} not found in any segment")]
    OffsetNotFound(u64),
}