use thiserror::Error;

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
