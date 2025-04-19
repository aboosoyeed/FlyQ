use std::io;
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

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("Topic does not exist")]
    NoTopic,

    #[error("Partition does not exist")]
    NoPartition,

    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Deserialization error: {0}")]
    Deserialize(#[from] DeserializeError),

    #[error("Unexpected engine error: {0}")]
    Other(String),
}
