use std::io;
use thiserror::Error;
use flyq_protocol::errors::DeserializeError;


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
