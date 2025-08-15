pub mod errors;
pub mod frame;
pub mod message;
mod op_code;
pub mod payload;
mod request;
mod response;
mod utils;

// Public re-exports for easy access
pub use errors::ProtocolError;
pub use frame::{Frame, FrameType};
pub use message::Message;
pub use payload::{RequestPayload, ResponsePayload};

// Re-export common requests/responses
pub use request::{
    CommitOffsetRequest, ConsumeRequest, ConsumeWithGroupRequest, ConsumerLagRequest,
    PartitionHealthRequest, ProduceRequest, WatermarkRequest,
};
pub use response::{
    ConsumerLagResponse, ConsumeResponse, PartitionHealthResponse, PartitionLag, ProduceAck,
    WatermarkResponse,
};

pub use op_code::OpCode;

pub use utils::read_bytes;
