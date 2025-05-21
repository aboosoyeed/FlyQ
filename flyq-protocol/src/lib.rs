pub mod frame;
pub mod payload;
pub mod message;
pub mod errors;
mod request;
mod response;
mod op_code;
mod utils;


// Public re-exports for easy access
pub use frame::{Frame, FrameType};
pub use payload::{RequestPayload, ResponsePayload};
pub use message::Message;
pub use errors::ProtocolError;

// Re-export common requests/responses
pub use request::{ProduceRequest, ConsumeRequest, ConsumeWithGroupRequest, CommitOffsetRequest};
pub use response::{ProduceAck, ConsumeResponse};

pub use op_code::{OpCode};

pub use utils::read_bytes;