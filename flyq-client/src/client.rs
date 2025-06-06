use anyhow::Context;
use bytes::{Bytes, BytesMut};
use flyq_protocol::{
    CommitOffsetRequest, ConsumeRequest, ConsumeResponse, ConsumeWithGroupRequest, Frame,
    FrameType, Message, OpCode, ProduceAck, ProduceRequest, ProtocolError, RequestPayload,
    ResponsePayload, WatermarkRequest, WatermarkResponse,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct FlyqClient {
    stream: TcpStream,
    correlation_id: u32,
}

impl FlyqClient {
    pub async fn connect(addr: &str) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .context("Failed to connect to FlyQ server")?;

        Ok(FlyqClient {
            stream,
            correlation_id: 0,
        })
    }

    async fn send_request(&mut self, payload: RequestPayload) -> Result<(), ProtocolError> {
        self.correlation_id = self.correlation_id.wrapping_add(1);

        let frame = Frame {
            version: 1,
            frame_type: FrameType::Request,
            correlation_id: self.correlation_id,
            payload: Vec::from(payload.serialize()),
        };

        let mut buf = BytesMut::new();
        frame.encode(&mut buf);
        self.stream
            .write_all(&buf)
            .await
            .map_err(ProtocolError::IoError)?;
        Ok(())
    }

    async fn read_response(&mut self) -> Result<Frame, ProtocolError> {
        let mut buf = BytesMut::with_capacity(4096);
        self.stream
            .read_buf(&mut buf)
            .await
            .map_err(ProtocolError::IoError)?;

        Frame::decode(&mut buf)?.ok_or(ProtocolError::IncompleteFrame)
    }

    pub async fn produce(
        &mut self,
        topic: &str,
        payload: &[u8],
    ) -> Result<ProduceAck, ProtocolError> {
        let req = ProduceRequest {
            topic: topic.to_string(),
            message: Bytes::copy_from_slice(payload),
        };
        let payload = RequestPayload {
            op_code: OpCode::Produce,
            data: req.serialize(),
        };

        self.send_request(payload).await?;

        let response = self.read_response().await?;
        let resp_payload = ResponsePayload::deserialize(Bytes::from(response.payload))?;

        if resp_payload.op_code != OpCode::Produce {
            return Err(ProtocolError::UnknownOpCode(resp_payload.op_code as u8));
        }

        let ack = ProduceAck::deserialize(resp_payload.data)?;
        Ok(ack)
    }

    pub async fn consume(
        &mut self,
        topic: &str,
        offset: u64,
    ) -> Result<Option<ConsumeResponse>, ProtocolError> {
        let req = ConsumeRequest {
            topic: topic.to_string(),
            partition: 0, // Hardcoded for now
            offset,
        };

        let payload = RequestPayload {
            op_code: OpCode::Consume,
            data: req.serialize(),
        };

        self.send_request(payload).await?;

        let response = self.read_response().await?;
        let resp_payload = ResponsePayload::deserialize(Bytes::from(response.payload))?;

        if resp_payload.op_code != OpCode::Consume {
            return Err(ProtocolError::UnknownOpCode(resp_payload.op_code as u8));
        }

        if resp_payload.data.is_empty() {
            return Ok(None);
        }

        let consume = ConsumeResponse::deserialize(resp_payload.data)?;
        Ok(Some(consume))
    }

    pub async fn consume_with_group(
        &mut self,
        topic: &str,
        partition: u32,
        group: &str,
    ) -> Result<Option<ConsumeResponse>, ProtocolError> {
        let req = ConsumeWithGroupRequest {
            topic: topic.to_string(),
            partition,
            group: group.to_string(),
        };

        let payload = RequestPayload {
            op_code: OpCode::ConsumeWithGroup,
            data: req.serialize(),
        };

        self.send_request(payload).await?;
        let response = self.read_response().await?;

        let resp_payload = ResponsePayload::deserialize(Bytes::from(response.payload))?;

        if resp_payload.op_code != OpCode::ConsumeWithGroup {
            return Err(ProtocolError::UnknownOpCode(resp_payload.op_code as u8));
        }

        if resp_payload.data.is_empty() {
            return Ok(None);
        }

        let consume = ConsumeResponse::deserialize(resp_payload.data)?;
        Ok(Some(consume))
    }

    pub async fn commit_offset(
        &mut self,
        topic: &str,
        partition: u32,
        group: &str,
        offset: u64,
    ) -> Result<(), ProtocolError> {
        let req = CommitOffsetRequest {
            topic: topic.to_string(),
            partition,
            group: group.to_string(),
            offset,
        };
        let payload = RequestPayload {
            op_code: OpCode::CommitOffset,
            data: req.serialize(),
        };

        self.send_request(payload).await?;
        let response = self.read_response().await?;
        let resp_payload = ResponsePayload::deserialize(Bytes::from(response.payload))?;
        if resp_payload.op_code != OpCode::CommitOffset {
            return Err(ProtocolError::UnknownOpCode(resp_payload.op_code as u8));
        }

        Ok(())
    }

    // Consume a message from a specified partition at a specific offset.
    pub async fn consume_from_partition(
        &mut self,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> anyhow::Result<Option<(u32, u64, Message)>> {
        todo!()
    }

    /// Consume a message by key — partitions are inferred using a hash function.
    /// The key must match the partitioning logic on the server.
    pub async fn consume_by_key(
        &mut self,
        topic: &str,
        key: &[u8],
        offset: u64,
    ) -> anyhow::Result<Option<(u32, u64, Message)>> {
        todo!()
    }

    /// Stream-style consume: fetch the next available message from a topic-partition.
    pub async fn consume_next(
        &mut self,
        topic: &str,
        partition: u32,
        last_seen_offset: u64,
    ) -> anyhow::Result<Option<(u32, u64, Message)>> {
        todo!()
    }

    pub async fn get_watermarks(
        &mut self,
        topic: &str,
        partition: u32,
    ) -> Result<Option<WatermarkResponse>, ProtocolError> {
        let req = WatermarkRequest {
            topic: topic.to_string(),
            partition,
        };
        let payload = RequestPayload {
            op_code: OpCode::Watermark,
            data: req.serialize(),
        };
        self.send_request(payload).await?;
        let response = self.read_response().await?;
        let resp_payload = ResponsePayload::deserialize(Bytes::from(response.payload))?;
        if resp_payload.op_code != OpCode::Watermark {
            return Err(ProtocolError::UnknownOpCode(resp_payload.op_code as u8));
        }
        let watermark = WatermarkResponse::deserialize(resp_payload.data)?;
        Ok(Some(watermark))
    }
}
