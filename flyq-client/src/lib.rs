// flyq-client/src/lib.rs

use tokio::net::TcpStream;
use tokio::io::{AsyncWriteExt, AsyncReadExt};
use flyq_protocol::{Frame, OpCode};
use anyhow::{Result, Context};

pub struct FlyqClient {
    stream: TcpStream,
}

impl FlyqClient {
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr)
            .await
            .context("Failed to connect to FlyQ server")?;

        Ok(FlyqClient { stream })
    }

    pub async fn produce(&mut self, topic: &str, message: &[u8]) -> Result<()> {
        let mut payload = Vec::new();
        payload.extend_from_slice(topic.as_bytes());
        payload.push(0); // null separator
        payload.extend_from_slice(message);

        let frame = Frame {
            op: OpCode::Produce,
            payload,
        };

        let mut buf = bytes::BytesMut::new();
        frame.encode(&mut buf);
        self.stream.write_all(&buf).await?;

        Ok(())
    }

    pub async fn consume(&mut self, topic: &str, offset: u64) -> Result<Vec<u8>> {
        let mut payload = Vec::new();
        payload.extend_from_slice(topic.as_bytes());
        payload.push(0);
        payload.extend_from_slice(&offset.to_be_bytes());

        let frame = Frame {
            op: OpCode::Consume,
            payload,
        };

        let mut buf = bytes::BytesMut::new();
        frame.encode(&mut buf);
        self.stream.write_all(&buf).await?;

        // just receive raw response for now
        let mut response_buf = bytes::BytesMut::with_capacity(1024);
        let mut tmp = [0u8; 1024];
        let n = self.stream.read(&mut tmp).await?;
        response_buf.extend_from_slice(&tmp[..n]);

        if let Some(frame) = Frame::decode(&mut response_buf)? {
            Ok(frame.payload)
        } else {
            Err(anyhow::anyhow!("Incomplete frame received"))
        }
    }
}
