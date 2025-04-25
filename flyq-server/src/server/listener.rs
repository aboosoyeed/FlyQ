use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use bytes::BytesMut;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tracing::{debug, info};
use flyQ::core::log_engine::LogEngine;
use flyq_protocol::{Frame, OpCode};
use flyq_protocol::message::Message;
use crate::types::SharedLogEngine;
use anyhow::{Result, Context};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use crate::server::config::Config;

pub async fn start(config:Config) -> Result<()>{

    let engine = Arc::new(Mutex::new(LogEngine::load(config.base_dir)));
    info!("log engine initiated");
    let addr = format!("0.0.0.0:{}", config.port);
    let listener = TcpListener::bind(addr).await.context("Failed to bind TCP listener")?;
    info!("server initiated");
    loop{
        let (socket, _) = listener.accept().await?;
        let engine = engine.clone();
        debug!("new incoming connection");
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, engine).await {
                eprintln!("Connection error: {:?}", e);
            }
        });

    }

}

async fn handle_connection(mut stream: TcpStream, engine: SharedLogEngine) -> anyhow::Result<()> {
    let mut buf = BytesMut::with_capacity(1024);


    loop {
        buf.reserve(1024);
        let _ = match stream.read_buf(&mut buf).await {
            Ok(0) => return Ok(()),  // EOF
            Ok(n) => n,
            Err(e) => return Err(e.into()),
        };

        while let Some(frame) = Frame::decode(&mut buf)?{
            let response = handle_frame(&frame, &engine).await?;
            let mut out = BytesMut::new();
            Frame{
                op: frame.op,
                payload: response,
            }.encode(&mut out);
            stream.write_all(&out).await?;
            stream.flush().await?;
        }
    }

}

async fn handle_frame(frame: &Frame, engine: &SharedLogEngine) -> anyhow::Result<Vec<u8>> {
    match frame.op {
        OpCode::Produce => {
            let (topic, message) = parse_produce(&frame.payload).await?;
            let (partition_id, offset) = engine.lock().await.produce(&topic, message)?;
            let mut response = Vec::with_capacity(12);
            response.extend_from_slice(&partition_id.to_be_bytes());
            response.extend_from_slice(&offset.to_be_bytes());
            Ok(response)
        },
        OpCode::Consume => {
            let (topic, offset) = parse_consume(&frame.payload)?;
            let maybe_msg  = engine.lock().await.consume(&topic, 0, offset)?;
            if let Some(msg) = maybe_msg {
                Ok(msg.value) // for now just send back value
            } else {
                Ok(Vec::new()) // return empty payload if not found
            }
        }
    }
}

async fn parse_produce(buf:&[u8]) -> anyhow::Result<(String, Message)> {
    let tpos = buf.iter().position(|&x|x==0).ok_or_else(||anyhow::anyhow!("missing topic separator"))?;
    let topic = std::str::from_utf8(&buf[..tpos])?.to_string();
    let message = buf[tpos+1..].to_vec();
    let message = Message {
        key: None,
        value: message,
        timestamp: SystemTime::now()
            .duration_since(UNIX_EPOCH)?
            .as_millis() as u64,
        headers: None,
    };

    Ok((topic, message))
}

fn parse_consume(buf: &[u8]) -> anyhow::Result<(String, u64)> {
    let tpos = buf.iter().position(|&b| b == 0)
        .ok_or_else(|| anyhow::anyhow!("missing topic separator"))?;

    let topic = std::str::from_utf8(&buf[..tpos])?.to_string();

    if buf.len() < tpos + 1 + 8 {
        anyhow::bail!("payload too short for u64 offset");
    }

    let offset = u64::from_be_bytes(buf[tpos + 1..tpos + 9].try_into()?);

    Ok((topic, offset))
}
