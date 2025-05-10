use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use bytes::{Bytes, BytesMut};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tracing::{debug, info};
use flyQ::core::log_engine::LogEngine;
use flyq_protocol::{ConsumeRequest, ConsumeResponse, Frame, FrameType, OpCode, ProduceAck, ProduceRequest, ProtocolError, RequestPayload, ResponsePayload};
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
    let mut buf = BytesMut::with_capacity(4096);


    loop {

        let n = stream.read_buf(&mut buf).await.map_err(ProtocolError::IoError)?;
         if n == 0{
             return Ok(())
         }

        while let Some(frame) = Frame::decode(&mut buf)?{
            if frame.frame_type != FrameType::Request{
                // Todo: handle non request frames
                continue;
            }

            let request_payload = RequestPayload::deserialize(Bytes::from(frame.payload))?;
            let response_payload = dispatch_request(request_payload, &engine).await?;

            let response_frame = Frame {
                version: 1,
                frame_type: FrameType::Response,
                correlation_id: frame.correlation_id,
                payload: Vec::from(response_payload.serialize()),
            };

            let mut out = BytesMut::new();
            response_frame.encode(&mut out);
            stream.write_all(&out).await?;
            stream.flush().await?;
        }
    }

}

async fn dispatch_request(request: RequestPayload, engine: &SharedLogEngine) -> Result<ResponsePayload, ProtocolError> {
    match request.op_code {
        OpCode::Produce => handle_produce(request.data, engine).await,
        OpCode::Consume => handle_consume(request.data, engine).await,
    }
}

async fn handle_produce(data: Bytes, engine: &SharedLogEngine) -> Result<ResponsePayload, ProtocolError> {
    let produce_req = ProduceRequest::deserialize(data)?;
    let message = Message {
        key: None,
        value: produce_req.message.to_vec(),
        timestamp: chrono::Utc::now().timestamp_millis() as u64,
        headers: None,
    };
    
    //println!("{}", message.clone().serialize().len());
    let (partition, offset) = engine.lock().await.produce(&produce_req.topic, message).map_err(ProtocolError::IoError)?;

    let ack = ProduceAck {
        partition,
        offset,
    };

    Ok(ResponsePayload {
        op_code: OpCode::Produce,
        data: ack.serialize(),
    })
}

async fn handle_consume(data:Bytes, engine: &SharedLogEngine)->Result<ResponsePayload, ProtocolError>{
    let consume_req = ConsumeRequest::deserialize(data)?;
    let maybe_msg  = engine.lock().await.consume(&consume_req.topic, 0, consume_req.offset).map_err(|e|ProtocolError::EngineErrorMapped(e.to_string()))?;
    if let Some(msg) = maybe_msg{
        let resp = ConsumeResponse{
            offset: consume_req.offset,
            message: msg,
        };
        Ok(ResponsePayload {
            op_code: OpCode::Consume,
            data: resp.serialize(), 
        })
        
    }else { 
        Ok(ResponsePayload{
            op_code: OpCode::Consume,
            data:Bytes::new(), // Empty payload means no message found
        })
    }
}


