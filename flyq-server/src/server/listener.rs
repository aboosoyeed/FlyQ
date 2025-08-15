use crate::server::params::Params;
use crate::types::SharedLogEngine;
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use flyq_protocol::message::Message;
use flyq_protocol::{
    CommitOffsetRequest, ConsumerLagRequest, ConsumerLagResponse, ConsumeRequest, ConsumeResponse,
    ConsumeWithGroupRequest, Frame, FrameType, OpCode, PartitionHealthRequest,
    PartitionHealthResponse, PartitionLag, ProduceAck, ProduceRequest, ProtocolError,
    RequestPayload, ResponsePayload, WatermarkRequest, WatermarkResponse,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info};

pub async fn start(params: Params, engine: SharedLogEngine) -> Result<()> {
    info!("log engine initiated");
    let addr = format!("0.0.0.0:{}", params.port);
    let listener = TcpListener::bind(addr)
        .await
        .context("Failed to bind TCP listener")?;
    info!("server initiated");
    loop {
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
        let n = stream
            .read_buf(&mut buf)
            .await
            .map_err(ProtocolError::IoError)?;
        if n == 0 {
            return Ok(());
        }

        while let Some(frame) = Frame::decode(&mut buf)? {
            if frame.frame_type != FrameType::Request {
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

async fn dispatch_request(
    request: RequestPayload,
    engine: &SharedLogEngine,
) -> Result<ResponsePayload, ProtocolError> {
    match request.op_code {
        OpCode::Produce => handle_produce(request.data, engine).await,
        OpCode::Consume => handle_consume(request.data, engine).await,
        OpCode::ConsumeWithGroup => handle_consume_with_group(request.data, engine).await,
        OpCode::CommitOffset => handle_commit_offset(request.data, engine).await,
        OpCode::Watermark => handle_watermark(request.data, engine).await,
        OpCode::GetConsumerLag => handle_consumer_lag(request.data, engine).await,
        OpCode::GetPartitionHealth => handle_partition_health(request.data, engine).await,
    }
}

async fn handle_produce(
    data: Bytes,
    engine: &SharedLogEngine,
) -> Result<ResponsePayload, ProtocolError> {
    let produce_req = ProduceRequest::deserialize(data)?;
    let message = Message {
        key: None,
        value: produce_req.message.to_vec(),
        timestamp: chrono::Utc::now().timestamp_millis() as u64,
        headers: None,
    };

    //println!("{}", message.clone().serialize().len());
    let (partition, offset) = engine
        .lock()
        .await
        .produce(&produce_req.topic, message)
        .await
        .map_err(ProtocolError::IoError)?;

    let ack = ProduceAck { partition, offset };

    Ok(ResponsePayload {
        op_code: OpCode::Produce,
        data: ack.serialize(),
    })
}

async fn handle_consume(
    data: Bytes,
    engine: &SharedLogEngine,
) -> Result<ResponsePayload, ProtocolError> {
    let consume_req = ConsumeRequest::deserialize(data)?;
    let maybe_msg = engine
        .lock()
        .await
        .consume(&consume_req.topic, 0, consume_req.offset)
        .await
        .map_err(|e| ProtocolError::EngineErrorMapped(e.to_string()))?;
    if let Some(msg) = maybe_msg {
        let resp = ConsumeResponse {
            offset: consume_req.offset,
            message: msg,
        };
        Ok(ResponsePayload {
            op_code: OpCode::Consume,
            data: resp.serialize(),
        })
    } else {
        Ok(ResponsePayload {
            op_code: OpCode::Consume,
            data: Bytes::new(), // Empty payload means no message found
        })
    }
}

async fn handle_consume_with_group(
    data: Bytes,
    engine: &SharedLogEngine,
) -> Result<ResponsePayload, ProtocolError> {
    let consume_req = ConsumeWithGroupRequest::deserialize(data)?;
    let maybe_msg = engine
        .lock()
        .await
        .consume_with_group(
            &consume_req.topic,
            consume_req.partition,
            &consume_req.group,
        )
        .await
        .map_err(|e| ProtocolError::EngineErrorMapped(e.to_string()))?;

    debug!(
        "consume_with_group for topic={}, partition={}, group={} => {:?}",
        consume_req.topic,
        consume_req.partition,
        consume_req.group,
        maybe_msg.as_ref().map(|(o, _)| o)
    );

    if let Some((offset, msg)) = maybe_msg {
        let resp = ConsumeResponse {
            offset,
            message: msg,
        };
        Ok(ResponsePayload {
            op_code: OpCode::ConsumeWithGroup,
            data: resp.serialize(),
        })
    } else {
        Ok(ResponsePayload {
            op_code: OpCode::ConsumeWithGroup,
            data: Bytes::new(), // Empty payload means no message found
        })
    }
}

async fn handle_commit_offset(
    data: Bytes,
    engine: &SharedLogEngine,
) -> Result<ResponsePayload, ProtocolError> {
    let req = CommitOffsetRequest::deserialize(data)?;
    engine
        .lock()
        .await
        .commit_offset(&req.topic, req.partition, &req.group, req.offset)
        .await
        .map_err(|e| ProtocolError::EngineErrorMapped(e.to_string()))?;

    debug!(
        "Committed offset={} for topic={}, partition={}, group={}",
        req.offset, req.topic, req.partition, req.group
    );

    Ok(ResponsePayload {
        op_code: OpCode::CommitOffset,
        data: Bytes::new(),
    })
}

async fn handle_watermark(
    data: Bytes,
    engine: &SharedLogEngine,
) -> Result<ResponsePayload, ProtocolError> {
    let req = WatermarkRequest::deserialize(data)?;
    let w = engine
        .lock()
        .await
        .get_watermark(&req.topic, req.partition)
        .await
        .map_err(|e| ProtocolError::EngineErrorMapped(e.to_string()))?;
    let resp = WatermarkResponse {
        low_watermark: w.0,
        high_watermark: w.1,
        log_end_offset: w.2,
    };
    Ok(ResponsePayload {
        op_code: OpCode::Watermark,
        data: resp.serialize(),
    })
}

async fn handle_consumer_lag(
    data: Bytes,
    engine: &SharedLogEngine,
) -> Result<ResponsePayload, ProtocolError> {
    let req = ConsumerLagRequest::deserialize(data)?;
    let (total_lag, partition_lags) = engine
        .lock()
        .await
        .get_consumer_lag(&req.consumer_group, req.topics)
        .await
        .map_err(|e| ProtocolError::EngineErrorMapped(e.to_string()))?;
    
    let partitions = partition_lags
        .into_iter()
        .map(|(topic, partition, committed_offset, high_watermark, lag)| PartitionLag {
            topic,
            partition,
            committed_offset,
            high_watermark,
            lag,
        })
        .collect();
    
    let resp = ConsumerLagResponse {
        consumer_group: req.consumer_group,
        total_lag,
        partitions,
    };
    
    Ok(ResponsePayload {
        op_code: OpCode::GetConsumerLag,
        data: resp.serialize(),
    })
}

async fn handle_partition_health(
    data: Bytes,
    engine: &SharedLogEngine,
) -> Result<ResponsePayload, ProtocolError> {
    let req = PartitionHealthRequest::deserialize(data)?;
    let (segment_count, total_size_bytes, low_watermark, high_watermark, log_end_offset, last_cleanup) =
        engine
            .lock()
            .await
            .get_partition_health(&req.topic, req.partition)
            .await
            .map_err(|e| ProtocolError::EngineErrorMapped(e.to_string()))?;
    
    let resp = PartitionHealthResponse {
        topic: req.topic,
        partition: req.partition,
        segment_count,
        total_size_bytes,
        low_watermark,
        high_watermark,
        log_end_offset,
        last_cleanup,
    };
    
    Ok(ResponsePayload {
        op_code: OpCode::GetPartitionHealth,
        data: resp.serialize(),
    })
}
