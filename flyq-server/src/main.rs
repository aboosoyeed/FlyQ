use anyhow::{Result, Context};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use bytes::BytesMut;
use flyq_protocol::{Frame, OpCode};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(false)
        .with_thread_ids(true)
        .compact()
        .init();

    let listener = TcpListener::bind("0.0.0.0:9092").await.context("Failed to bind TCP listener")?;

    loop{
        let (socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket).await {
                eprintln!("Connection error: {:?}", e);
            }
        });

    }

}

async fn handle_connection(mut stream: TcpStream) -> Result<()> {
    let mut buf = BytesMut::with_capacity(1024);


    loop {
        buf.reserve(1024);
        let _ = match stream.read_buf(&mut buf).await {
            Ok(0) => return Ok(()),  // EOF
            Ok(n) => n,
            Err(e) => return Err(e.into()),
        };
        
        while let Some(frame) = Frame::decode(&mut buf)?{
            let response = handle_frame(frame).await?;
            stream.write_all(&response).await?;
            stream.flush().await?;
        }
    }

}

async fn handle_frame(frame: Frame)->Result<Vec<u8>>{
    match frame.op {
        OpCode::Produce => {
            let (_, response) = parse_produce(&frame.payload)?;
            Ok(response)
        },
        OpCode::Consume => {
            let (_, response) = parse_consume(&frame.payload)?;
            Ok(response.to_be_bytes().to_vec())
        }
    }
}

fn parse_produce(buf:&[u8]) -> Result<(String, Vec<u8>)>{
    let tpos = buf.iter().position(|&x|x==0).ok_or_else(||anyhow::anyhow!("missing topic separator"))?;
    let topic = std::str::from_utf8(&buf[..tpos])?.to_string();
    let message = buf[tpos+1..].to_vec();
    
    println!("{:?}", String::from_utf8(message.clone()));
    Ok((topic, message))
}

fn parse_consume(buf: &[u8]) -> Result<(String, u64)> {
    let tpos = buf.iter().position(|&b| b == 0)
        .ok_or_else(|| anyhow::anyhow!("missing topic separator"))?;

    let topic = std::str::from_utf8(&buf[..tpos])?.to_string();

    if buf.len() < tpos + 1 + 8 {
        anyhow::bail!("payload too short for u64 offset");
    }

    let offset = u64::from_be_bytes(buf[tpos + 1..tpos + 9].try_into()?);

    Ok((topic, offset))
}
