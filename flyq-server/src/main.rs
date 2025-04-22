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
        let (socket, addr) = listener.accept().await?;

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
        let mut temp = [0u8; 1024];
        let n = stream.read(&mut temp).await.context("Failed to read from stream")?;
        if n == 0 {
            return Ok(()); // client disconnected
        }

        while let Some(frame) = Frame::decode(&mut buf)?{
            handle_frame(frame).await;
        }
    }

    Ok(())
}

async fn handle_frame(frame: Frame){
    match frame.op {
        OpCode::Produce => {

        },
        OpCode::Consume => {

        }
    }
}

fn parse_produce(buf:&[u8]) -> Result<(String, Vec<u8>)>{
    let tpos = buf.iter().position(|&x|x==0).ok_or_else(||anyhow::anyhow!("missing topic seperator"))?;
    let topic = std::str::from_utf8(&buf[..tpos])?.to_string();
    let message = buf[tpos+1..].to_vec();
    Ok((topic, message))
}
