use anyhow::{Result, Context};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(false)
        .with_thread_ids(true)
        .compact()
        .init();

    let listener = TcpListener::bind("127.0.0.1:9092").await.context("Failed to bind TCP listener")?;

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
    let mut buf = [0u8; 1024];

    // Just echo back for now
    let n = stream.read(&mut buf).await.context("Failed to read from stream")?;
    if n == 0 {
        return Ok(()); // client disconnected
    }

    println!("Received: {:?}", &buf[..n]);

    stream.write_all(&buf[..n])
        .await
        .context("Failed to write response")?;

    Ok(())
}
