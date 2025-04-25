use anyhow::{Result};

mod types;
mod server;

#[tokio::main]
async fn main() -> Result<()> {
    
    server::start().await

}

