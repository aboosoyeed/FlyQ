use crate::server::config::Config;
use anyhow::Result;
use clap::Parser;
use flyQ::core::log_engine::LogEngine;
use std::sync::Arc;
use tokio::sync::Mutex;

mod runtime;
mod server;
mod types;
#[tokio::main]
async fn main() -> Result<()> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
    let config = Config::parse();
    let engine = Arc::new(Mutex::new(LogEngine::load(&config.base_dir)));

    runtime::run(engine.clone(), shutdown_rx).await;
    tokio::select! {
          _ = server::start(config, engine)=>{
            // If the server exits unexpectedly
            let _ = shutdown_tx.send(());
        }
        _ = tokio::signal::ctrl_c()=>{
            let _ = shutdown_tx.send(());
        }

    }

    Ok(())
}
