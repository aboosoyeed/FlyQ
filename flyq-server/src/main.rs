use crate::server::params::Params;
use anyhow::Result;
use clap::Parser;
use flyQ::core::log_engine::LogEngine;
use std::sync::Arc;
use tokio::sync::Mutex;

mod runtime;
mod server;
pub mod types;
#[tokio::main]
async fn main() -> Result<()> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
    let params = Params::parse();
    let engine = Arc::new(Mutex::new(LogEngine::load(&params.base_dir).await));

    runtime::run(engine.clone(), shutdown_rx).await;
    tokio::select! {
          _ = server::start(params, engine)=>{
            // If the server exits unexpectedly
            let _ = shutdown_tx.send(());
        }
        _ = tokio::signal::ctrl_c()=>{
            let _ = shutdown_tx.send(());
        }

    }

    Ok(())
}
