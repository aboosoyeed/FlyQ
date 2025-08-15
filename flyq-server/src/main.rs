use crate::server::params::Params;
use anyhow::Result;
use clap::Parser;
use flyQ::core::log_engine::LogEngine;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;
use flyQ::{BROKER_CONFIG, BrokerConfig};

mod runtime;
mod server;
pub mod types;
mod config;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_thread_ids(true)
        .compact()
        .init();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
    let params = Params::parse();
    info!("FlyQ starting with params: {:?}", params);

    let cfg = BrokerConfig::load_or_default(params.config.as_deref())?;
    info!("FlyQ starting with config: {:?}", cfg);

    BROKER_CONFIG.set(cfg).expect("config initialised twice");

    let engine = Arc::new(Mutex::new(LogEngine::load(&params.base_dir).await));

    runtime::run(engine.clone(), shutdown_rx).await;
    tokio::select! {
        _ = server::start(params, engine) => {
            // If the server exits unexpectedly
            let _ = shutdown_tx.send(());
        }
        _ = tokio::signal::ctrl_c() => {
            let _ = shutdown_tx.send(());
        }
    }

    Ok(())
}
