use crate::server::config::Config;

pub(crate) mod config;
mod listener;

use tracing::info;
use crate::types::SharedLogEngine;

pub async fn start(config:Config, engine:SharedLogEngine) -> anyhow::Result<()>{
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(false)
        .with_thread_ids(true)
        .compact()
        .init();

    info!("FlyQ starting with config: {:?}", config);
    listener::start(config, engine).await
}