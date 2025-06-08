use crate::server::params::Params;

pub(crate) mod params;
mod listener;
mod config;

use tracing::info;
use crate::types::SharedLogEngine;

pub async fn start(config: Params, engine:SharedLogEngine) -> anyhow::Result<()>{
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .with_thread_ids(true)
        .compact()
        .init();

    info!("FlyQ starting with config: {:?}", config);
    listener::start(config, engine).await
}