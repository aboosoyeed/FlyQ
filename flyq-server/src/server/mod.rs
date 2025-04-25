use crate::server::config::Config;

mod config;
mod listener;

use clap::Parser;
use tracing::info;

pub async fn start() -> anyhow::Result<()>{
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(false)
        .with_thread_ids(true)
        .compact()
        .init();

    let config = Config::parse();
    info!("FlyQ starting with config: {:?}", config);
    listener::start(config).await
}