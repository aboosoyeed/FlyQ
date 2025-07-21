use crate::server::params::Params;

pub(crate) mod params;
mod listener;

use crate::types::SharedLogEngine;

pub async fn start(params: Params, engine:SharedLogEngine) -> anyhow::Result<()>{
    listener::start(params, engine).await
}