/*
Coordinates background behaviors during live execution

Orchestrates time-based, scheduled, or reactive behavior

Bridges core logic (storage, indexing) with operational behavior (flush, retention, health)
*/
use std::time::Duration;
use tokio::sync::watch::Receiver;
use crate::types::SharedLogEngine;

mod flush;

pub async fn run(engine:SharedLogEngine ,shutdown_rx:Receiver<()>){
    let store = engine.lock().await.offset_tracker_handle();
    
    tokio::spawn(flush::run_periodic_offset_flush(
        store,
        shutdown_rx.clone(),
        Duration::from_secs(5), // Todo: determine the optimal duration. should it come from config?
    ));
}