/*
Coordinates background behaviors during live execution

Orchestrates time-based, scheduled, or reactive behavior

Bridges core logic (storage, indexing) with operational behavior (flush, retention, health)
*/
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch::Receiver;
use crate::types::SharedLogEngine;
use flyQ::broker_config;

mod flush;

pub async fn run(engine:SharedLogEngine ,shutdown_rx:Receiver<()>){
    let cfg = broker_config();
    
    // 1. Offset flush
    let store = engine.lock().await.offset_tracker_handle();
    tokio::spawn(flush::run_periodic_offset_flush(
        store,
        shutdown_rx.clone(),
        Duration::from_secs(5), // Todo: determine the optimal duration. should it come from config?
    ));

    // 2. Metadata flush (per partition)
    let engine_clone_meta = Arc::clone(&engine);
    tokio::spawn(flush::run_periodic_metadata_flush(
        engine_clone_meta,
        shutdown_rx.clone(),
        Duration::from_secs(5),
    ));

    // 3. Segment cleanup (retention)
    let engine_clone_cleanup = Arc::clone(&engine);
    tokio::spawn(flush::run_periodic_cleanup(
        engine_clone_cleanup,
        shutdown_rx,
        cfg.cleanup_interval,
    ));
}