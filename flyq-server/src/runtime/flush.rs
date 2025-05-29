use flyQ::core::offset_tracker::OffsetTracker;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::watch::Receiver;
use tokio::sync::Mutex;
use crate::types::SharedLogEngine;

pub async fn run_periodic_offset_flush(
    store: Arc<Mutex<OffsetTracker>>,
    mut shutdown_rx: Receiver<()>,
    interval: Duration,
) {
    let mut ticker = tokio::time::interval(interval);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let mut store = store.lock().await;
                if let Err(e) = store.flush_dirty_offsets() {
                    tracing::error!("Offset flush failed: {:?}", e);
                } else {
                    tracing::debug!("Offset flush completed.");
                }
            }

            _ = shutdown_rx.changed() => {
                tracing::info!("Shutdown signal received. Flushing before exit...");
                let mut store = store.lock().await;
                if let Err(e) = store.flush_dirty_offsets() {
                    tracing::error!("Final offset flush failed: {:?}", e);
                } else {
                    tracing::info!("Final offset flush completed.");
                }
                break;
            }
        }
    }
}

pub async fn run_periodic_metadata_flush(
    engine: SharedLogEngine,
    mut shutdown_rx: Receiver<()>,
    interval: Duration, ){
    let mut ticker = tokio::time::interval(interval);
    
    async fn flush_meta_data(engine: &SharedLogEngine){
        let engine_guard = engine.lock().await;
        for topic in engine_guard.topics.values(){
            for partition in topic.partitions.values(){
                let partition = partition.lock().await;
                if partition.meta_flush_pending.swap(false, Ordering::Relaxed){
                    if let Err(e) = partition.persist_meta() {
                        tracing::warn!(error = ?e, "Failed to persist metadata");
                    }
                }
            }

        }
    }
    
    loop{
        tokio::select! {
            _ = ticker.tick()=>{
                flush_meta_data(&engine).await; 
            }
            _ = shutdown_rx.changed() => {
                flush_meta_data(&engine).await; 
            }
        }
        
        

    }
}