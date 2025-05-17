use flyQ::core::offset_tracker::OffsetTracker;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch::Receiver;
use tokio::sync::Mutex;

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
