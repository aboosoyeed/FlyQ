use std::sync::Arc;
use tokio::sync::Mutex;
use flyQ::core::log_engine::LogEngine;

pub type SharedLogEngine = Arc<Mutex<LogEngine>>;