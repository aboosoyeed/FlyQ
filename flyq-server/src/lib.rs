pub mod core;
mod config;

use std::sync::OnceLock;

pub use config::BrokerConfig;

/// is Filled by `main()` **once**; thereafter read-only everywhere.
pub static BROKER_CONFIG: OnceLock<BrokerConfig> = OnceLock::new();

/// Convenience accessor used throughout the codebase.
pub fn broker_config() -> &'static BrokerConfig {
    BROKER_CONFIG.get_or_init(BrokerConfig::default)
}
