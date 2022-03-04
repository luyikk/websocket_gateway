mod config;
mod services;
mod static_def;
mod time;
mod timer;

use crate::services::IServiceManager;
use crate::static_def::{CONFIG, SERVICE_MANAGER, TIMER_MANAGER};
use anyhow::Result;
use log::LevelFilter;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::new()
        .filter_level(LevelFilter::Trace)
        .filter_module("mio::poll", LevelFilter::Error)
        .init();

    SERVICE_MANAGER.start();
    TIMER_MANAGER.start();
    sleep(Duration::from_secs(10000)).await;
    Ok(())
}

#[macro_export]
macro_rules! get_len {
    ($buff:expr) => {
        ($buff.len() - 4) as u32
    };
}
