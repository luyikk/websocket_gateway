mod config;
mod services;
mod static_def;
mod time;
mod timer;
mod users;

use crate::services::IServiceManager;
use crate::static_def::{CONFIG, SERVICE_MANAGER, TIMER_MANAGER};
use crate::users::Listen;
use anyhow::Result;
use log::LevelFilter;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::new()
        .filter_level(LevelFilter::Trace)
        .filter_module("mio::poll", LevelFilter::Error)
        .init();

    SERVICE_MANAGER.start();
    TIMER_MANAGER.start();
    let server = Listen::new(format!("0.0.0.0:{}", CONFIG.listen_port)).await?;
    server.start().await
}
