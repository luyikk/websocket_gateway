mod services;
mod time;

use std::time::Duration;
use anyhow::Result;
use tokio::time::sleep;
use log::LevelFilter;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::new()
        .filter_level(LevelFilter::Trace)
        .filter_module("mio::poll", LevelFilter::Error)
        .init();

    let server= services::service::Service::new(1000,0,"127.0.0.1",18000);
    server.start();
    server.start();
    sleep(Duration::from_secs(10000)).await;
    Ok(())
}

#[macro_export]
macro_rules! get_len {
    ($buff:expr) => {
        ($buff.len() - 4) as u32
    };
}
