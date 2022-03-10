use anyhow::{bail, Result};
use aqueue::Actor;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio::time::timeout;
use websocket_server_async::*;

use crate::static_def::USER_MANAGER;
use crate::users::{input_buff, Client, IUserManager};
use crate::{IServiceManager, CONFIG, SERVICE_MANAGER};

/// 最大数据表长度限制 1M
const MAX_BUFF_LEN: usize = 1024 * 1024;
/// 最大帧长度 512K
const MAX_FRAME_LEN: usize = 512 * 1024;

pub type Peer = Arc<Actor<WSPeer>>;

/// 客户端监听服务
pub struct Listen {
    server: Arc<dyn IWebSocketServer<()>>,
}

impl Listen {
    pub async fn new<ToAddress: ToSocketAddrs>(address: ToAddress) -> Result<Self> {
        let server = Builder::new(address)
            .set_config(WebSocketConfig {
                max_send_queue: Some(10),
                max_message_size: Some(MAX_BUFF_LEN),
                max_frame_size: Some(MAX_FRAME_LEN),
                accept_unmasked_frames: false,
            })
            .set_connect_event(|addr| {
                log::info!("ipaddress:{} connect", addr);
                true
            })
            .set_input_event(|reader, peer, _| async move {
                let client = USER_MANAGER.make_client(peer).await?;
                let session_id = client.session_id;
                let res = Self::data_input(reader, client).await;
                if let Err(err) = USER_MANAGER.remove_client(session_id).await {
                    log::error!("remove peer:{} error:{}", session_id, err);
                }
                res
            })
            .build()
            .await;
        Ok(Self { server })
    }

    /// 启动服务器
    pub async fn start(&self) -> Result<()> {
        self.server.start_block(()).await
    }

    /// 数据包处理
    #[inline]
    async fn data_input(
        mut reader: SplitStream<WebSocketStream<TcpStream>>,
        client: Arc<Client>,
    ) -> Result<()> {
        log::debug!("create peer:{}", client);
        SERVICE_MANAGER
            .open_service(client.session_id, 0, &client.address)
            .await?;

        loop {
            let msg = match timeout(
                Duration::from_secs(CONFIG.client_timeout_seconds as u64),
                reader.next(),
            )
            .await
            {
                Ok(Some(Ok(msg))) => msg,
                Ok(Some(Err(err)))=>bail!("read msg error:{:?}",err),
                Ok(None) => bail!("client:{} not read message", client),
                Err(_) => {
                    bail!(
                        "client:{} {}secs not read timeout",
                        client,
                        CONFIG.client_timeout_seconds
                    )
                }
            };

            if client.peer.is_disconnect().await?{
                break;
            }

            //client.last_recv_time.store(timestamp(), Ordering::Release);

            if msg.is_binary() {
                //如果没有OPEN 直接掐线
                if !client.is_open_zero.load(Ordering::Acquire) {
                    log::warn!("peer:{} not open send data,disconnect!", client);
                    break;
                }
                input_buff(&client, msg.into_data()).await?;
            } else if msg.is_close() {
                log::info!("client:{} close", client);
                break;
            } else {
                log::error!("I won't support it msg:{:?}", msg);
                break;
            }
        }

        Ok(())
    }
}
