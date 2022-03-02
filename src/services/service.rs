use super::service_inner::ServiceInner;
use crate::services::service_inner::IServiceInner;
use crate::time::timestamp;
use anyhow::{bail, ensure, Result};
use aqueue::Actor;
use data_rw::DataReader;
use log::info;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tcpclient::TcpClient;
use tokio::io::{AsyncReadExt, ReadHalf};
use tokio::net::TcpStream;
use tokio::time::sleep;

/// 内部服务
pub struct Service {
    /// 服务器id
    pub service_id: u32,
    /// 服务器地址
    pub address: String,
    pub is_start: AtomicBool,
    /// 内部服务Actor
    pub inner: Arc<Actor<ServiceInner>>,
}

impl Service {
    pub fn new(gateway_id: u32, service_id: u32, ip: &str, port: i32) -> Service {
        Service {
            service_id,
            address: format!("{}:{}", ip, port),
            is_start: Default::default(),
            inner: Arc::new(Actor::new(ServiceInner {
                gateway_id,
                service_id,
                client: None,
                type_ids: Default::default(),
                last_ping_time: Default::default(),
                ping_delay_tick: Default::default(),
                wait_open_table: Default::default(),
                open_table: Default::default(),
                disconnect_tx: None,
            })),
        }
    }

    /// 启动
    pub fn start(&self) {
        if self
            .is_start
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Acquire)
            == Ok(false)
        {
            log::info!("service:{} is start", self.service_id);
            self.try_connect();
        }
    }

    /// 尝试连接
    fn try_connect(&self) {
        let service_id = self.service_id;
        let address = self.address.clone();
        let inner = self.inner.clone();
        tokio::spawn(async move {
            let (tx, rx) = bi_directional_pipe::sync::pipe::<(), ()>();

            inner
                .inner_call(|inner| async move {
                    inner.get_mut().disconnect_tx = Some(tx);
                    Ok(())
                })
                .await
                .expect("not set service tx");

            let ref_address = address.as_str();
            let ref_inner = &inner;
            let mut need_wait = false;

            loop {
                if need_wait {
                    sleep(Duration::from_secs(5)).await;
                }

                let connect = async move {
                    loop {
                        match TcpClient::connect(
                            ref_address,
                            |inner, client, reader| async move {
                                let res = Self::reader_buffer(&inner, client, reader).await;
                                // 注意断线 触发tx
                                inner.disconnect().await?;
                                res
                            },
                            ref_inner.clone(),
                        )
                        .await
                        {
                            Ok(client) => {
                                ref_inner.set_client(client).await?;
                                return ref_inner.send_register().await;
                            }
                            Err(err) => {
                                log::debug!(
                                    "connect to {}-{:?} fail:{};restart in 5 seconds",
                                    service_id,
                                    ref_address,
                                    err
                                );
                                sleep(Duration::from_secs(5)).await;
                            }
                        }
                    }
                };
                if let Err(err) = connect.await {
                    log::error!("connect service:{} error:{}", service_id, err);
                    if let Err(err) = inner.disconnect().await {
                        log::error!("disconnect service:{} error:{:?}", service_id, err);
                    }
                } else {
                    log::info!("connect to {}-{} ok", service_id, ref_address);
                    //等待断线重连
                    rx.recv().await.expect("bi_directional_pipe read fail");
                }
                need_wait = true;
            }
        });
    }

    /// 读取数据包
    async fn reader_buffer(
        inner: &Arc<Actor<ServiceInner>>,
        _client: Arc<Actor<TcpClient<TcpStream>>>,
        mut reader: ReadHalf<TcpStream>,
    ) -> Result<bool> {
        let service_id = inner.get_service_id();
        loop {
            let len = {
                if let Ok(len) = reader.read_u32_le().await {
                    len as usize
                } else {
                    log::warn!("service:{} disconnect not read data", service_id);
                    break;
                }
            };

            let mut buff = vec![0; len];
            let rev = reader.read_exact(&mut buff).await?;
            ensure!(
                len == rev,
                "service:{} read buff error len:{}>rev:{}",
                service_id,
                len,
                rev
            );

            let mut dr = DataReader::from(&buff);
            if 0xFFFFFFFFu32 == dr.read_fixed::<u32>()? {
                //到网关的数据
                let cmd = dr.read_var_str()?;
                match cmd {
                    "typeids" => {
                        let len = dr.read_fixed::<u32>()?;
                        let mut ids = Vec::with_capacity(len as usize);
                        for _ in 0..len {
                            ids.push(dr.read_fixed::<u32>()?);
                        }
                        inner
                            .inner_call(
                                |inner| async move { inner.get_mut().init_typeid_table(ids) },
                            )
                            .await?;
                        info!("service:{} push type ids count:{}", service_id, len);
                    }
                    "ping" => {
                        let now = timestamp();
                        if let Ok(tick) = dr.read_var_integer::<i64>() {
                            inner.set_ping_delay_tick(now - tick);
                        } else {
                            log::warn!("service:{} read ping tick fail", service_id)
                        }
                        inner.set_last_ping_time(now);
                    }
                    _ => {
                        bail!("service:{} incompatible cmd:{}", service_id, cmd)
                    }
                }
            } else {
                //发送数据包给客户端
            }
        }
        Ok(true)
    }
}
