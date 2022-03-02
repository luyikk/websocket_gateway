use crate::get_len;
use ahash::AHashSet;
use anyhow::{bail, Result};
use aqueue::Actor;
use bi_directional_pipe::sync::Left;
use bytes::BufMut;
use std::ops::Deref;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tcpclient::{SocketClientTrait, TcpClient};
use tokio::net::TcpStream;

/// 内部服务器 Inner
pub struct ServiceInner {
    /// 网格Id
    pub gateway_id: u32,
    /// 服务器id
    pub service_id: u32,
    /// socket client
    pub client: Option<Arc<Actor<TcpClient<TcpStream>>>>,
    /// 服务器注册的pkg id路由表
    /// 说明此服务器支持什么类型的数据包
    pub type_ids: AHashSet<u32>,
    /// 最后ping时间
    pub last_ping_time: AtomicI64,
    /// 服务器延迟
    pub ping_delay_tick: AtomicI64,
    /// 等待open的客户端
    pub wait_open_table: AHashSet<u32>,
    /// 已经open的客户端
    pub open_table: AHashSet<u32>,
    /// disconnect pipe
    pub disconnect_tx: Option<Left<(), ()>>,
}

impl ServiceInner {
    /// 强制断线
    #[inline]
    async fn disconnect(&mut self) -> Result<()> {
        if let Some(ref client) = self.client {
            client.disconnect().await?;
            self.client = None;
            if let Some(ref tx) = self.disconnect_tx {
                tx.send(());
            }
        }
        Ok(())
    }

    ///添加type ids
    #[inline]
    pub(crate) fn init_typeid_table(&mut self, ids: Vec<u32>) -> Result<()> {
        self.type_ids.clear();
        for id in ids {
            self.type_ids.insert(id);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait IServiceInner {
    /// 获取服务器id
    fn get_service_id(&self) -> u32;
    /// 设置 ping_delay_tick
    fn set_ping_delay_tick(&self, timestamp: i64);
    /// 获取 ping_delay_tick
    fn get_ping_delay_tick(&self) -> i64;
    /// 设置 last_ping_time
    fn set_last_ping_time(&self, timestamp: i64);
    /// 获取last_ping_time
    fn get_last_ping_time(&self) -> i64;
    /// 设置socket 链接
    async fn set_client(&self, client: Arc<Actor<TcpClient<TcpStream>>>) -> Result<()>;
    /// 发送注册包
    async fn send_register(&self) -> Result<()>;
    /// 发送数据包
    async fn send_buff<B: Deref<Target = [u8]> + Send + Sync + 'static>(
        &self,
        buff: B,
    ) -> Result<()>;
    /// 发送数据包切片
    async fn send_all_ref<'a>(&'a self, buff: &'a [u8]) -> Result<()>;
    /// 断线
    async fn disconnect(&self) -> Result<()>;
}

#[async_trait::async_trait]
impl IServiceInner for Actor<ServiceInner> {
    #[inline]
    fn get_service_id(&self) -> u32 {
        unsafe { self.deref_inner().service_id }
    }

    #[inline]
    fn set_ping_delay_tick(&self, timestamp: i64) {
        unsafe {
            self.deref_inner()
                .ping_delay_tick
                .store(timestamp, Ordering::Release);
        }
    }

    #[inline]
    fn get_ping_delay_tick(&self) -> i64 {
        unsafe { self.deref_inner().ping_delay_tick.load(Ordering::Acquire) }
    }
    #[inline]
    fn set_last_ping_time(&self, timestamp: i64) {
        unsafe {
            self.deref_inner()
                .last_ping_time
                .store(timestamp, Ordering::Release);
        }
    }
    #[inline]
    fn get_last_ping_time(&self) -> i64 {
        unsafe { self.deref_inner().last_ping_time.load(Ordering::Acquire) }
    }

    #[inline]
    async fn set_client(&self, client: Arc<Actor<TcpClient<TcpStream>>>) -> Result<()> {
        self.inner_call(|inner| async move {
            inner.get_mut().client = Some(client);
            Ok(())
        })
        .await
    }

    #[inline]
    async fn send_register(&self) -> Result<()> {
        unsafe {
            let mut buffer = data_rw::Data::new();
            buffer.write_fixed(0u32);
            buffer.write_fixed(0xFFFFFFFFu32);
            buffer.write_var_integer("gatewayId");
            buffer.write_var_integer(self.deref_inner().gateway_id);
            buffer.write_fixed(1u8);
            let len = get_len!(buffer);
            (&mut buffer[0..4]).put_u32_le(len);
            self.send_buff(buffer.into_inner()).await
        }
    }

    #[inline]
    async fn send_buff<B: Deref<Target = [u8]> + Send + Sync + 'static>(
        &self,
        buff: B,
    ) -> Result<()> {
        unsafe {
            if let Some(ref client) = self.deref_inner().client {
                client.send_all(buff).await
            } else {
                bail!("service:{} not connect", self.deref_inner().service_id)
            }
        }
    }

    #[inline]
    async fn send_all_ref<'a>(&'a self, buff: &'a [u8]) -> Result<()> {
        unsafe {
            if let Some(ref client) = self.deref_inner().client {
                client.send_all_ref(buff).await
            } else {
                bail!("service:{} not connect", self.deref_inner().service_id)
            }
        }
    }

    #[inline]
    async fn disconnect(&self) -> Result<()> {
        self.inner_call(|inner| async move { inner.get_mut().disconnect().await })
            .await
    }
}
