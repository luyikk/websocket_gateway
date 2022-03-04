use crate::get_len;
use crate::time::timestamp;
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
    pub disconnect_sender: Option<Left<(), ()>>,
}

impl ServiceInner {
    /// 强制断线
    #[inline]
    async fn disconnect(&mut self) -> Result<()> {
        if let Some(ref client) = self.client {
            client.disconnect().await?;
            self.client = None;
            if let Some(ref tx) = self.disconnect_sender {
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

    /// 检查ping
    #[inline]
    async fn check_ping(&self) -> Result<bool> {
        let last_ping_time = self.last_ping_time.load(Ordering::Acquire);
        let now = timestamp();
        //30秒超时 单位tick 秒后 7个0
        if last_ping_time > 0 && now - last_ping_time > 30 * 1000 * 10000 {
            log::warn!(
                "service:{} ping time out,shutdown it,now:{},last_ping_time:{},ping_delay_tick:{}",
                self.service_id,
                now,
                last_ping_time,
                self.ping_delay_tick.load(Ordering::Acquire)
            );

            return Ok(true);
        } else if let Err(er) = self.send_ping(now).await {
            log::error!("service{} send ping  error:{:?}", self.service_id, er)
        }

        Ok(false)
    }

    /// 发送ping
    #[inline]
    async fn send_ping(&self, time: i64) -> Result<()> {
        let mut buffer = data_rw::Data::new();
        buffer.write_fixed(0u32);
        buffer.write_fixed(0xFFFFFFFFu32);
        buffer.write_var_integer("ping");
        buffer.write_var_integer(time);
        let len = get_len!(buffer);
        (&mut buffer[0..4]).put_u32_le(len);
        self.send_buff(buffer.into_inner()).await
    }

    /// 发送数据包
    #[inline]
    async fn send_buff<B: Deref<Target = [u8]> + Send + Sync + 'static>(
        &self,
        buff: B,
    ) -> Result<()> {
        if let Some(ref client) = self.client {
            client.send_all(buff).await
        } else {
            bail!("service:{} not connect", self.service_id)
        }
    }

    /// 发送数据包 ref
    #[inline]
    async fn send_all_ref<'a>(&'a self, buff: &'a [u8]) -> Result<()> {
        if let Some(ref client) = self.client {
            client.send_all_ref(buff).await
        } else {
            bail!("service:{} not connect", self.service_id)
        }
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
    /// 检查ping超时
    async fn check_ping(&self) -> Result<bool>;
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
    async fn check_ping(&self) -> Result<bool> {
        unsafe { self.deref_inner().check_ping().await }
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
            self.deref_inner().send_buff(buffer.into_inner()).await
        }
    }

    #[inline]
    async fn send_buff<B: Deref<Target = [u8]> + Send + Sync + 'static>(
        &self,
        buff: B,
    ) -> Result<()> {
        unsafe { self.deref_inner().send_buff(buff).await }
    }

    #[inline]
    async fn send_all_ref<'a>(&'a self, buff: &'a [u8]) -> Result<()> {
        unsafe { self.deref_inner().send_all_ref(buff).await }
    }

    #[inline]
    async fn disconnect(&self) -> Result<()> {
        self.inner_call(|inner| async move { inner.get_mut().disconnect().await })
            .await
    }
}
