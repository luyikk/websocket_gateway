use crate::services::service::Service;
use crate::services::service_inner::IServiceInner;
use crate::CONFIG;
use ahash::AHashMap;
use anyhow::{Context, Result};
use aqueue::Actor;

/// 服务器管理器
pub struct ServiceManager {
    gateway_id: u32,
    services: AHashMap<u32, Service>,
}

impl ServiceManager {
    pub fn new() -> Actor<ServiceManager> {
        let mut services = AHashMap::new();

        for service in CONFIG.services.iter() {
            services.insert(
                service.service_id,
                Service::new(
                    CONFIG.gateway_id,
                    service.service_id,
                    &service.ip,
                    service.port,
                ),
            );
        }

        Actor::new(ServiceManager {
            gateway_id: CONFIG.gateway_id,
            services,
        })
    }
    /// 启动服务器
    fn start(&self) {
        for service in self.services.values() {
            service.start();
        }
    }

    /// open 服务器
    #[inline]
    async fn open_service(&self, session_id: u32, service_id: u32, ipaddress: &str) -> Result<()> {
        let server = self
            .services
            .get(&service_id)
            .with_context(|| format!("not found service:{}", service_id))?;
        log::info!("start open service:{} peer:{}", service_id, session_id);
        server.inner.open(session_id, ipaddress).await
    }

    /// 客户端断线事件
    #[inline]
    async fn disconnect_events(&self, session_id: u32) -> Result<()> {
        let services = self
            .services
            .values()
            .filter(|service| service.inner.have_session_id(session_id));

        let mut have = false;

        for service in services {
            have = true;
            if let Err(err) = service.inner.drop_client(session_id).await {
                log::error! {"DropClientPeer error service {} session_id:{} error:{:?}", service.service_id, session_id, err}
            }
        }

        if !have {
            if let Some(service) = self.services.get(&0) {
                if let Err(err) = service.inner.drop_client(session_id).await {
                    log::error! {"DropClientPeer error main service 0 session_id:{} error:{:?}",  session_id, err}
                }
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait]
pub trait IServiceManager {
    /// 启动服务
    fn start(&self);
    /// 检查服务器ping超时
    async fn check_ping(&self) -> Result<()>;
    /// open 服务器
    async fn open_service(&self, session_id: u32, service_id: u32, ipaddress: &str) -> Result<()>;
    /// 客户端断线事件
    async fn disconnect_events(&self, session_id: u32) -> Result<()>;
}

#[async_trait::async_trait]
impl IServiceManager for Actor<ServiceManager> {
    #[inline]
    fn start(&self) {
        unsafe {
            self.deref_inner().start();
        }
    }

    #[inline]
    async fn check_ping(&self) -> Result<()> {
        unsafe {
            for service in self.deref_inner().services.values() {
                if !service.is_disconnect() && service.check_ping().await? {
                    service.disconnect_now().await?;
                }
            }
            Ok(())
        }
    }

    #[inline]
    async fn open_service(&self, session_id: u32, service_id: u32, ipaddress: &str) -> Result<()> {
        unsafe {
            self.deref_inner()
                .open_service(session_id, service_id, ipaddress)
                .await
        }
    }

    #[inline]
    async fn disconnect_events(&self, session_id: u32) -> Result<()> {
        unsafe { self.deref_inner().disconnect_events(session_id).await }
    }
}
