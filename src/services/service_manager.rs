use crate::services::service::Service;
use crate::CONFIG;
use ahash::AHashMap;
use anyhow::Result;
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
    pub fn start(&self) {
        for service in self.services.values() {
            service.start();
        }
    }
}

#[async_trait::async_trait]
pub trait IServiceManager {
    /// 启动服务
    fn start(&self);
    /// 检查服务器ping超时
    async fn check_ping(&self) -> Result<()>;
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
}
