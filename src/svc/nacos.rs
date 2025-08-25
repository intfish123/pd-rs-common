//! nacos discover and configuration

use anyhow::{Result, anyhow};
use dashmap::DashMap;
use nacos_sdk::api::config::ConfigResponse;
use nacos_sdk::api::constants;
use nacos_sdk::api::props::ClientProps;
use nacos_sdk::api::{
    config::{ConfigChangeListener, ConfigService, ConfigServiceBuilder},
    naming::{
        NamingChangeEvent, NamingEventListener, NamingService, NamingServiceBuilder,
        ServiceInstance,
    },
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug)]
pub struct NacosNamingAndConfigData {
    pub naming: NamingService,
    pub config: ConfigService,

    state: RwLock<NamingState>,

    pub event_listener: Arc<NacosEventListener>,
}

#[derive(Clone, Debug)]
pub struct NacosEventListener {
    pub sub_svc_map: DashMap<String, Vec<ServiceInstance>>,
    pub sub_svc_change_sender: async_broadcast::Sender<Arc<NamingChangeEvent>>,
    pub sub_svc_change_receiver: async_broadcast::Receiver<Arc<NamingChangeEvent>>,

    pub config_data_map: DashMap<String, ConfigResponse>,
    pub config_change_sender: async_broadcast::Sender<ConfigResponse>,
    pub config_change_receiver: async_broadcast::Receiver<ConfigResponse>,
}

#[derive(Clone, Debug, Default)]
pub struct NamingState {
    pub service_name: String,
    pub group_name: Option<String>,
    pub service_instance: Vec<ServiceInstance>,
}

impl NacosNamingAndConfigData {
    pub fn get_state(&self) -> NamingState {
        self.state.read().unwrap().clone()
    }
    pub fn update_state(
        &self,
        service_name: String,
        group_name: Option<String>,
        service_instance: Vec<ServiceInstance>,
    ) {
        let mut state = self.state.write().unwrap();
        state.service_name = service_name;
        state.group_name = group_name;
        state.service_instance = service_instance;
    }

    pub fn new(
        server_addr: String,
        namespace: String,
        app_name: String,
        user_name: Option<String>,
        password: Option<String>,
    ) -> Result<Self> {
        let mut client_props = ClientProps::new()
            // eg. "127.0.0.1:8848"
            .server_addr(server_addr)
            .namespace(if namespace.to_lowercase() == "public" {
                ""
            } else {
                namespace.as_str()
            })
            .app_name(app_name.clone());

        let mut enable_http_login = false;
        if let Some(user_name) = user_name {
            if !user_name.is_empty() {
                client_props = client_props.auth_username(user_name);
                enable_http_login = true;
            }
        }
        if let Some(password) = password {
            if !password.is_empty() {
                client_props = client_props.auth_password(password);
                enable_http_login = true;
            }
        }
        let naming_service;
        let config_service;
        if enable_http_login {
            naming_service = NamingServiceBuilder::new(client_props.clone())
                .enable_auth_plugin_http()
                .build()?;
            config_service = ConfigServiceBuilder::new(client_props)
                .enable_auth_plugin_http()
                .build()?;
        } else {
            naming_service = NamingServiceBuilder::new(client_props.clone()).build()?;
            config_service = ConfigServiceBuilder::new(client_props).build()?;
        }

        let (mut sub_svc_s, sub_svc_r) = async_broadcast::broadcast(100);
        sub_svc_s.set_overflow(true);

        let (mut config_s, config_r) = async_broadcast::broadcast(100);
        config_s.set_overflow(true);

        let nel = NacosEventListener {
            sub_svc_map: DashMap::default(),
            sub_svc_change_sender: sub_svc_s,
            sub_svc_change_receiver: sub_svc_r,
            config_data_map: DashMap::default(),
            config_change_sender: config_s,
            config_change_receiver: config_r,
        };
        Ok(NacosNamingAndConfigData {
            naming: naming_service,
            config: config_service,
            state: RwLock::new(NamingState {
                service_name: "".to_string(),
                group_name: None,
                service_instance: Vec::new(),
            }),
            event_listener: Arc::new(nel),
        })
    }

    /// register self to nacos
    pub async fn register_service(
        &self,
        service_name: String,
        service_port: i32,
        service_ip: Option<String>,
        group_name: Option<String>,
        service_metadata: HashMap<String, String>,
    ) -> Result<Vec<ServiceInstance>> {
        let mut tmp_ip = "127.0.0.1".to_string();
        if let Some(ip) = service_ip {
            tmp_ip = ip;
        } else {
            let local_ip = local_ip_address::local_ip()?;
            tmp_ip = local_ip.to_string();
        }

        let svc_inst = ServiceInstance {
            ip: tmp_ip.clone(),
            port: service_port,
            metadata: service_metadata,
            ..Default::default()
        };

        let mut tmp_group = Some(constants::DEFAULT_GROUP.to_string());
        if let Some(gn) = group_name {
            tmp_group = Some(gn);
        }

        let _register_inst_ret = self
            .naming
            .register_instance(service_name.clone(), tmp_group.clone(), svc_inst.clone())
            .await;
        match _register_inst_ret {
            Ok(_) => {
                tracing::info!(
                    "register service {}@{} to nacos successfully",
                    service_name.clone(),
                    tmp_ip.clone()
                );
                self.update_state(service_name, tmp_group, vec![svc_inst.clone()]);
                Ok(vec![svc_inst])
            }
            Err(e) => {
                tracing::error!(
                    "failed to register service {}@{} to nacos: {}",
                    service_name.clone(),
                    tmp_ip.to_string(),
                    e
                );
                Err(anyhow!(e))
            }
        }
    }

    /// deregister self from nacos
    pub async fn deregister_service(&self) -> Result<()> {
        let state = self.get_state();
        let service_name = state.service_name;
        let group_name = state.group_name;
        let svc_inst = state.service_instance;

        let mut errors = Vec::new();
        let mut insts = Vec::new();

        if !svc_inst.is_empty() {
            for inst in svc_inst {
                match self
                    .naming
                    .deregister_instance(service_name.clone(), group_name.clone(), inst.clone())
                    .await
                {
                    Ok(_) => insts.push(format!("{}@{}", service_name.clone(), inst.ip.clone())),
                    Err(e) => errors.push(e.to_string()),
                }
            }
        }

        if !errors.is_empty() {
            Err(anyhow!(
                "failed to deregister instances: {}",
                errors.join(", ")
            ))
        } else {
            tracing::info!("deregister instances: {}", insts.join(", "));
            Ok(())
        }
    }

    pub async fn subscribe_service(&self, sub_service_name: String) -> Result<()> {
        let state = self.get_state();
        let group_name = state.group_name;
        match self
            .naming
            .subscribe(
                sub_service_name,
                group_name,
                Vec::default(),
                self.event_listener.clone(),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!("subscribe_service error: {}", e)),
        }
    }

    pub async fn add_config_listener(
        &self,
        data_id: String,
        group_name: String,
        listener: Arc<dyn ConfigChangeListener>,
    ) -> Result<()> {
        let config_service = self.config.clone();
        let _listen = config_service
            .add_listener(data_id, group_name, listener)
            .await;
        match _listen {
            Ok(_) => Ok(()),
            Err(err) => Err(anyhow!("listen config error {:?}", err)),
        }
    }

    pub async fn add_default_config_listener(
        &self,
        data_id: String,
        group_name: String,
    ) -> Result<()> {
        self.add_config_listener(data_id, group_name, self.event_listener.clone())
            .await
    }

    pub async fn get_config(&self, data_id: String, group_name: String) -> Result<String> {
        let ret = self.config.get_config(data_id, group_name).await;
        match ret {
            Ok(config) => Ok(config.content().clone()),
            Err(err) => Err(anyhow!("failed to get config: {}", err)),
        }
    }
}

impl NamingEventListener for NacosEventListener {
    fn event(&self, event: Arc<NamingChangeEvent>) {
        tracing::debug!("subscriber notify event={:?}", event.clone());
        let inst_list = event.instances.clone().unwrap_or_default();
        self.sub_svc_map
            .insert(event.service_name.clone(), inst_list);
        let _ = self.sub_svc_change_sender.try_broadcast(event);
    }
}

impl ConfigChangeListener for NacosEventListener {
    fn notify(&self, config_resp: ConfigResponse) {
        tracing::debug!("config change event={:?}", config_resp.clone());
        self.config_data_map
            .insert(config_resp.data_id().clone(), config_resp.clone());

        let _ = self.config_change_sender.try_broadcast(config_resp);
    }
}
