//! 加载配置模块

use anyhow::{anyhow, Result};
use serde::de::DeserializeOwned;

pub trait LoadConfig: Sized + DeserializeOwned {
    fn load_toml(path: &str) -> Result<Self>;
}

impl<T: Sized + DeserializeOwned> LoadConfig for T {
    fn load_toml(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        toml::from_str(content.as_str()).map_err(|e| anyhow!(e))
    }
}
