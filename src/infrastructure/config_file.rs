//! TOML config-file loading — adapts an on-disk file to domain config types.

use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::fs;

use crate::domain::config::{BenchConfig, Endpoint};

#[derive(Debug, Deserialize, Serialize)]
pub struct ConfigToml {
    pub config: BenchConfig,
    pub endpoint: Vec<Endpoint>,
}

impl ConfigToml {
    pub fn load(path: &str) -> Result<Self> {
        let content =
            fs::read_to_string(path).with_context(|| format!("Failed to read config {}", path))?;
        let config = toml::from_str(&content).map_err(|err| anyhow!(err))?;
        Ok(config)
    }
}
