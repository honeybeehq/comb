use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub tenant: String,
    /// Tenant digest key, hex. Local secret for this slice; key management
    /// arrives with the encryption envelope work.
    pub digest_key: String,
    pub backend: BackendConfig,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum BackendConfig {
    Local { root: String },
    S3 {
        bucket: String,
        region: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        profile: Option<String>,
        #[serde(default = "default_prefix")]
        prefix: String,
        /// S3-compatible endpoint URL (MinIO etc.); AWS when absent.
        #[serde(skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
    },
}

fn default_prefix() -> String {
    "comb".into()
}

pub fn config_dir(explicit: Option<&Path>) -> PathBuf {
    explicit.map(|p| p.to_path_buf()).unwrap_or_else(|| PathBuf::from(".comb"))
}

pub fn load(dir: &Path) -> Result<Config> {
    let path = dir.join("config.toml");
    if !path.exists() {
        bail!("no Comb config at {} — run `combctl init` first", path.display());
    }
    let text = std::fs::read_to_string(&path)?;
    toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))
}

pub fn save(dir: &Path, config: &Config) -> Result<()> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join("config.toml");
    if path.exists() {
        bail!("{} already exists — refusing to overwrite (it holds the tenant digest key)", path.display());
    }
    std::fs::write(&path, toml::to_string_pretty(config)?)?;
    Ok(())
}
