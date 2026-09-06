//! Comb Log process bridge. Stdio JSONL v1. stdout is protocol frames only.

use anyhow::{anyhow, Result};
use clap::Parser;
use comb_core::DigestKey;
use comb_object::local::LocalBackend;
use comb_object::s3::S3Backend;
use combctl::config::{self, BackendConfig};
use combctl::store::Store;
use std::path::PathBuf;
use std::sync::Arc;

#[path = "../bridge/mod.rs"]
mod bridge;

use bridge::handler::{mint_writer, Bridge};
use bridge::limits::Limits;
use bridge::stdio;

#[derive(Parser)]
#[command(
    name = "comb-bridge",
    about = "Comb Log process bridge (JSONL v1 stdio)"
)]
struct Cli {
    /// Config directory (default: ./.comb)
    #[arg(long)]
    dir: Option<PathBuf>,
    /// Writer identity used for Log leases. Unique per process when omitted.
    #[arg(long)]
    writer: Option<String>,
    /// Lease TTL in seconds for appends
    #[arg(long, default_value_t = 60)]
    lease: i64,
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cli = Cli::parse();
    let writer = match cli.writer {
        Some(w) if w.is_empty() => return Err(anyhow!("--writer must not be empty")),
        Some(w) => w,
        None => mint_writer(),
    };
    if cli.lease <= 0 {
        return Err(anyhow!("--lease must be positive"));
    }
    let dir = config::config_dir(cli.dir.as_deref());
    let cfg = config::load(&dir)?;
    let key = DigestKey::from_hex(&cfg.digest_key)?;
    let (backend, cache_dir): (
        std::sync::Arc<dyn comb_object::ObjectBackend>,
        Option<PathBuf>,
    ) = match &cfg.backend {
        BackendConfig::Local { root } => (std::sync::Arc::new(LocalBackend::new(root)), None),
        BackendConfig::S3 {
            bucket,
            region,
            profile,
            prefix,
            endpoint,
        } => (
            std::sync::Arc::new(
                S3Backend::connect(
                    profile.as_deref(),
                    Some(region),
                    bucket,
                    prefix,
                    endpoint.as_deref(),
                )
                .await,
            ),
            Some(dir.join("cache")),
        ),
    };
    let store = Store {
        backend,
        tenant: cfg.tenant.clone(),
        key,
        cache_dir,
    };
    let bridge = Arc::new(Bridge::new(store, writer, cli.lease, Limits::default()));
    stdio::run(tokio::io::stdin(), tokio::io::stdout(), bridge).await
}
