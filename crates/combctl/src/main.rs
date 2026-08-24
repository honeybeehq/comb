mod config;
mod store;

use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use comb_core::{Digest, DigestKey};
use comb_object::local::LocalBackend;
use comb_object::s3::S3Backend;
use config::{BackendConfig, Config};
use std::path::PathBuf;
use store::{GetSource, Store};

#[derive(Parser)]
#[command(name = "combctl", about = "Comb durable-state substrate CLI", version)]
struct Cli {
    /// Config directory (default: ./.comb)
    #[arg(long, global = true)]
    dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Initialize a Comb config in this directory
    Init {
        #[arg(long, default_value = "org_dev")]
        tenant: String,
        /// Backend kind: local | s3
        #[arg(long)]
        backend: String,
        /// Local backend: storage root directory
        #[arg(long)]
        root: Option<String>,
        /// S3 backend: bucket name
        #[arg(long)]
        bucket: Option<String>,
        #[arg(long)]
        region: Option<String>,
        #[arg(long)]
        profile: Option<String>,
        #[arg(long, default_value = "comb")]
        prefix: String,
    },
    /// Store a file as an immutable blob; prints its digest
    Put { file: PathBuf },
    /// Fetch a blob by digest, verify it, write to stdout or a file
    Get {
        digest: String,
        #[arg(short, long)]
        out: Option<PathBuf>,
    },
    /// Verify a blob straight from the backend (bypasses the cache)
    Verify { digest: String },
    /// Ref operations
    #[command(subcommand)]
    Ref(RefCommand),
    /// Acquire a ref's lease; prints the fence (epoch)
    Claim {
        name: String,
        #[arg(long, default_value = "veteran")]
        writer: String,
        #[arg(long, default_value_t = 60)]
        ttl: i64,
        /// Administrative takeover of a live lease (fences the old writer)
        #[arg(long)]
        steal: bool,
    },
    /// Renew a lease (requires the fence; not journaled)
    Renew {
        name: String,
        #[arg(long)]
        fence: u64,
        #[arg(long, default_value_t = 60)]
        ttl: i64,
    },
    /// Release a lease (requires the fence)
    Release {
        name: String,
        #[arg(long)]
        fence: u64,
    },
}

#[derive(Subcommand)]
enum RefCommand {
    /// Show a ref
    Get { name: String },
    /// Advance a ref to a digest with a single guarded write
    Set {
        name: String,
        digest: String,
        /// Fencing token from `claim`; required while the ref is leased
        #[arg(long)]
        fence: Option<u64>,
    },
    /// Show the journal for a ref (newest first)
    History {
        name: String,
        #[arg(long, default_value_t = 20)]
        max: usize,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(e) = run(cli).await {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<()> {
    let dir = config::config_dir(cli.dir.as_deref());

    if let Command::Init { tenant, backend, root, bucket, region, profile, prefix } = &cli.command {
        let backend = match backend.as_str() {
            "local" => BackendConfig::Local {
                root: root.clone().ok_or_else(|| anyhow!("--root is required for local backend"))?,
            },
            "s3" => BackendConfig::S3 {
                bucket: bucket.clone().ok_or_else(|| anyhow!("--bucket is required for s3 backend"))?,
                region: region.clone().ok_or_else(|| anyhow!("--region is required for s3 backend"))?,
                profile: profile.clone(),
                prefix: prefix.clone(),
            },
            other => return Err(anyhow!("unknown backend kind: {other}")),
        };
        let mut key = [0u8; 32];
        rand::Rng::fill(&mut rand::rng(), &mut key);
        let config = Config {
            tenant: tenant.clone(),
            digest_key: hex::encode(key),
            backend,
        };
        config::save(&dir, &config)?;
        println!("initialized tenant {} in {}", config.tenant, dir.display());
        return Ok(());
    }

    let cfg = config::load(&dir)?;
    let key = DigestKey::from_hex(&cfg.digest_key)?;
    let (backend, cache_dir): (Box<dyn comb_object::ObjectBackend>, Option<PathBuf>) = match &cfg.backend {
        BackendConfig::Local { root } => (Box::new(LocalBackend::new(root)), None),
        BackendConfig::S3 { bucket, region, profile, prefix } => (
            Box::new(S3Backend::connect(profile.as_deref(), Some(region), bucket, prefix).await),
            Some(dir.join("cache")),
        ),
    };
    let store = Store { backend, tenant: cfg.tenant.clone(), key, cache_dir };

    match cli.command {
        Command::Init { .. } => unreachable!(),
        Command::Put { file } => {
            let bytes = std::fs::read(&file)?;
            let size = bytes.len();
            let (digest, dedup) = store.put_blob(bytes).await?;
            println!("{digest}");
            eprintln!(
                "{size} bytes stored{}",
                if dedup { " (already present — deduplicated)" } else { "" }
            );
        }
        Command::Get { digest, out } => {
            let digest = Digest::parse(&digest)?;
            let (payload, source) = store.get_blob(&digest).await?;
            match out {
                Some(path) => {
                    std::fs::write(&path, &payload)?;
                    eprintln!(
                        "{} bytes written to {} (from {})",
                        payload.len(),
                        path.display(),
                        if source == GetSource::Cache { "cache" } else { "backend" }
                    );
                }
                None => {
                    use std::io::Write;
                    std::io::stdout().write_all(&payload)?;
                }
            }
        }
        Command::Verify { digest } => {
            let digest = Digest::parse(&digest)?;
            // Bypass the cache: verify what the backend actually holds.
            let no_cache = Store { cache_dir: None, ..store };
            let (payload, _) = no_cache.get_blob(&digest).await?;
            println!("ok: {digest} verified ({} bytes)", payload.len());
        }
        Command::Ref(RefCommand::Get { name }) => {
            match store.read_ref(&name).await? {
                None => println!("ref {name} does not exist"),
                Some((value, _)) => println!("{}", serde_json::to_string_pretty(&value)?),
            }
        }
        Command::Ref(RefCommand::Set { name, digest, fence }) => {
            let digest = Digest::parse(&digest)?;
            let value = store.set_target(&name, digest, fence).await?;
            println!(
                "{} -> {} (generation {}, epoch {})",
                name,
                value.target.as_ref().unwrap(),
                value.generation,
                value.epoch
            );
        }
        Command::Ref(RefCommand::History { name, max }) => {
            let entries = store.history(&name, max).await?;
            if entries.is_empty() {
                println!("no journal entries for {name}");
            }
            for e in entries {
                println!(
                    "gen {:>4}  epoch {:>3}  {}  {}  {}",
                    e.new_generation,
                    e.epoch,
                    e.at.format("%Y-%m-%d %H:%M:%S"),
                    e.writer.as_deref().unwrap_or("-"),
                    e.target.as_ref().map(|d| d.to_string()).unwrap_or_else(|| "-".into()),
                );
            }
        }
        Command::Claim { name, writer, ttl, steal } => {
            let value = store.claim(&name, &writer, ttl, steal).await?;
            let lease = value.lease.as_ref().unwrap();
            println!(
                "claimed {name} as {} — fence {} (lease until {})",
                lease.writer,
                value.epoch,
                lease.lease_until.format("%H:%M:%S")
            );
        }
        Command::Renew { name, fence, ttl } => {
            let value = store.renew(&name, fence, ttl).await?;
            println!(
                "renewed {name} until {} (generation unchanged: {})",
                value.lease.as_ref().unwrap().lease_until.format("%H:%M:%S"),
                value.generation
            );
        }
        Command::Release { name, fence } => {
            let value = store.release(&name, fence).await?;
            println!("released {name} (generation {})", value.generation);
        }
    }
    Ok(())
}
