use anyhow::{anyhow, Result};
use clap::{Parser, Subcommand};
use comb_core::{Digest, DigestKey, OperationId};
use comb_object::local::LocalBackend;
use comb_object::s3::S3Backend;
use combctl::config::{self, BackendConfig, Config};
use combctl::store::{GetSource, Store};
use std::path::PathBuf;

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
        /// S3-compatible endpoint URL (MinIO etc.)
        #[arg(long)]
        endpoint: Option<String>,
    },
    /// Backend operations
    #[command(subcommand)]
    Backend(BackendCommand),
    /// Reachability sweep: delete unreachable objects past the grace window
    Sweep {
        #[arg(long, default_value_t = 60)]
        grace_mins: i64,
        /// Actually delete (default is dry-run)
        #[arg(long)]
        yes: bool,
    },
    /// Torture the ref protocol with injected faults; verify invariants
    Chaos {
        #[arg(long, default_value_t = 2000)]
        iterations: u64,
        #[arg(long, default_value_t = 1)]
        seed: u64,
        /// Probability of each fault type per backend call
        #[arg(long, default_value_t = 0.15)]
        fail_prob: f64,
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
    /// Log operations (minimal Comb Log, single partition)
    #[command(subcommand)]
    Log(LogCommand),
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
        #[arg(long)]
        operation: Option<String>,
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
        #[arg(long)]
        operation: Option<String>,
    },
}

#[derive(Subcommand)]
enum BackendCommand {
    /// Run the conformance suite against the configured backend
    Test,
}

#[derive(Subcommand)]
enum LogCommand {
    /// Append one or more events; acknowledged only after the manifest ref advances
    Append {
        name: String,
        events: Vec<String>,
        #[arg(long, default_value = "cli")]
        writer: String,
        #[arg(long, default_value_t = 60)]
        lease: i64,
        #[arg(long)]
        operation: Option<String>,
        /// Hex-encoded opaque stable key (complete-feed logs only)
        #[arg(long)]
        stable_key: Option<String>,
        #[arg(long)]
        complete: bool,
    },
    /// Read events in order from a sequence position
    Read {
        name: String,
        #[arg(long, default_value_t = 1)]
        from: u64,
    },
    /// Follow the log live (durable tail -f); Ctrl-C to stop
    Follow {
        name: String,
        #[arg(long, default_value_t = 1)]
        from: u64,
        #[arg(long, default_value_t = 500)]
        poll_ms: u64,
    },
    /// Show head sequence, epoch, leader, and chunk count
    Status { name: String },
    /// Take over leadership at a new epoch (fences the old leader)
    Steal {
        name: String,
        #[arg(long)]
        writer: String,
        #[arg(long)]
        operation: Option<String>,
    },
    /// Merge WAL chunks into a segment (representation only, contents unchanged)
    Compact {
        name: String,
        #[arg(long)]
        operation: Option<String>,
    },
    /// Advance the retention floor; readers below it get Trimmed{resume_at}
    Trim {
        name: String,
        #[arg(long)]
        before: u64,
        #[arg(long)]
        operation: Option<String>,
    },
    /// Throughput benchmark through the group-commit writer
    Bench {
        name: String,
        #[arg(long, default_value_t = 2000)]
        events: usize,
        #[arg(long, default_value_t = 8)]
        producers: usize,
        #[arg(long, default_value_t = 64)]
        payload_bytes: usize,
        #[arg(long, default_value_t = 10)]
        window_ms: u64,
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
        #[arg(long)]
        operation: Option<String>,
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

    if let Command::Chaos {
        iterations,
        seed,
        fail_prob,
    } = &cli.command
    {
        println!(
            "chaos: {iterations} iterations, seed {seed}, fault probability {fail_prob} per call\n\
             (in-memory backend; every operation may lose its request or its response)\n"
        );
        let report = combctl::chaos::run(*iterations, *seed, *fail_prob, true).await?;
        println!("\n  operations acked      {}", report.acked);
        println!(
            "  clean failures        {}  (LeaseHeld / stale CAS — correct rejections)",
            report.clean_failures
        );
        println!("  faults injected       {}", report.injected_faults);
        println!(
            "  ambiguous acks        {}  (committed but caller saw an error)",
            report.ambiguous_acks
        );
        println!("  invariant violations  {}", report.violations.len());
        for v in &report.violations {
            println!("    VIOLATION: {v}");
        }
        if report.violations.is_empty() {
            println!("\nall invariants held. generation and epoch never went backwards,\nno stale fence advanced state, committed state never became unreadable,\njournal chain intact and digest-verified.");
        } else {
            std::process::exit(2);
        }
        return Ok(());
    }

    if let Command::Init {
        tenant,
        backend,
        root,
        bucket,
        region,
        profile,
        prefix,
        endpoint,
    } = &cli.command
    {
        let backend = match backend.as_str() {
            "local" => BackendConfig::Local {
                root: root
                    .clone()
                    .ok_or_else(|| anyhow!("--root is required for local backend"))?,
            },
            "s3" => BackendConfig::S3 {
                bucket: bucket
                    .clone()
                    .ok_or_else(|| anyhow!("--bucket is required for s3 backend"))?,
                region: region
                    .clone()
                    .ok_or_else(|| anyhow!("--region is required for s3 backend"))?,
                profile: profile.clone(),
                prefix: prefix.clone(),
                endpoint: endpoint.clone(),
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
    let store = Store::new(backend, cfg.tenant.clone(), key, cache_dir);

    match cli.command {
        Command::Init { .. } | Command::Chaos { .. } => unreachable!(),
        Command::Backend(BackendCommand::Test) => {
            let prefix = format!(
                "comb/v1/tenants/{}/conformance/{}",
                store.tenant,
                chrono::Utc::now().format("%Y%m%dT%H%M%S")
            );
            println!("running conformance suite (key prefix {prefix})...\n");
            let results = comb_object::conformance::run(store.backend.clone(), &prefix).await;
            let mut failed = 0;
            for r in &results {
                println!(
                    "  {}  {:<45} {}",
                    if r.passed { "PASS" } else { "FAIL" },
                    r.name,
                    if r.passed {
                        String::new()
                    } else {
                        r.detail.clone()
                    }
                );
                if !r.passed {
                    failed += 1;
                }
            }
            println!();
            if failed == 0 {
                println!("backend CONFORMS ({} checks)", results.len());
            } else {
                println!(
                    "backend DOES NOT CONFORM: {failed}/{} checks failed",
                    results.len()
                );
                println!("this backend MUST NOT be used as an authoritative Comb backend");
                std::process::exit(2);
            }
        }
        Command::Put { file } => {
            let bytes = std::fs::read(&file)?;
            let size = bytes.len();
            let (digest, dedup) = store.put_blob(bytes).await?;
            println!("{digest}");
            eprintln!(
                "{size} bytes stored{}",
                if dedup {
                    " (already present — deduplicated)"
                } else {
                    ""
                }
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
                        if source == GetSource::Cache {
                            "cache"
                        } else {
                            "backend"
                        }
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
            let no_cache = Store::new(
                store.backend.clone(),
                store.tenant.clone(),
                store.key.clone(),
                None,
            );
            let (payload, _) = no_cache.get_blob(&digest).await?;
            println!("ok: {digest} verified ({} bytes)", payload.len());
        }
        Command::Ref(RefCommand::Get { name }) => match store.read_ref(&name).await? {
            None => println!("ref {name} does not exist"),
            Some((value, _)) => println!("{}", serde_json::to_string_pretty(&value)?),
        },
        Command::Ref(RefCommand::Set {
            name,
            digest,
            fence,
            operation,
        }) => {
            let digest = Digest::parse(&digest)?;
            let op = take_op(&store, operation)?;
            let value = store.set_target_op(op, &name, digest, fence).await?;
            println!(
                "{} -> {} (generation {}, epoch {}, first {})",
                name,
                value.value.target.as_ref().unwrap(),
                value.generation,
                value.epoch,
                value.first_delivery
            );
        }
        Command::Ref(RefCommand::History { name, max }) => {
            let entries = store.history(&name, max).await?;
            if entries.is_empty() {
                println!("no commit history for {name}");
            }
            for e in entries {
                println!(
                    "gen {:>4}  epoch {:>3}  {}  {}  {}",
                    e.generation,
                    e.epoch,
                    e.at.format("%Y-%m-%d %H:%M:%S"),
                    e.identity,
                    e.commit,
                );
            }
        }
        Command::Log(cmd) => {
            use combctl::log::LogStore;
            match cmd {
                LogCommand::Append {
                    name,
                    events,
                    writer,
                    lease,
                    operation,
                    stable_key,
                    complete,
                } => {
                    if events.is_empty() {
                        return Err(anyhow!("nothing to append"));
                    }
                    let log = if complete {
                        LogStore::complete_feed(&store, &name)
                    } else {
                        LogStore::new(&store, &name)
                    };
                    let payloads: Vec<Vec<u8>> =
                        events.into_iter().map(|e| e.into_bytes()).collect();
                    if let Some(hex_key) = stable_key {
                        if payloads.len() != 1 {
                            return Err(anyhow!("stable append takes exactly one payload"));
                        }
                        let key = comb_core::StableKey::from_hex(&hex_key)?;
                        let rec = log.append_stable(key, &writer, &payloads[0], lease).await?;
                        println!(
                            "stable appended seq {}..{} generation {}",
                            rec.range.first, rec.range.last, rec.generation
                        );
                    } else {
                        let op = take_op(&store, operation)?;
                        let a = log.append(op, &writer, &payloads, lease).await?;
                        println!(
                            "appended seq {}..{} ({} events) as {writer} generation {}",
                            a.first,
                            a.last,
                            a.last - a.first + 1,
                            a.generation
                        );
                    }
                }
                LogCommand::Read { name, from } => {
                    let log = LogStore::new(&store, &name);
                    for f in log.read(from).await? {
                        let text = String::from_utf8_lossy(&f.payload);
                        println!("{:>6}  {}  {}", f.seq, f.at.format("%H:%M:%S%.3f"), text);
                    }
                }
                LogCommand::Follow {
                    name,
                    from,
                    poll_ms,
                } => {
                    let log = LogStore::new(&store, &name);
                    eprintln!(
                        "following {name} from seq {from} (poll {poll_ms}ms, Ctrl-C to stop)"
                    );
                    log.follow(
                        from,
                        poll_ms,
                        |f| {
                            let text = String::from_utf8_lossy(&f.payload);
                            println!("{:>6}  {}  {}", f.seq, f.at.format("%H:%M:%S%.3f"), text);
                        },
                        || false,
                    )
                    .await?;
                }
                LogCommand::Status { name } => {
                    let log = LogStore::new(&store, &name);
                    match log.status().await? {
                        None => println!("log {name} does not exist"),
                        Some((value, manifest)) => {
                            let leader = value
                                .lease
                                .as_ref()
                                .map(|l| {
                                    format!(
                                        "{} (until {})",
                                        l.writer,
                                        l.lease_until.format("%H:%M:%S")
                                    )
                                })
                                .unwrap_or_else(|| "none".into());
                            println!(
                                "head_seq {}  epoch {}  chunks {}  leader {}",
                                manifest.head_seq,
                                value.epoch,
                                manifest.chunks.len(),
                                leader
                            );
                        }
                    }
                }
                LogCommand::Steal {
                    name,
                    writer,
                    operation,
                } => {
                    let log = LogStore::new(&store, &name);
                    let op = take_op(&store, operation)?;
                    let epoch = log.steal(op, &writer, 60).await?;
                    println!("leadership taken by {writer} at epoch {epoch}");
                }
                LogCommand::Compact { name, operation } => {
                    let log = LogStore::new(&store, &name);
                    let op = take_op(&store, operation)?;
                    let merged = log.compact(op).await?;
                    if merged == 0 {
                        println!("nothing to compact (fewer than 2 WAL chunks)");
                    } else {
                        println!("compacted {merged} chunks into 1 segment; contents unchanged; old chunks are now orphans");
                    }
                }
                LogCommand::Trim {
                    name,
                    before,
                    operation,
                } => {
                    let log = LogStore::new(&store, &name);
                    let op = take_op(&store, operation)?;
                    let floor = log.trim_before(op, before).await?;
                    println!(
                        "retention floor is now {floor}; reads resume at {}",
                        floor + 1
                    );
                }
                LogCommand::Bench {
                    name,
                    events,
                    producers,
                    payload_bytes,
                    window_ms,
                } => {
                    use combctl::log::GroupWriter;
                    let payload = "x".repeat(payload_bytes);
                    let (writer, task) =
                        GroupWriter::spawn(store.clone(), &name, "bench".into(), window_ms, 2000);
                    let writer = std::sync::Arc::new(writer);
                    let per = events / producers;
                    println!(
                        "bench: {events} events, {producers} producers, {payload_bytes}B payloads, {window_ms}ms commit window"
                    );
                    let start = std::time::Instant::now();
                    let mut tasks = Vec::new();
                    for _ in 0..producers {
                        let writer = writer.clone();
                        let payload = payload.clone();
                        tasks.push(tokio::spawn(async move {
                            let mut latencies = Vec::with_capacity(per);
                            for _ in 0..per {
                                let t0 = std::time::Instant::now();
                                let op = comb_core::OperationId::mint(&comb_core::SystemClock);
                                writer
                                    .submit(op, vec![payload.clone().into_bytes()])
                                    .await
                                    .unwrap();
                                latencies.push(t0.elapsed());
                            }
                            latencies
                        }));
                    }
                    let mut latencies = Vec::new();
                    for t in tasks {
                        latencies.extend(t.await?);
                    }
                    let wall = start.elapsed();
                    drop(writer);
                    let stats = task.await?;
                    latencies.sort();
                    let p = |q: f64| latencies[((latencies.len() - 1) as f64 * q) as usize];
                    println!("\n  wall time        {:.2}s", wall.as_secs_f64());
                    println!("  events acked     {}", stats.events);
                    println!(
                        "  events/sec       {:.0}",
                        stats.events as f64 / wall.as_secs_f64()
                    );
                    println!(
                        "  commits (CAS)    {}  (avg batch {:.1} events)",
                        stats.commits,
                        stats.events as f64 / stats.commits.max(1) as f64
                    );
                    println!("  CAS conflicts    {}", stats.conflicts);
                    println!("  ack latency p50  {:.0}ms", p(0.5).as_millis());
                    println!("  ack latency p99  {:.0}ms", p(0.99).as_millis());
                }
            }
        }
        Command::Sweep { grace_mins, yes } => {
            let report = combctl::sweep::sweep(&store, grace_mins, yes).await?;
            println!(
                "refs {}  objects {}  reachable {}  in-grace {}  candidates {}",
                report.refs_scanned,
                report.objects_scanned,
                report.reachable,
                report.in_grace,
                report.candidates.len()
            );
            for key in report.candidates.iter().take(20) {
                println!("  orphan: ...{}", &key[key.len().saturating_sub(24)..]);
            }
            if report.candidates.len() > 20 {
                println!("  ... and {} more", report.candidates.len() - 20);
            }
            if yes {
                println!("deleted {} objects", report.deleted);
            } else if !report.candidates.is_empty() {
                println!("dry-run: pass --yes to delete");
            }
        }
        Command::Claim {
            name,
            writer,
            ttl,
            steal,
            operation,
        } => {
            let op = take_op(&store, operation)?;
            let value = store.claim(op, &name, &writer, ttl, steal).await?;
            let lease = value.value.lease.as_ref().unwrap();
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
        Command::Release {
            name,
            fence,
            operation,
        } => {
            let op = take_op(&store, operation)?;
            let value = store.release(op, &name, fence).await?;
            println!("released {name} (generation {})", value.generation);
        }
    }
    Ok(())
}

fn take_op(store: &combctl::store::Store, flag: Option<String>) -> Result<OperationId> {
    match flag {
        Some(s) => Ok(OperationId::parse(&s)?),
        None => {
            let op = store.mint_operation();
            eprintln!("operation: {op}");
            Ok(op)
        }
    }
}
