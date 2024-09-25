// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc; // Allocator recommended by Datafusion

#[derive(Parser, Debug)]
#[command(about = "gRPC server for the Software Heritage Provenance Index", long_about = None)]
struct Args {
    #[arg(long)]
    /// Keep Parquet metadata in RAM between queries, instead of re-parsing them every time
    cache_parquet: bool,
    #[arg(long)]
    /// Runs a few queries and exits instead of starting a gRPC server
    benchmark: bool,
    /// Path to the provenance database
    database: PathBuf,
    #[arg(long, default_value = "[::]:50141")]
    bind: std::net::SocketAddr,
    #[arg(long)]
    /// Defaults to `localhost:8125` (or whatever is configured by the `STATSD_HOST`
    /// and `STATSD_PORT` environment variables).
    statsd_host: Option<String>,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    let fmt_layer = tracing_subscriber::fmt::layer();
    let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("info"))
        .unwrap();

    let logger = tracing_subscriber::registry();

    #[cfg(feature = "sentry")]
    let (_guard, sentry_layer) = swh_provenance::sentry::setup();

    #[cfg(feature = "sentry")]
    let logger = logger.with(sentry_layer);

    logger
        .with(filter_layer)
        .with(fmt_layer)
        .try_init()
        .context("Could not initialize logging")?;

    let statsd_client = swh_provenance::statsd::statsd_client(args.statsd_host)?;

    // can't use #[tokio::main] because Sentry must be initialized before we start the tokio runtime
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            log::info!("Loading Database");
            let db = swh_provenance::database::ProvenanceDatabase::new(
                args.database,
                args.cache_parquet,
            )
            .await
            .context("Could not initialize provenance database")?;

            if args.benchmark {
                for i in 0..100 {
                    tracing::info!("Iteration {i}/100");
                    let df = if i % 10 == 0 {
                        db.ctx.sql(
                            "EXPLAIN ANALYZE SELECT cnt, dir FROM c_in_d WHERE cnt = 8480961860;",
                        )
                        .await
                        .context("SQL query failed")?
                    } else {
                        db.ctx
                            .sql("SELECT cnt, dir FROM c_in_d WHERE cnt = 8480961860;")
                            .await
                            .context("SQL query failed")?
                    };
                    for batch in df.collect().await? {
                        tracing::debug!("{:?}", batch)
                    }
                }
            } else {
                log::info!("Starting server");
                swh_provenance::grpc_server::serve(db, args.bind, statsd_client).await?
            }

            Ok(())
        })
}
