// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

/// Parquet backend for the Provenance service
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use swh_provenance::database::ProvenanceDatabase;

use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long)]
    cache_parquet: bool,
    /// Path to the provenance database
    database: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    let fmt_layer = tracing_subscriber::fmt::layer();
    let filter_layer = tracing_subscriber::EnvFilter::try_from_default_env()
        .or_else(|_| tracing_subscriber::EnvFilter::try_new("info"))
        .unwrap();

    let logger = tracing_subscriber::registry();

    //#[cfg(feature = "sentry")]
    //let logger = logger.with(sentry_layer);

    logger
        .with(filter_layer)
        .with(fmt_layer)
        .try_init()
        .context("Could not initialize logging")?;

    let args = Args::parse();

    let ctx = ProvenanceDatabase::new(args.database, args.cache_parquet)
        .await
        .context("Could not initialize provenance database")?
        .ctx;
    for i in 0..100 {
        tracing::info!("Iteration {i}/100");
        let df = if i % 10 == 0 {
            ctx.sql("EXPLAIN ANALYZE SELECT cnt, dir FROM c_in_d WHERE cnt = 8480961860;")
                .await
                .context("SQL query failed")?
        } else {
            ctx.sql("SELECT cnt, dir FROM c_in_d WHERE cnt = 8480961860;")
                .await
                .context("SQL query failed")?
        };
        for batch in df.collect().await? {
            tracing::debug!("{:?}", batch)
        }
    }

    Ok(())
}
