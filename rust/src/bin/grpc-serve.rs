// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use mimalloc::MiMalloc;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use swh_graph::graph::SwhBidirectionalGraph;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc; // Allocator recommended by Datafusion

#[derive(Parser, Debug)]
#[command(about = "gRPC server for the Software Heritage Provenance Index", long_about = None)]
struct Args {
    #[arg(long)]
    /// Keep Parquet metadata in RAM between queries, instead of re-parsing them every time
    cache_parquet: bool,
    #[arg(long)]
    /// Path to the graph prefix
    graph: Option<PathBuf>,
    #[arg(long)]
    /// Path to the provenance database
    database: url::Url,
    #[arg(long)]
    /// Path to Elias-Fano indexes, default to `--database` (when it is a file:// URL)
    indexes: Option<PathBuf>,
    #[arg(long, default_value = "[::]:50141")]
    bind: std::net::SocketAddr,
    #[arg(long)]
    /// Defaults to `localhost:8125` (or whatever is configured by the `STATSD_HOST`
    /// and `STATSD_PORT` environment variables).
    statsd_host: Option<String>,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    let indexes = args
        .indexes
        .or_else(|| args.database.to_file_path().ok())
        .context("--indexes must be provided when --database is not a file:// URL")?;

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
            if args.graph.is_some() {
                log::info!("Loading graph and database");
            } else {
                log::info!("Loading database");
            }
            let (graph, db) = tokio::join!(
                tokio::task::spawn_blocking(|| {
                    let graph = args
                        .graph
                        .map(|graph_path| {
                            SwhBidirectionalGraph::new(graph_path)
                                .context("Could not load graph")?
                                .init_properties()
                                .load_properties(|props| {
                                    props.load_maps::<swh_graph::mph::DynMphf>()
                                })
                                .context("Could not load graph maps")
                        })
                        .transpose()
                        .context("Could not load graph");
                    match graph {
                        Ok(Some(_)) => log::info!("Graph loaded"),
                        Ok(None) => {
                            log::warn!("--graph not given, will use slow fallback for node lookup")
                        }
                        Err(_) => (),
                    }
                    graph
                }),
                tokio::task::spawn(async move {
                    let db =
                        swh_provenance::database::ProvenanceDatabase::new(args.database, &indexes)
                            .await
                            .context("Could not initialize provenance database");
                    if let Ok(ref db) = db {
                        db.mmap_ef_indexes()
                            .context("Could not mmap Elias-Fano indexes")?;
                        log::info!("Database loaded");
                    }
                    db
                })
            );

            let graph = graph.expect("Could not join graph load task")?;
            let db = db.expect("Could not join graph load task")?;

            log::info!("Starting server");
            swh_provenance::grpc_server::serve(db, graph, args.bind, statsd_client).await?;

            Ok(())
        })
}
