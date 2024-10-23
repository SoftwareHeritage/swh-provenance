// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::io::Read;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use mimalloc::MiMalloc;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use swh_graph::mph::SwhidMphf;
use swh_graph::properties;
use swh_graph::SwhGraphProperties;

use swh_provenance::database::ProvenanceDatabase;

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
    graph: PathBuf,
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
            log::info!("Loading graph properties and database");
            let (graph_properties, db) = tokio::join!(
                tokio::task::spawn_blocking(|| load_graph_properties(args.graph)),
                tokio::task::spawn(load_database(args.database, indexes)),
            );

            let graph_properties = graph_properties.expect("Could not join graph load task")?;
            let db = db.expect("Could not join graph load task")?;

            log::info!("Starting server");
            swh_provenance::grpc_server::serve(db, graph_properties, args.bind, statsd_client)
                .await?;

            Ok(())
        })
}

fn load_graph_properties(
    graph_path: PathBuf,
) -> Result<
    SwhGraphProperties<
        properties::MappedMaps<impl SwhidMphf>,
        properties::NoTimestamps,
        properties::NoPersons,
        properties::NoContents,
        properties::NoStrings,
        properties::NoLabelNames,
    >,
> {
    let node_count_path = graph_path.with_extension("nodes.count.txt");
    let mut num_nodes = String::new();
    std::fs::File::open(&node_count_path)
        .with_context(|| format!("Could not open {}", node_count_path.display()))?
        .read_to_string(&mut num_nodes)
        .with_context(|| format!("Could not read {}", node_count_path.display()))?;
    let num_nodes = num_nodes.strip_suffix('\n').unwrap_or(&num_nodes);
    let num_nodes = num_nodes.parse().with_context(|| {
        format!(
            "Could not parse content of {} as an integer",
            node_count_path.display()
        )
    })?;
    let graph_properties = SwhGraphProperties::new(graph_path, num_nodes)
        .load_maps::<swh_graph::mph::DynMphf>()
        .context("Could not load graph maps")?;
    log::info!("Graph loaded");
    Ok(graph_properties)
}

async fn load_database(database_url: url::Url, indexes_path: PathBuf) -> Result<ProvenanceDatabase> {
    let db = swh_provenance::database::ProvenanceDatabase::new(database_url, &indexes_path)
        .await
        .context("Could not initialize provenance database")?;
    db.mmap_ef_indexes()
        .context("Could not mmap Elias-Fano indexes")?;
    log::info!("Database loaded");
    Ok(db)
}
