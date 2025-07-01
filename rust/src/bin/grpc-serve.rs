// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::io::BufReader;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};
use mimalloc::MiMalloc;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use swh_graph::graph::SwhBidirectionalGraph;
use swh_graph::properties;
use swh_graph::SwhGraphProperties;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc; // Allocator recommended by Datafusion

/// On-disk format of the graph. This should always be `Webgraph` unless testing.
#[derive(ValueEnum, Clone, Debug)]
enum GraphFormat {
    Webgraph,
    Json,
}

#[derive(Parser, Debug)]
#[command(about = "gRPC server for the Software Heritage Provenance Index", long_about = None)]
struct Args {
    #[arg(long, value_enum, default_value_t = GraphFormat::Webgraph)]
    graph_format: GraphFormat,
    #[arg(long)]
    /// Path to the graph prefix
    graph: PathBuf,
    #[arg(long)]
    /// URL to the provenance database (which may be a file:// URL)
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
            match args.graph_format {
                GraphFormat::Webgraph => {
                    let (graph, db) = tokio::join!(
                        tokio::task::spawn_blocking(|| {
                            swh_provenance::utils::load_graph_properties(args.graph)
                        }),
                        tokio::task::spawn(swh_provenance::utils::load_database(
                            args.database,
                            indexes
                        )),
                    );

                    let graph = graph.expect("Could not join graph load task")?;
                    let db = db.expect("Could not join graph load task")?;

                    log::info!("Starting server");
                    swh_provenance::grpc_server::serve(db, graph, args.bind, statsd_client).await?;
                }
                GraphFormat::Json => {
                    let (graph, db) = tokio::join!(
                        tokio::task::spawn_blocking(move || -> Result<_> {
                            let file = std::fs::File::open(&args.graph).with_context(|| {
                                format!("Could not open {}", args.graph.display())
                            })?;
                            let mut deserializer =
                                serde_json::Deserializer::from_reader(BufReader::new(file));
                            swh_graph::serde::deserialize_with_labels_and_maps(
                                &mut deserializer,
                                args.graph.clone(),
                            )
                            .map_err(|e| anyhow!("Could not read JSON graph: {e}"))
                        }),
                        tokio::task::spawn(swh_provenance::utils::load_database(
                            args.database,
                            indexes
                        )),
                    );

                    let graph: SwhBidirectionalGraph<
                        SwhGraphProperties<
                            _,
                            properties::VecTimestamps,
                            properties::VecPersons,
                            properties::VecContents,
                            properties::VecStrings,
                            properties::VecLabelNames,
                        >,
                        _,
                        _,
                    > = graph.expect("Could not join graph load task")?;
                    let db = db.expect("Could not join graph load task")?;

                    log::info!("Starting server");
                    swh_provenance::grpc_server::serve(db, graph, args.bind, statsd_client).await?;
                }
            }

            Ok(())
        })
}
