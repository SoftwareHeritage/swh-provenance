// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use clap::Parser;
use dsi_progress_logger::{progress_logger, ProgressLog};
use epserde::ser::Serialize;
use mimalloc::MiMalloc;
use tokio::task::JoinSet;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc; // Allocator recommended by Datafusion

#[derive(Parser, Debug)]
#[command(about = "Builds .ef indexes for extra quick querying of the Software Heritage Provenance Index", long_about = None)]
struct Args {
    #[arg(long)]
    /// URL to the provenance database (which may be a file:// URL)
    database: url::Url,
    #[arg(long)]
    /// Path to the directory where to write paths to. Defaults to `--database` (when it is a file:// URL).
    indexes: Option<PathBuf>,
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

    let _statsd_client = swh_provenance::statsd::statsd_client(args.statsd_host)?;

    // can't use #[tokio::main] because Sentry must be initialized before we start the tokio runtime
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async move {
            log::info!("Loading database...");
            let db = swh_provenance::database::ProvenanceDatabase::new(args.database, &indexes)
                .await
                .context("Could not initialize provenance database")?;
            log::info!("Database loaded.");

            let tables = [
                (db.c_in_d, "cnt"),
                (db.d_in_r, "dir"),
                (db.c_in_r, "cnt"),
                (db.r_in_o, "revrel"),
            ];
            let mut pl = progress_logger!(
                item_name = "index",
                display_memory = true,
                local_speed = true,
                expected_updates = Some(tables.iter().map(|(table, _col)| table.files.len()).sum()),
            );
            pl.start("Building and writing indexes...");
            let shared_pl = Arc::new(Mutex::new(pl));

            let mut tasks = Vec::new();
            for (table, key_column) in tables {
                let ef_index_path = table
                    .files
                    .first()
                    .expect("Table has no files")
                    .ef_index_path(key_column);
                let ef_indexes_directory = ef_index_path
                    .parent()
                    .expect("ef index path is not in a directory");
                std::fs::create_dir_all(ef_indexes_directory).with_context(|| {
                    format!("Could not create {}", ef_indexes_directory.display())
                })?;
                for file in table.files {
                    let shared_pl = shared_pl.clone();
                    tasks.push(tokio::task::spawn(async move {
                        // Build index
                        let ef_values = file
                            .build_ef_index(key_column)
                            .await
                            .context("Could not build Elias-Fano index")?;

                        // Write index to disk
                        let index_path = file.ef_index_path(key_column);
                        let mut ef_file =
                            std::fs::File::create_new(&index_path).with_context(|| {
                                format!("Could not create {}", index_path.display())
                            })?;
                        ef_values.serialize(&mut ef_file).with_context(|| {
                            format!(
                                "Could not serialize {} index to {}",
                                file.object_meta().location,
                                index_path.display()
                            )
                        })?;
                        shared_pl.lock().unwrap().update();
                        Ok(())
                    }));
                }
            }

            tasks
                .into_iter()
                .collect::<JoinSet<_>>()
                .join_all()
                .await
                .into_iter()
                .collect::<Result<Result<Vec<_>>, tokio::task::JoinError>>()
                .expect("Could not join task")?;

            shared_pl.lock().unwrap().done();

            log::info!("Index built.");
            Ok(())
        })
}
