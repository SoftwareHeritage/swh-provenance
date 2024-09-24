// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

/// Parquet backend for the Provenance service
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use datafusion::datasource::file_format::parquet::ParquetFormatFactory;
use datafusion::prelude::{SessionConfig, SessionContext};

mod caching_parquet_format;
use caching_parquet_format::CachingParquetFormatFactory;
mod pooled_reader;
use pooled_reader::ParquetFileReaderPool;
mod caching_parquet_reader;
use caching_parquet_reader::CachingParquetFileReaderFactory;

pub struct ProvenanceDatabase {
    pub ctx: SessionContext,
}

impl ProvenanceDatabase {
    pub async fn new(path: impl AsRef<Path>, cache_parquet: bool) -> Result<Self> {
        let path = path.as_ref();

        let config =
            SessionConfig::new().set_bool("datafusion.execution.parquet.pushdown_filters", true);
        let ctx = SessionContext::new_with_config(config);

        // Use the same underlying ParquetFormatFactory so they share their configuration
        let parquet_format_factory = ctx
            .state_ref()
            .read()
            .get_file_format_factory("parquet")
            .context("Could not get Parquet File Format")?;
        assert!(
            ctx.state_ref()
                .read()
                .get_file_format_factory("parquet")
                .unwrap()
                .as_any()
                .downcast_ref::<ParquetFormatFactory>()
                .is_some(),
            "unexpected type of parquet factory"
        );
        let caching_parquet_format_factory = Arc::new(CachingParquetFormatFactory::new(
            parquet_format_factory,
            CachingParquetFileReaderFactory::new,
        ));

        if cache_parquet {
            ctx.state_ref()
                .write()
                .register_file_format(
                    caching_parquet_format_factory,
                    true, // overwrite
                )
                .context("Could not register CachingParquetFormatFactory")?;
        }

        {
            let ctx = &ctx;
            futures::future::join_all(
                [
                    ("node", "nodes"),
                    ("c_in_d", "contents_in_frontier_directories"),
                    ("d_in_r", "frontier_directories_in_revisions"),
                    ("c_in_r", "contents_in_revisions_without_frontiers"),
                ]
                .into_iter()
                .map(|(table_name, dir_name)| async move {
                    let table_path = path.join(dir_name);
                    let Ok(table_path) = table_path.clone().into_os_string().into_string() else {
                        bail!("Could not parse table path {table_path:?} as UTF8");
                    };
                    ctx.sql(&format!(
                        "
                    CREATE EXTERNAL TABLE {}
                    STORED AS PARQUET
                    LOCATION '{}'
                    ",
                        table_name, table_path,
                    ))
                    .await
                    .with_context(|| {
                        format!(
                            "Could not register '{}' table from {}",
                            table_name, table_path
                        )
                    })
                }),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        }

        Ok(Self { ctx })
    }
}
