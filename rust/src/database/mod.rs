// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

/// Parquet backend for the Provenance service
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use datafusion::prelude::{SessionConfig, SessionContext};

mod parquet;
use parquet::CachingParquetFormatFactory;

pub struct ProvenanceDatabase {
    pub ctx: SessionContext,
}

impl ProvenanceDatabase {
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        let config =
            SessionConfig::new().set_bool("datafusion.execution.parquet.pushdown_filters", true);
        let ctx = SessionContext::new_with_config(config);

        // Use the same underlying ParquetFormatFactory so they share their configuration
        let parquet_format_factory = ctx
            .state()
            .get_file_format_factory("parquet")
            .context("Could not get Parquet File Format")?;

        ctx.state()
            .register_file_format(
                Arc::new(CachingParquetFormatFactory::new(parquet_format_factory)),
                true, // overwrite
            )
            .context("Could not register CachingParquetFormatFactory")?;

        let table_path = |table_name| {
            path.join(table_name)
                .into_os_string()
                .into_string()
                .map_err(|table_path| anyhow!("Could not parse table path {table_path:?} as UTF8"))
        };

        let options = datafusion::datasource::file_format::options::ParquetReadOptions {
            parquet_pruning: Some(true),
            ..Default::default()
        };
        ctx.register_parquet("node", &table_path("nodes")?, options.clone())
            .await
            .context("Could not register 'node' table")?;

        ctx.sql(&format!(
            "
            CREATE EXTERNAL TABLE c_in_d
            STORED AS PARQUET
            LOCATION '{}'
            ",
            table_path("contents_in_frontier_directories")?
        ))
        .await
        .context("Could not register 'c_in_d' table")?;
        /*
        ctx.register_parquet(
            "c_in_d",
            &table_path("contents_in_frontier_directories")?,
            options.clone(),
        )
        .await
        .context("Could not register 'c_in_d' table")?;

        /*
        ctx.register_parquet(
            "d_in_r",
            &table_path("frontier_directories_in_revisions")?,
            options.clone(),
        )
        .await
        .context("Could not register 'd_in_r' table")?;

        ctx.register_parquet(
            "c_in_r",
            &table_path("contents_in_revisions_without_frontiers")?,
            options.clone(),
        )
        .await
        .context("Could not register 'c_in_r' table")?;
        */

        Ok(Self { ctx })
    }
}
