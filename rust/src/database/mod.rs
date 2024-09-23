// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

/// Parquet backend for the Provenance service
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use datafusion::datasource::file_format::parquet::ParquetFormatFactory;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::ObjectStore;

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
        let caching_parquet_format_factory: Arc<
            CachingParquetFormatFactory<
                CachingParquetFileReaderFactory,
                fn(Arc<dyn ObjectStore>) -> CachingParquetFileReaderFactory,
            >,
        > = Arc::new(CachingParquetFormatFactory::new(
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

            assert!(
                ctx.state_ref()
                    .read()
                    .get_file_format_factory("parquet")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<CachingParquetFormatFactory<
                        CachingParquetFileReaderFactory,
                        fn(Arc<dyn ObjectStore>) -> CachingParquetFileReaderFactory,
                    >>()
                    .is_some(),
                "didn't overwrite the parquet factory"
            );
        }

        let table_path = |table_name| {
            //  datafusion::datasource::listing::ListingTableUrl::parse(format!(
            //      "file://{}",
            path.join(table_name)
                .into_os_string()
                .into_string()
                .map_err(|table_path| anyhow!("Could not parse table path {table_path:?} as UTF8"))
            // ?
            //  ))
            //  .context("Could not parse file path in a file:// URL")
        };

        let options = datafusion::datasource::file_format::options::ParquetReadOptions {
            parquet_pruning: Some(true),
            ..Default::default()
        };
        ctx.register_parquet("node", &table_path("nodes")?, options.clone())
            .await
            .context("Could not register 'node' table")?;

        /*
        let config = ListingTableConfig::new(table_path("contents_in_frontier_directories")?)
            .infer_options(&ctx.state())
            .await
            .context("Could not infer options from contents_in_frontier_directories")?;
        assert!(config
            .options
            .expect("Missing ListingOptions")
            .format
            .as_any()
            .downcast_ref::<ParquetFormat>()
            .is_some());
        //config.options.format = caching_parquet_format_factory.
        */
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
        */

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
