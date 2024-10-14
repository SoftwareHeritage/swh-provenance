// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;
use std::sync::OnceLock;

use anyhow::{ensure, Context, Result};
use arrow::array::*;
use arrow::datatypes::*;
use futures::{StreamExt, TryStreamExt};
use itertools::Itertools;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ProjectionMask;
use rdst::RadixSort;
use sux::dict::elias_fano::{EfDict, EfSeq, EliasFanoBuilder};

use super::caching_parquet_reader::CachingParquetFileReader;
use super::pooled_reader::ParquetFileReaderPool;

/// Stores the list of values of a column in memory, for file-level pruning
struct EfIndex {
    column_name: &'static str,
    values: EfDict,
}

pub struct FileReader {
    store: Arc<dyn ObjectStore>,
    object_meta: Arc<ObjectMeta>,
    pool: ParquetFileReaderPool<CachingParquetFileReader<ParquetObjectReader>>,
    ef_index: OnceLock<EfIndex>,
}

impl FileReader {
    pub async fn new(store: Arc<dyn ObjectStore>, object_meta: Arc<ObjectMeta>) -> Self {
        Self {
            store,
            object_meta,
            pool: ParquetFileReaderPool::default(),
            ef_index: OnceLock::new(),
        }
    }

    pub async fn reader(&self) -> Result<impl AsyncFileReader> {
        self.pool.try_get_reader(|| {
            Ok(CachingParquetFileReader::new(
                ParquetObjectReader::new(Arc::clone(&self.store), (*self.object_meta).clone())
                    .with_preload_column_index(true)
                    .with_preload_offset_index(true),
            ))
        })
    }

    pub fn ef_index(&self, column_name: &'static str) -> Option<&EfDict> {
        let index = self.ef_index.get()?;
        if index.column_name == column_name {
            Some(&index.values)
        } else {
            None
        }
    }

    pub async fn load_ef_index(&self, column_name: &'static str) -> Result<()> {
        let reader = self.reader().await.context("Could not get reader")?;

        let stream_builder = ParquetRecordBatchStreamBuilder::new_with_options(
            reader,
            ArrowReaderOptions::new().with_page_index(false),
        )
        .await
        .context("Could not get stream builder")?;
        let schema = stream_builder.schema();
        let parquet_schema = stream_builder.parquet_schema(); // clone

        /*
        let row_groups_metadata = stream_builder.metadata().row_groups();
        let max_value = arrow::compute::max(
            StatisticsConverter::try_new(column_name, schema, parquet_schema)
                .context("Could not build statistics converter")?
                .row_group_maxes(row_groups_metadata)
                .context("Could not read row group maxes")?
                .as_primitive_opt::<UInt64Type>()
                .with_context(|| {
                    format!("{} column could not be cast as UInt64Array", column_name)
                })?,
        )
        .with_context(|| format!("{} column contains null statistics", column_name))?;
        */

        let column_idx = parquet_schema
            .columns()
            .iter()
            .position(|column| column.name() == column_name)
            .with_context(|| format!("{:?} has no column named {}", schema, column_name))?;

        let projection_mask = ProjectionMask::roots(parquet_schema, [column_idx]);
        let ef_sequences: Vec<EfSeq> = stream_builder
            .with_projection(projection_mask)
            .with_batch_size(1024 * 1024) // large batches to avoid wasting time merging sorted lists
            .build()
            .context("Could not build ParquetRecordBatchStream")?
            .map(|batch| {
                let batch = batch.context("Could not read batch")?;
                let column_chunk = batch
                    .column(0)
                    .as_primitive_opt::<UInt64Type>()
                    .with_context(|| {
                        format!(
                            "Column {} has UInt64Array statistics, but is not a UInt64Array",
                            column_name
                        )
                    })?;
                ensure!(
                    column_chunk.null_count() == 0,
                    "{column_name} has null values"
                );

                let mut values = column_chunk
                    .into_iter()
                    .map(Option::unwrap) // can't panic because we just checked there are no nulls
                    .dedup() // early dedup to save time in the sort below
                    .map(|value| {
                        usize::try_from(value)
                            .with_context(|| format!("Value in {} overflowed usize", column_name))
                    })
                    .collect::<Result<Vec<_>>>()?;

                values.radix_sort_unstable();
                values = values.into_iter().dedup().collect();

                let &max_value = values.last().expect("Batch is empty");
                let mut efb = EliasFanoBuilder::new(values.len(), max_value);
                efb.extend(values);
                Ok(efb.build_with_seq())
            })
            .try_collect::<Vec<_>>()
            .await?;

        let values = ef_sequences
            .iter()
            .map(EfSeq::iter)
            .kmerge()
            .dedup()
            .collect::<Vec<_>>();
        let max_value = values.last().copied().unwrap_or_else(|| {
            tracing::warn!("Empty table");
            0
        });
        let mut efb = EliasFanoBuilder::new(values.len(), max_value);
        efb.extend(values.into_iter());
        let res = self.ef_index.set(EfIndex {
            column_name,
            values: efb.build_with_dict(),
        });
        if res.is_err() {
            tracing::warn!("ef_index was already set");
        }

        Ok(())
    }
}
