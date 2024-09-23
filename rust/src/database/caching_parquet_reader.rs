// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use dashmap::DashMap;
use datafusion::datasource::physical_plan::parquet::DefaultParquetFileReaderFactory;
use datafusion::datasource::physical_plan::parquet::ParquetFileReader;
use datafusion::datasource::physical_plan::{FileMeta, ParquetFileReaderFactory};
use datafusion::error::Result;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::future::{BoxFuture, Either};
use futures::FutureExt;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::ParquetMetaData;

use super::ParquetFileReaderPool;

pub struct CachingParquetFileReaderFactory {
    inner: DefaultParquetFileReaderFactory,
    /// Cache, keyed by file name and partition index. Values are pools of readers, as each reader
    /// can not be used by two threads at the same time.
    readers: Arc<DashMap<(object_store::path::Path, usize), ParquetFileReaderPool>>,
}

impl std::fmt::Debug for CachingParquetFileReaderFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachingParquetFileReaderFactory")
            .field("inner", &self.inner)
            .field(
                "readers",
                &self
                    .readers
                    .iter()
                    .map(|entry| {
                        let (file_name, partition_index) = entry.key();
                        (file_name.clone(), *partition_index)
                    })
                    .collect::<HashSet<_>>(),
            )
            .finish()
    }
}

impl CachingParquetFileReaderFactory {
    pub fn new(object_store: Arc<dyn ObjectStore>) -> Self {
        Self {
            inner: DefaultParquetFileReaderFactory::new(object_store),
            readers: Default::default(),
        }
    }
}

impl ParquetFileReaderFactory for CachingParquetFileReaderFactory {
    fn create_reader(
        &self,
        partition_index: usize,
        file_meta: FileMeta,
        metadata_size_hint: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Result<Box<dyn ParquetFileReader>> {
        let filename = file_meta.location().to_owned();
        let reader_pool_guard = self.readers.entry((filename, partition_index)).or_default();
        let reader_pool = reader_pool_guard.value();
        reader_pool.try_get_reader(|| {
            Ok(
                Box::new(CachingParquetFileReader::new(self.inner.create_reader(
                    partition_index,
                    file_meta,
                    metadata_size_hint,
                    metrics,
                )?)) as _,
            )
        })
    }
}

/// Wrapper for [`ParquetFileReader`] that  only reads its metadata the first time it is requested
struct CachingParquetFileReader {
    inner: Box<dyn ParquetFileReader>,
    metadata: Option<ArrowReaderMetadata>,
}

impl CachingParquetFileReader {
    fn new(inner: Box<dyn ParquetFileReader>) -> Self {
        Self {
            inner,
            metadata: None,
        }
    }
}

impl AsyncFileReader for CachingParquetFileReader {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        self.inner.get_bytes(range)
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        self.inner.get_metadata()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<bytes::Bytes>>> {
        self.inner.get_byte_ranges(ranges)
    }
}

impl ParquetFileReader for CachingParquetFileReader {
    fn upcast(self: Box<Self>) -> Box<dyn AsyncFileReader + 'static> {
        Box::new(*self)
    }

    fn load_metadata(
        &mut self,
        options: ArrowReaderOptions,
    ) -> BoxFuture<'_, parquet::errors::Result<ArrowReaderMetadata>> {
        Box::pin(match &self.metadata {
            Some(metadata) => Either::Left(std::future::ready(Ok(metadata.clone()))),
            None => Either::Right(self.inner.load_metadata(options).inspect(|metadata| {
                if let Ok(metadata) = metadata {
                    self.metadata = Some(metadata.clone())
                }
            })),
        })
    }
}
