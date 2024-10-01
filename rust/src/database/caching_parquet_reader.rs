// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use dashmap::DashMap;
use datafusion::datasource::physical_plan::parquet::DefaultParquetFileReaderFactory;
use datafusion::datasource::physical_plan::{FileMeta, ParquetFileReaderFactory};
use datafusion::error::Result;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::future::{BoxFuture, Either};
use futures::FutureExt;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::MetadataFetch;
use parquet::arrow::async_reader::MetadataLoader;
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
    ) -> Result<Box<dyn AsyncFileReader + Send>> {
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

/// Wrapper for [`AsyncFileReader`] that  only reads its metadata the first time it is requested
struct CachingParquetFileReader {
    inner: Box<dyn AsyncFileReader + Send>,
    metadata: Option<Arc<ParquetMetaData>>,
}

impl CachingParquetFileReader {
    fn new(inner: Box<dyn AsyncFileReader + Send>) -> Self {
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
        Box::pin(self.get_metadata_async())
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<bytes::Bytes>>> {
        self.inner.get_byte_ranges(ranges)
    }
}

impl CachingParquetFileReader {
    /// Implementation of [`AsyncFileReader::get_metadata`] using new-style async,
    /// so it can pass the borrow checker
    async fn get_metadata_async(&mut self) -> parquet::errors::Result<Arc<ParquetMetaData>> {
        match &self.metadata {
            Some(metadata) => Ok(Arc::clone(metadata)),
            None => match self.inner.get_metadata().await {
                Ok(metadata) => {
                    // This function is called by `ArrowReaderMetadata::load_async`.
                    // Then, `load_async` may enrich the `ParquetMetaData` we return with
                    // the page index, using `MetadataLoader`; and this enriched
                    // `ParquetMetaData` reader would not be cached.
                    //
                    // Datafusion does not (currently) support caching the enriched
                    // `ParquetMetaData`, so we unconditionally enrich it here with
                    // the page index, so we can cache it.
                    //
                    // See:
                    // * discussion on https://github.com/apache/datafusion/pull/12593
                    // * https://github.com/apache/arrow-rs/blob/62825b27e98e6719cb66258535c75c7490ddba44/parquet/src/arrow/async_reader/mod.rs#L212-L228
                    let metadata = Arc::try_unwrap(metadata).unwrap_or_else(|e| e.as_ref().clone());
                    let mut loader =
                        MetadataLoader::new(CachingParquetFileReaderMetadataFetch(self), metadata);
                    loader.load_page_index(true, true).await?;
                    let metadata = Arc::new(loader.finish());
                    self.metadata = Some(Arc::clone(&metadata));
                    Ok(metadata)
                }
                Err(e) => Err(e),
            },
        }
    }
}

struct CachingParquetFileReaderMetadataFetch<'a>(&'a mut CachingParquetFileReader);

impl<'a> MetadataFetch for CachingParquetFileReaderMetadataFetch<'a> {
    fn fetch(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        self.0.fetch(range)
    }
}
