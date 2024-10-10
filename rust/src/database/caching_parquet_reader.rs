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

/// Wrapper for [`AsyncFileReader`] that only reads its metadata the first time it is requested
pub struct CachingParquetFileReader<R: AsyncFileReader> {
    inner: R,
    metadata: Option<Arc<ParquetMetaData>>,
}

impl<R: AsyncFileReader> CachingParquetFileReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            metadata: None,
        }
    }
}

impl<R: AsyncFileReader> AsyncFileReader for CachingParquetFileReader<R> {
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

impl<R: AsyncFileReader> CachingParquetFileReader<R> {
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

struct CachingParquetFileReaderMetadataFetch<'a, R: AsyncFileReader>(
    &'a mut CachingParquetFileReader<R>,
);

impl<'a, R: AsyncFileReader> MetadataFetch for CachingParquetFileReaderMetadataFetch<'a, R> {
    fn fetch(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        self.0.fetch(range)
    }
}
