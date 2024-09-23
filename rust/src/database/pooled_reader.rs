// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::ops::Range;
use std::sync::{Arc, Weak};

use datafusion::datasource::physical_plan::parquet::ParquetFileReader;
use datafusion::error::Result;
use futures::future::BoxFuture;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::ParquetMetaData;

/// A collection of [`ParquetFileReader`] that gets the reader back before they are dropped
///
/// This allows reusing readers across requests, so they can cache the metadata.
#[derive(Default)]
pub struct ParquetFileReaderPool(Arc<crossbeam_queue::SegQueue<Box<dyn ParquetFileReader>>>);

impl ParquetFileReaderPool {
    pub fn try_get_reader<E>(
        &self,
        reader_init: impl FnOnce() -> Result<Box<dyn ParquetFileReader>, E>,
    ) -> Result<Box<dyn ParquetFileReader>, E> {
        let inner_reader = match self.0.pop() {
            None => reader_init()?,
            Some(reader) => reader,
        };
        Ok(Box::new(PooledParquetFileReader::new(
            inner_reader,
            Arc::downgrade(&self.0), // downgrade so orphan readers don't keep the pool in memory
        )) as _)
    }
}

/// Wrapper for [`ParquetFileReader`] that puts its wrapped reader back into a pool when dropped.
struct PooledParquetFileReader {
    inner: Option<Box<dyn ParquetFileReader>>,
    pool: Weak<crossbeam_queue::SegQueue<Box<dyn ParquetFileReader>>>,
}

impl PooledParquetFileReader {
    fn new(
        inner: Box<dyn ParquetFileReader>,
        pool: Weak<crossbeam_queue::SegQueue<Box<dyn ParquetFileReader>>>,
    ) -> Self {
        Self {
            inner: Some(inner),
            pool,
        }
    }
}

impl AsyncFileReader for PooledParquetFileReader {
    fn get_bytes(
        &mut self,
        range: Range<usize>,
    ) -> BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        self.inner.as_mut().unwrap().get_bytes(range)
    }
    fn get_metadata(&mut self) -> BoxFuture<'_, parquet::errors::Result<Arc<ParquetMetaData>>> {
        self.inner.as_mut().unwrap().get_metadata()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<usize>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<bytes::Bytes>>> {
        self.inner.as_mut().unwrap().get_byte_ranges(ranges)
    }
}

impl ParquetFileReader for PooledParquetFileReader {
    fn upcast(self: Box<Self>) -> Box<dyn AsyncFileReader + 'static> {
        Box::new(*self)
    }

    fn load_metadata(
        &mut self,
        options: ArrowReaderOptions,
    ) -> BoxFuture<'_, parquet::errors::Result<ArrowReaderMetadata>> {
        self.inner.as_mut().unwrap().load_metadata(options)
    }
}

impl Drop for PooledParquetFileReader {
    fn drop(&mut self) {
        // If the pool still exists, put the wrapped reader back into the pool
        if let Some(pool) = self.pool.upgrade() {
            pool.push(self.inner.take().unwrap());
        }
    }
}
