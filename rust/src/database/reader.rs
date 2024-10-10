// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use anyhow::Result;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetObjectReader;

use super::caching_parquet_reader::CachingParquetFileReader;
use super::pooled_reader::ParquetFileReaderPool;

pub struct FileReader {
    store: Arc<dyn ObjectStore>,
    object_meta: Arc<ObjectMeta>,
    pool: ParquetFileReaderPool<CachingParquetFileReader<ParquetObjectReader>>,
}

impl FileReader {
    pub async fn new(store: Arc<dyn ObjectStore>, object_meta: Arc<ObjectMeta>) -> Self {
        Self {
            store,
            object_meta,
            pool: ParquetFileReaderPool::default(),
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
}
