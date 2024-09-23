// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

/// Alternative implementation of [`FileFormat`] for Parquet without re-opening files on each query
use std::any::Any;
use std::collections::HashSet;
use std::ops::Range;
use std::sync::{Arc, Weak};

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::not_impl_err;
use datafusion::common::GetExt;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::datasource::physical_plan::parquet::{
    DefaultParquetFileReaderFactory, ParquetExecBuilder,
};
use datafusion::datasource::physical_plan::{
    FileMeta, FileScanConfig, FileSinkConfig, ParquetFileReaderFactory,
};
use datafusion::error::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::SessionState;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr, Statistics};
use futures_core::future::BoxFuture;
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::file::metadata::ParquetMetaData;

#[derive(Debug)]
pub struct CachingParquetFormatFactory(Arc<dyn FileFormatFactory>);

impl CachingParquetFormatFactory {
    pub fn new(parquet_format_factory: Arc<dyn FileFormatFactory>) -> Self {
        Self(parquet_format_factory)
    }
}

impl FileFormatFactory for CachingParquetFormatFactory {
    fn create(
        &self,
        state: &SessionState,
        format_options: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        self.0
            .create(state, format_options)
            .map(|format| Arc::new(CachingParquetFormat::new(format)) as _)
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(CachingParquetFormat::new(FileFormatFactory::default(
            self.0.as_ref(),
        )))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl GetExt for CachingParquetFormatFactory {
    fn get_ext(&self) -> String {
        self.0.get_ext()
    }
}

#[derive(Debug)]
struct CachingParquetFormat {
    inner: Arc<dyn FileFormat>,
    factories: DashMap<ObjectStoreUrl, Arc<CachingParquetFileReaderFactory>>,
}

impl CachingParquetFormat {
    fn new(inner: Arc<dyn FileFormat>) -> Self {
        assert!(
            inner.as_any().downcast_ref::<ParquetFormat>().is_some(),
            "ParquetFormatFactory::create a non-Parquet FileFormat"
        );
        Self {
            inner,
            factories: Default::default(),
        }
    }
}

#[async_trait]
impl FileFormat for CachingParquetFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }

    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> Result<String> {
        self.inner.get_ext_with_compression(file_compression_type)
    }

    async fn infer_schema(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        self.inner.infer_schema(state, store, objects).await
    }

    async fn infer_stats(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        self.inner
            .infer_stats(state, store, table_schema, object)
            .await
    }

    async fn create_physical_plan(
        &self,
        state: &SessionState,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Copied from ParquetFormat::create_physical_plan, with the addition of
        // .with_parquet_file_reader_factory(...)

        // .unwrap() can't fail, was checked by constructor
        let inner = self.inner.as_any().downcast_ref::<ParquetFormat>().unwrap();

        let object_store_url = conf.object_store_url.clone();
        let mut builder = ParquetExecBuilder::new_with_options(conf, inner.options().clone())
            .with_parquet_file_reader_factory(Arc::clone(
                self.factories
                    .entry(object_store_url.clone())
                    .or_try_insert_with(|| {
                        state
                            .runtime_env()
                            .object_store(object_store_url)
                            .map(CachingParquetFileReaderFactory::new)
                            .map(Arc::new)
                    })?
                    .value(),
            ) as _);

        // If enable pruning then combine the filters to build the predicate.
        // If disable pruning then set the predicate to None, thus readers
        // will not prune data based on the statistics.
        if inner.enable_pruning() {
            if let Some(predicate) = filters.cloned() {
                builder = builder.with_predicate(predicate);
            }
        }
        if let Some(metadata_size_hint) = inner.metadata_size_hint() {
            builder = builder.with_metadata_size_hint(metadata_size_hint);
        }

        Ok(builder.build_arc())
    }

    async fn create_writer_physical_plan(
        &self,
        _input: Arc<dyn ExecutionPlan>,
        _state: &SessionState,
        _conf: FileSinkConfig,
        _order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("cached_parquet format does not support writing")
    }
}

/*
#[derive(Debug, Default)]
struct CachingParquetFileReaderFactoryFactory {
    /// Cache, keyed by file name and partition index
    readers: Arc<DashMap<(PathBuf, usize), Mutex<Box<dyn AsyncFileReader + Send>>>>,
}

impl CachingParquetFileReaderFactoryFactory {
    fn for_runtime_env(runtime_env: Arc<RuntimeEnv>) -> CachingParquetFileReaderFactory {
        CachingParquetFileReaderFactory {
            runtime_env,
            readers: Arc::clone(self.readers),
        }
    }
}
*/

struct CachingParquetFileReaderFactory {
    inner: DefaultParquetFileReaderFactory,
    /// Cache, keyed by file name and partition index. Values are pools of readers, as each reader
    /// can not be used by two threads at the same time.
    readers: Arc<
        DashMap<
            (object_store::path::Path, usize),
            Arc<crossbeam_queue::SegQueue<Box<dyn AsyncFileReader + Send>>>,
        >,
    >,
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
    fn new(object_store: Arc<dyn ObjectStore>) -> Self {
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
        let reader = match reader_pool.pop() {
            None => {
                tracing::debug!("Creating new reader for {}", file_meta.location());
                self.inner
                    .create_reader(partition_index, file_meta, metadata_size_hint, metrics)?
            }
            Some(reader) => {
                tracing::debug!("Reusing reader for {}", file_meta.location());
                reader
            }
        };
        Ok(Box::new(PooledAsyncFileReader::new(
            reader,
            Arc::downgrade(reader_pool),
        )) as _)
    }
}

/// Wrapper for [`AsyncFileReader`] that puts its wrapped reader back into a pool when dropped.
struct PooledAsyncFileReader {
    inner: Option<Box<dyn AsyncFileReader + Send>>,
    pool: Weak<crossbeam_queue::SegQueue<Box<dyn AsyncFileReader + Send>>>,
}

impl PooledAsyncFileReader {
    fn new(
        inner: Box<dyn AsyncFileReader + Send>,
        pool: Weak<crossbeam_queue::SegQueue<Box<dyn AsyncFileReader + Send>>>,
    ) -> Self {
        Self {
            inner: Some(inner),
            pool,
        }
    }
}

impl AsyncFileReader for PooledAsyncFileReader {
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

impl Drop for PooledAsyncFileReader {
    fn drop(&mut self) {
        // If the pool still exists, put the wrapped reader back into the pool
        if let Some(pool) = self.pool.upgrade() {
            tracing::debug!("Releasing reader");
            pool.push(self.inner.take().unwrap());
        }
    }
}

/*
pub struct CachedParquetTableProvider {
}

impl CachedParquetTableProvider {
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let mut options = TableParquetOptions::new();
        options.global = ParquetOptions {
            enable_page_index: true,
            pruning: true, // row group pruning based on min/max + bloom filter
            pushdown_filters: true,
            reorder_filters: true, // allow Datafusion to run its optimizations
            ..options.global
        };

        let format = ParquetFormat::new()
            .with_enable_pruning(true)  // row group pruning based on min/max + bloom filter
            .with_options(options);
        let factory = ParquetFormatFactory::new_with_options(options);

        // Normally, Datafusion would persist the factory, and call it every time it needs to run a
        // query on the table. However, this causes a very high latency on each query, as it needs
        // to re-open the files and parse them, in particular the Page Index.
        // So here, we open the files immediately, once and for all.

    }
}

#[async_trait]
impl TableProvider for CachedParquetTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let indexed_file = &self.indexed_file;
        let predicate = self.filters_to_predicate(state, filters)?;

        // Figure out which row groups to scan based on the predicate
        let access_plan = self.create_plan(&predicate)?;
        println!("{access_plan:?}");

        let partitioned_file = indexed_file
            .partitioned_file()
            // provide the starting access plan to the ParquetExec by
            // storing it as  "extensions" on PartitionedFile
            .with_extensions(Arc::new(access_plan) as _);

        // Prepare for scanning
        let schema = self.schema();
        let object_store_url = ObjectStoreUrl::parse("file://")?;
        let file_scan_config = FileScanConfig::new(object_store_url, schema)
            .with_limit(limit)
            .with_projection(projection.cloned())
            .with_file(partitioned_file);

        // Configure a factory interface to avoid re-reading the metadata for each file
        let reader_factory =
            CachedParquetFileReaderFactory::new(Arc::clone(&self.object_store))
                .with_file(indexed_file);

        // Finally, put it all together into a ParquetExec
        Ok(ParquetExecBuilder::new(file_scan_config)
            // provide the predicate so the ParquetExec can try and prune
            // row groups internally
            .with_predicate(predicate)
            // provide the factory to create parquet reader without re-reading metadata
            .with_parquet_file_reader_factory(Arc::new(reader_factory))
            .build_arc())
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}
*/