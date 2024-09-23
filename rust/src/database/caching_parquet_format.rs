// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::any::Any;
use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::not_impl_err;
use datafusion::common::GetExt;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::datasource::physical_plan::parquet::ParquetExecBuilder;
use datafusion::datasource::physical_plan::ParquetFileReaderFactory;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSinkConfig};
use datafusion::error::Result;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::SessionState;
use datafusion::physical_expr::LexRequirement;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr, Statistics};
use object_store::{ObjectMeta, ObjectStore};

/// Implementation of [`FileFormatFormatFactory`] that yields [`CachingParquetFormat`]
/// instead of [`DefaultParquetFormat`]
pub struct CachingParquetFormatFactory<
    T: ParquetFileReaderFactory,
    F: Fn(Arc<dyn ObjectStore>) -> T + Sync + Send,
> {
    inner: Arc<dyn FileFormatFactory>,
    reader_factory_init: F,
    marker: PhantomData<T>,
}

impl<
        T: ParquetFileReaderFactory,
        F: Fn(Arc<dyn ObjectStore>) -> T + Clone + Sync + Send + 'static,
    > CachingParquetFormatFactory<T, F>
{
    /// `inner` is the wrapped `FileFormatFactory`, whose [`FileFormat`] instances will be wrapped
    /// by [`CachingParquetFormatFactory`]
    pub fn new(inner: Arc<dyn FileFormatFactory>, reader_factory_init: F) -> Self {
        Self {
            inner,
            reader_factory_init,
            marker: PhantomData,
        }
    }
}

impl<
        T: ParquetFileReaderFactory,
        F: Fn(Arc<dyn ObjectStore>) -> T + Clone + Sync + Send + 'static,
    > std::fmt::Debug for CachingParquetFormatFactory<T, F>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachingParquetFormatFactory")
            .field("inner", &self.inner)
            .field("reader_factory_init", &"...")
            .finish()
    }
}

impl<
        T: ParquetFileReaderFactory,
        F: Fn(Arc<dyn ObjectStore>) -> T + Clone + Sync + Send + 'static,
    > FileFormatFactory for CachingParquetFormatFactory<T, F>
{
    fn create(
        &self,
        state: &SessionState,
        format_options: &std::collections::HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        self.inner.create(state, format_options).map(|format| {
            Arc::new(CachingParquetFormat::new(
                format,
                self.reader_factory_init.clone(),
            )) as _
        })
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(CachingParquetFormat::new(
            FileFormatFactory::default(self.inner.as_ref()),
            self.reader_factory_init.clone(),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<T: ParquetFileReaderFactory, F: Fn(Arc<dyn ObjectStore>) -> T + Sync + Send + 'static> GetExt
    for CachingParquetFormatFactory<T, F>
{
    fn get_ext(&self) -> String {
        self.inner.get_ext()
    }
}

/// Re-implementation of [`ParquetFormat`] that instantiates [`ParquetFileReaderFactory`]
/// only once per object store, so they can store state.
pub struct CachingParquetFormat<
    T: ParquetFileReaderFactory,
    F: Fn(Arc<dyn ObjectStore>) -> T + Sync + Send,
> {
    inner: Arc<dyn FileFormat>,
    factories: DashMap<ObjectStoreUrl, Arc<T>>,
    factory_init: F,
}

impl<
        T: ParquetFileReaderFactory,
        F: Fn(Arc<dyn ObjectStore>) -> T + Clone + Sync + Send + 'static,
    > CachingParquetFormat<T, F>
{
    fn new(inner: Arc<dyn FileFormat>, factory_init: F) -> Self {
        assert!(
            inner.as_any().downcast_ref::<ParquetFormat>().is_some(),
            "ParquetFormatFactory::create a non-Parquet FileFormat"
        );
        Self {
            inner,
            factories: Default::default(),
            factory_init,
        }
    }
}

impl<
        T: ParquetFileReaderFactory,
        F: Fn(Arc<dyn ObjectStore>) -> T + Clone + Sync + Send + 'static,
    > std::fmt::Debug for CachingParquetFormat<T, F>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachingParquetFormat")
            .field("inner", &self.inner)
            .field("factories", &self.factories)
            .field("factory_init", &"...")
            .finish()
    }
}

#[async_trait]
impl<
        T: ParquetFileReaderFactory,
        F: Fn(Arc<dyn ObjectStore>) -> T + Clone + Sync + Send + 'static,
    > FileFormat for CachingParquetFormat<T, F>
{
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
                            .map(self.factory_init.clone())
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
