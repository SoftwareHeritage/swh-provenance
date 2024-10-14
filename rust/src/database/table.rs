// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use anyhow::{anyhow, ensure, Context, Result};
use arrow::array::*;
use arrow::datatypes::*;
use futures::stream::FuturesUnordered;
use futures::FutureExt;
use futures::{Stream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::schema::types::SchemaDescriptor;
use tokio::task::JoinSet;

use super::metrics::TableScanInitMetrics;
use super::reader::FileReader;
use super::types::IndexKey;

pub struct Table {
    pub files: Box<[Arc<FileReader>]>,
    schema: Arc<Schema>,
    path: Path,
}

impl Table {
    pub async fn new(store: Arc<dyn ObjectStore>, path: Path) -> Result<Self> {
        let objects_meta: Vec<_> = store
            .list(Some(&path))
            .map(|object_meta_res| object_meta_res.map(Arc::new))
            .try_collect()
            .await
            .with_context(|| format!("Could not list {} in {}", path, store))?;
        let files: Vec<_> = objects_meta
            .iter()
            .map(|object_meta| {
                FileReader::new(Arc::clone(&store), Arc::clone(object_meta)).map(Arc::new)
            })
            .collect::<JoinSet<_>>()
            .join_all()
            .await;
        let file_metadata: Vec<_> = files
            .iter()
            .map(|file| {
                let file = Arc::clone(file);
                async move {
                    file.reader()
                        .await
                        .context("Could not get reader")?
                        .get_metadata()
                        .await
                        .context("Could not get file metadata")
                }
            })
            .collect::<JoinSet<_>>()
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let mut file_metadata: Vec<_> = file_metadata
            .iter()
            .map(|file_metadata| file_metadata.file_metadata())
            .collect();
        let last_file_metadata = file_metadata
            .pop()
            .ok_or_else(|| anyhow!("No files in {}", path))?;
        for (object_meta, other_file_metadata) in
            std::iter::zip(objects_meta.iter(), file_metadata.into_iter())
        {
            ensure!(
                last_file_metadata.schema_descr() == other_file_metadata.schema_descr(),
                "Schema of {} and {} differ: {:?} != {:?}",
                objects_meta.last().unwrap().location,
                object_meta.location,
                last_file_metadata,
                other_file_metadata
            );
        }
        Ok(Self {
            files: files.into(),
            schema: Arc::new(
                parquet::arrow::parquet_to_arrow_schema(
                    last_file_metadata.schema_descr(),
                    // Note: other files may have different key-value metadata, but we can't
                    // easily check for equality because it includes the creation date
                    last_file_metadata.key_value_metadata(),
                )
                .context("Could not read schema")?,
            ),
            path,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns a reader for each file in the table
    pub async fn readers(&self) -> impl Stream<Item = Result<impl AsyncFileReader>> + '_ {
        self.files
            .iter()
            .map(|file_reader| file_reader.reader())
            .collect::<FuturesUnordered<_>>()
    }

    /// Returns all rows in which the given column contains any of the given keys
    #[allow(clippy::single_range_in_vec_init)] // false positive
    pub async fn filtered_record_batch_stream_builder<'a, K: IndexKey>(
        &'a self,
        column: &'static str,
        keys: Arc<Vec<K>>,
        builder_configurator: Arc<impl ReaderBuilderConfigurator>,
    ) -> Result<impl Stream<Item = Result<RecordBatch>> + 'static> {
        let column_idx: usize = self
            .schema
            .index_of(column)
            .with_context(|| format!("Unknown column {}", column))?;

        let (metrics, reader_streams): (Vec<_>, Vec<ParquetRecordBatchStream<_>>) = self
            .readers()
            .await
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(move |reader| {
                let keys = Arc::clone(&keys);
                let builder_configurator = Arc::clone(&builder_configurator);
                async move {
                    let mut metrics = TableScanInitMetrics::default();
                    let total_timer_guard = metrics.total_time.timer();

                    let reader = reader.context("Could not get AsyncFileReader")?;
                    let mut stream_builder = {
                        let _timer_guard = metrics.open_builder_time.timer();
                        ParquetRecordBatchStreamBuilder::new_with_options(
                            reader,
                            ArrowReaderOptions::new().with_page_index(true),
                        )
                        .await
                        .context("Could not open stream")?
                    };

                    if keys.is_empty() {
                        // shortcut, return nothing
                        drop(total_timer_guard);
                        return Ok((metrics, stream_builder.with_row_groups(vec![]).build().context("Could not build empty record stream")?));
                    }

                    let parquet_metadata = {
                        let _timer_guard = metrics.read_metadata_time.timer();
                        Arc::clone(stream_builder.metadata())
                    };
                    let column_index = parquet_metadata.column_index();
                    let offset_index = parquet_metadata.offset_index();

                    let schema = Arc::clone(stream_builder.schema());
                    let parquet_schema = SchemaDescriptor::new(stream_builder.parquet_schema().root_schema_ptr()); // clone
                    let statistics_converter = StatisticsConverter::try_new(
                        column,
                        &schema,
                        &parquet_schema,
                    )
                    .context("Could not build statistics converter")?;
                    let row_groups_match_statistics = {
                        let _timer_guard = metrics.eval_row_groups_statistics_time.timer();
                        IndexKey::check_column_chunk(
                            &keys,
                            &statistics_converter,
                            parquet_metadata.row_groups(),
                        )
                        .context("Could not check row group statistics")?
                    };

                    let selected_row_groups = if let Some(row_groups_match_statistics) = row_groups_match_statistics {
                        let mut selected_row_groups = Vec::new();
                        for (row_group_idx, row_group_matches_statistics) in
                                row_groups_match_statistics.into_iter()
                                .enumerate()
                        {
                            // Prune row group using statistics
                            if row_group_matches_statistics {
                                // there may be a key in this row group
                                metrics.row_groups_selected_by_statistics += 1
                            } else {
                                // we know for sure there is no key in this row group
                                metrics.row_groups_pruned_by_statistics += 1;
                                continue; // shortcut
                            }

                            // TODO: filter out keys that didn't match the statistics, to reduce the
                            // runtime and number of false positives in checking the bloom filter

                            // Prune row groups using Bloom Filters
                            {
                                let timer_guard = metrics.eval_bloom_filter_time.timer();
                                if let Some(bloom_filter) = stream_builder
                                    .get_row_group_column_bloom_filter(row_group_idx, column_idx)
                                    .await
                                    .context("Could not get Bloom Filter")?
                                {
                                    drop(timer_guard);
                                    let _timer_guard = metrics.eval_bloom_filter_time.timer();
                                    let mut keys_in_group =
                                        keys.iter().filter(|&key| bloom_filter.check(key));
                                    if keys_in_group.next().is_none() {
                                        // None of the keys matched the Bloom Filter
                                        metrics.row_groups_pruned_by_bloom_filters += 1;
                                        continue; // shortcut
                                    }
                                    // At least one key matched the Bloom Filter
                                    metrics.row_groups_selected_by_bloom_filters += 1;
                                }
                            }

                            selected_row_groups.push(row_group_idx);
                        }
                        selected_row_groups
                    } else {
                        // We don't know how to filter on row group statistics, so we
                        // unconditionally select every row group
                        (0..parquet_metadata.row_groups().len()).collect()
                    };

                    // TODO: remove keys that did not match any of the bloom filters

                    // Prune pages using page index
                    let row_selection = if let Some(column_index) = column_index {
                        let _timer_guard = metrics.eval_page_index_time.timer();
                        let offset_index =
                            offset_index.expect("column_index is present but offset_index is not");

                        let num_rows_in_selected_row_groups: i64 = selected_row_groups
                            .iter()
                            .map(|&row_group_idx| {
                                parquet_metadata.row_group(row_group_idx).num_rows()
                            })
                            .sum();
                        let num_rows_in_selected_row_groups = usize::try_from(num_rows_in_selected_row_groups).context("Number of rows in selected row groups overflows usize")?;

                        // TODO: if no page in a row group was selected, deselect the row group as
                        // well. This makes sense to do in the case where a row group has two
                        // pages, one with values from 0 to 10 and the other with values from 20 to
                        // 30 and we are looking for 15; because the row group statistics are too
                        // coarse-grained and missed the discontinuity in ranges.
                        let selected_pages = IndexKey::check_page_index(
                            &keys,
                            &statistics_converter,
                            column_index,
                            offset_index,
                            &selected_row_groups,
                        )?;
                        match selected_pages {
                            None => {
                                // IndexKey does not implement check_page_index, so we need to read
                                // every page from the selected row groups
                                RowSelection::from_consecutive_ranges([
                                    0..num_rows_in_selected_row_groups
                                ].into_iter(), num_rows_in_selected_row_groups)
                            }
                            Some(selected_pages) => {
                                // TODO: exit early if no page is selected

                                let mut selected_pages_iter = selected_pages.iter();
                                let mut selected_ranges = Vec::new();

                                // Index of the first row in the current group within the current row group selection.
                                // This is 0 for every row group that does not have a selected row group before itself.
                                // See https://docs.rs/parquet/53.1.0/parquet/arrow/async_reader/type.ParquetRecordBatchStreamBuilder.html#tymethod.with_row_selection
                                let mut current_row_group_first_row_idx = 0usize;

                                // For each row group, get selected pages locations inside that row group,
                                // and translate these locations into ranges that we can feed to
                                // RowSelection
                                for &row_group_idx in &selected_row_groups {
                                    let row_group_meta = parquet_metadata.row_group(row_group_idx);
                                    let num_rows_in_row_group = usize::try_from(
                                        row_group_meta.num_rows(),
                                    )
                                    .context("number of rows in row group overflowed usize")?;
                                    let next_row_group_first_row_idx =
                                        current_row_group_first_row_idx
                                            .checked_add(num_rows_in_row_group)
                                            .context("Number of rows in file overflowed usize")?;

                                    let mut page_locations_iter = offset_index[row_group_idx]
                                        [column_idx]
                                        .page_locations().iter().peekable();
                                    while let Some(page_location) = page_locations_iter.next() {
                                        if selected_pages_iter.next().expect("check_page_index returned an array smaller than the number of pages") {
                                            assert!(page_location.first_row_index < row_group_meta.num_rows(), "page_location.first_row_index is greater or equal to the number of rows in its row group");
                                            let page_first_row_index = usize::try_from(page_location.first_row_index).context("page_location.first_row_index overflowed usize")?;
                                            if let Some(next_page_location) = page_locations_iter.peek() {
                                                let next_page_first_row_index = usize::try_from(next_page_location.first_row_index).context("next_page_location.first_row_index overflowed usize")?;
                                                selected_ranges.push((current_row_group_first_row_idx+page_first_row_index)..(current_row_group_first_row_idx+next_page_first_row_index))
                                            } else {
                                                // last page of the row group
                                                selected_ranges.push((current_row_group_first_row_idx+page_first_row_index)..next_row_group_first_row_idx);
                                            }
                                        }
                                    }
                                    current_row_group_first_row_idx = next_row_group_first_row_idx;
                                }

                                // Build RowSelection from the ranges corresponding to each selected page
                                RowSelection::from_consecutive_ranges(
                                    selected_ranges.into_iter(),
                                    num_rows_in_selected_row_groups,
                                )
                            }
                        }
                    } else {
                        let num_rows_in_selected_row_groups: i64 = selected_row_groups
                            .iter()
                            .map(|&row_group_idx| {
                                parquet_metadata.row_group(row_group_idx).num_rows()
                            })
                            .sum();
                        let num_rows_in_selected_row_groups: usize =
                            num_rows_in_selected_row_groups
                                .try_into()
                                .context("num_rows_in_selected_row_groups overflowed usize")?;
                        RowSelection::from_consecutive_ranges(
                            [0..num_rows_in_selected_row_groups].into_iter(),
                            num_rows_in_selected_row_groups,
                        )
                    };
                    metrics.rows_pruned_by_page_index = row_selection.skipped_row_count();
                    metrics.rows_selected_by_page_index = row_selection.row_count();
                    let stream_builder = stream_builder
                        .with_row_groups(selected_row_groups)
                        .with_row_selection(row_selection);
                    drop(total_timer_guard);
                    Ok((metrics, builder_configurator.configure(stream_builder).context("Could not finish configuring ParquetRecordBatchStreamBuilder")?.build().context("Could not build ParquetRecordBatchStream")?))

                }
            })
            .collect::<JoinSet<_>>()
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip();

        let metrics: TableScanInitMetrics = metrics.into_iter().sum();

        tracing::debug!("Scan init metrics: {:#?}", metrics);

        Ok(futures::stream::iter(reader_streams.into_iter())
            .map(|stream| -> Result<_> {
                Ok(stream.map(|batch_result| batch_result.context("Could not read batch")))
            })
            .try_flatten_unordered(num_cpus::get()))
    }
}

pub trait ReaderBuilderConfigurator: Send + Sync + 'static {
    fn configure<R: AsyncFileReader>(
        &self,
        reader_builder: ParquetRecordBatchStreamBuilder<R>,
    ) -> Result<ParquetRecordBatchStreamBuilder<R>>;
}

impl ReaderBuilderConfigurator for () {
    fn configure<R: AsyncFileReader>(
        &self,
        reader_builder: ParquetRecordBatchStreamBuilder<R>,
    ) -> Result<ParquetRecordBatchStreamBuilder<R>> {
        Ok(reader_builder)
    }
}
