// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::ops::Range;
use std::sync::Arc;

use anyhow::{anyhow, ensure, Context, Result};
use arrow::array::*;
use arrow::datatypes::*;
use futures::stream::FuturesUnordered;
use futures::{Stream, StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::bloom_filter::Sbbf;

use super::reader::FileReader;
use super::IndexKey;

pub struct Table {
    pub files: Box<[FileReader]>,
    schema: Arc<Schema>,
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
            .map(|object_meta| FileReader::new(Arc::clone(&store), Arc::clone(object_meta)))
            .collect::<FuturesUnordered<_>>()
            .collect()
            .await;
        let file_metadata: Vec<_> = files
            .iter()
            .map(|file| async {
                file.reader()
                    .await
                    .context("Could not get reader")?
                    .get_metadata()
                    .await
                    .context("Could not get file metadata")
            })
            .collect::<FuturesUnordered<_>>()
            .try_collect()
            .await?;
        let mut file_metadata: Vec<_> = file_metadata
            .iter()
            .map(|file_metadata| file_metadata.file_metadata())
            .collect();
        let last_file_metadata = file_metadata
            .pop()
            .ok_or_else(|| anyhow!("No files in {}", path))?;
        for (object_meta, other_file_metadata) in std::iter::zip(
            objects_meta.iter(),
            file_metadata.into_iter().map(|file_metadata| file_metadata),
        ) {
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
        })
    }

    /// Returns a reader for each file in the table
    pub async fn readers(&self) -> impl Stream<Item = Result<impl AsyncFileReader>> + '_ {
        self.files
            .iter()
            .map(|file_reader| file_reader.reader())
            .collect::<FuturesUnordered<_>>()
    }

    /// Returns all rows in which the given column contains any of the given keys
    pub async fn filtered_record_batch_stream_builder<'a, K: IndexKey>(
        &'a self,
        column: &str,
        keys: &'a [K],
    ) -> Result<
        impl Stream<Item = Result<ParquetRecordBatchStreamBuilder<impl AsyncFileReader>>> + 'a,
    > {
        let column_idx: usize = self
            .schema
            .index_of(column)
            .with_context(|| format!("Unknown column {}", column))?;
        Ok(self
            .readers()
            .await
            .map(move |reader| async move {
                let reader = reader?;
                let mut stream_builder = ParquetRecordBatchStreamBuilder::new_with_options(
                    reader,
                    ArrowReaderOptions::new().with_page_index(true),
                )
                .await
                .context("Could not open stream")?;
                if keys.is_empty() {
                    return Ok(stream_builder.with_row_groups(vec![])); // shortcut
                }
                let parquet_metadata = Arc::clone(stream_builder.metadata());
                let column_index = parquet_metadata.column_index();
                let offset_index = parquet_metadata.offset_index();
                let mut row_groups_to_read = Vec::new();
                let mut row_selection = RowSelection::default();

                let mut row_groups_pruned_by_statistics = 0;
                let mut row_groups_selected_by_statistics = 0;
                let mut row_groups_pruned_by_bloom_filters = 0;
                let mut row_groups_selected_by_bloom_filters = 0;
                let mut row_groups_pruned_by_page_index = 0;
                let mut row_groups_selected_by_page_index = 0;

                // Index of the first row in the current group within the current row group selection.
                // This is 0 for every row group that does not have a selected row group before itself.
                // See https://docs.rs/parquet/53.1.0/parquet/arrow/async_reader/type.ParquetRecordBatchStreamBuilder.html#tymethod.with_row_selection
                let mut current_row_group_first_row_idx = 0usize;

                for (row_group_idx, row_group_meta) in
                    parquet_metadata.row_groups().iter().enumerate()
                {
                    let num_rows_in_row_group = usize::try_from(row_group_meta.num_rows())
                        .context("number of rows in row group overflowed usize")?;
                    let next_row_group_first_row_idx = current_row_group_first_row_idx
                        .checked_add(num_rows_in_row_group)
                        .context("Number of rows in file overflowed usize")?;
                    let mut keys_in_group: Vec<_> = keys.iter().cloned().collect();
                    let mut group_row_selection = RowSelection::from_consecutive_ranges([current_row_group_first_row_idx..next_row_group_first_row_idx].into_iter(), next_row_group_first_row_idx);

                    // Prune row group using statistics
                    if let Some(column_chunk_statistics) =
                        row_group_meta.column(column_idx).statistics()
                    {
                        keys_in_group = keys_in_group
                            .into_iter()
                            .filter(|key| key.check_column_chunk(column_chunk_statistics))
                            .collect();
                        if keys_in_group.is_empty() {
                            row_groups_pruned_by_statistics += 1;
                            continue; // shortcut
                        }
                        row_groups_selected_by_statistics += 1;
                    }

                    // Prune row groups using Bloom Filters
                    if let Some(bloom_filter) = stream_builder
                        .get_row_group_column_bloom_filter(row_group_idx, column_idx)
                        .await
                        .context("Could not get Bloom Filter")?
                    {
                        keys_in_group = keys_in_group
                            .into_iter()
                            .filter(|key| bloom_filter.check(key))
                            .collect();
                        if keys_in_group.is_empty() {
                            row_groups_pruned_by_bloom_filters += 1;
                            continue; // shortcut
                        }
                        row_groups_selected_by_bloom_filters += 1;
                    }

                    // Prune pages using page index
                    if let Some(column_index) = column_index {
                        let column_chunk_index = &column_index[row_group_idx][column_idx];
                        // Get the list of pages that may contain any of the keys

                        // TODO: optimize this to avoid redundant work (eg. when a page is already a
                        // candidate, we don't need to check it again; and when all pages are selected
                        // for the first keys, we can skip checking the remaining keys)
                        let candidate_pages = keys_in_group
                            .into_iter()
                            .map(|key| key.check_page_index(&column_chunk_index))
                            .reduce(|left, right| {
                                Ok(match (left?, right?) {
                                    (None, right) => right,
                                    (Some(left), None) => Some(left),
                                    (Some(left), Some(right)) => Some(
                                        arrow::compute::kernels::boolean::or(&left, &right)
                                            .context(
                                                "Could not reduce results of page index checks",
                                            )?,
                                    ),
                                })
                            })
                            .expect("Empty set of keys")?; // can't panic, we checked before
                        if let Some(candidate_pages) = candidate_pages {
                            if candidate_pages.is_empty() {
                                row_groups_pruned_by_page_index += 1;
                                continue; // shortcut
                            }
                            let offset_index =
                                offset_index.expect("column_index is present but offset_index is not");
                            let candidate_pages_locations: Box<[_]> = offset_index[row_group_idx]
                                [column_idx]
                                .page_locations()
                                .into_iter()
                                .zip(candidate_pages.into_iter())
                                .filter(|(_, is_candidate_page)| {
                                    is_candidate_page.expect("null value in bool array")
                                })
                                .map(|(page_location, _)| page_location.clone())
                                .collect();

                            let ranges_within_row_group =
                                RowSelection::default().scan_ranges(&candidate_pages_locations);
                            let ranges_within_selected_file =
                                ranges_within_row_group
                                    .into_iter()
                                    .map(|Range { start, end }| Range {
                                        start: start + current_row_group_first_row_idx,
                                        end: end + current_row_group_first_row_idx,
                                    });
                            group_row_selection = RowSelection::from_consecutive_ranges(
                                ranges_within_selected_file,
                                // max row id in the selection:
                                next_row_group_first_row_idx,
                            )
                        }
                    }
                    if group_row_selection.selects_any() {
                        row_groups_to_read.push(row_group_idx);
                        current_row_group_first_row_idx = next_row_group_first_row_idx;
                        row_groups_selected_by_page_index += 1;
                        row_selection = row_selection.union(&group_row_selection)
                    } else {
                        row_groups_pruned_by_page_index += 1;
                    }
                }
                tracing::trace!("Statistics pruned {}, selected {} row groups", row_groups_pruned_by_statistics, row_groups_selected_by_statistics);
                tracing::trace!("Bloom Filters pruned {}, selected {} row groups", row_groups_pruned_by_bloom_filters, row_groups_selected_by_bloom_filters);
                tracing::trace!("Page Index pruned {}, selected {} row groups", row_groups_pruned_by_page_index, row_groups_selected_by_page_index);
                tracing::trace!("Page Index pruned {}, selected {} rows", row_selection.skipped_row_count(), row_selection.row_count());
                Ok(stream_builder
                    .with_row_groups(row_groups_to_read)
                    .with_row_selection(row_selection))
            })
            .collect::<FuturesUnordered<_>>()
            .await)
    }
}
