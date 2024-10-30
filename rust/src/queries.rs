// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::{anyhow, bail, ensure, Context, Result};
use futures::stream::FuturesUnordered;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use parquet_aramid::metrics::TableScanInitMetrics;
use parquet_aramid::{
    arrow,
    arrow::array::*,
    arrow::datatypes::*,
    parquet::arrow::arrow_reader::{ArrowPredicate, RowFilter},
    parquet::arrow::async_reader::AsyncFileReader,
    parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask},
    parquet::schema::types::SchemaDescriptor,
};
use parquet_aramid::{ReaderBuilderConfigurator, Table};
use swh_graph::SWHID;
use tracing::{instrument, span_enabled, Level};

use swh_graph::graph::SwhGraphWithProperties;
use swh_graph::properties::NodeIdFromSwhidError;

use crate::database::metrics::TableScanMetrics;
use crate::database::ProvenanceDatabase;
use crate::proto;

pub type NodeId = u64;

/// Given a Parquet schema and a list of columns, returns a [`ProjectionMask`] that can be passed
/// to [`parquet`] to select which columns to read.
fn projection_mask(
    schema: &SchemaDescriptor,
    columns: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<ProjectionMask> {
    let column_indices = columns
        .into_iter()
        .map(|column_name| {
            let column_name = column_name.as_ref();
            schema
                .columns()
                .iter()
                .position(|column| column.name() == column_name)
                .with_context(|| format!("{:?} has no column named {}", schema, column_name))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ProjectionMask::roots(schema, column_indices))
}

/// Queries the ``keys`` from the c_in_r/c_in_d/d_in_r table.
///
/// `keys` must be sorted.
///
/// `limit` is per-file, so it is an upper bound to the number of results.
#[instrument(skip(table, expected_schema, key_column, value_column), fields(table=%table.path()))]
async fn query_x_in_y_table<'a>(
    table: &'a Table,
    expected_schema: Arc<Schema>,
    key_column: &'static str,
    value_column: &'static str,
    keys: Arc<Vec<u64>>,
    limit: Option<usize>,
) -> Result<(
    TableScanInitMetrics,
    Arc<TableScanMetrics>,
    impl Stream<Item = Result<RecordBatch>> + Send + 'a,
)> {
    let metrics = Arc::new(TableScanMetrics::default());

    /// Used to filter out rows that do not match the key early, ie. before deserializing the values
    struct Predicate {
        projection: ProjectionMask,
        key_column: &'static str,
        keys: Arc<Vec<u64>>,
        metrics: Arc<TableScanMetrics>,
    }

    impl ArrowPredicate for Predicate {
        /// Which columns to deserialize to evaluate this predicate
        fn projection(&self) -> &ProjectionMask {
            &self.projection
        }

        /// Evaluate the predicate for a RecordBatch, returning a batch of booleans
        fn evaluate(
            &mut self,
            batch: RecordBatch,
        ) -> Result<BooleanArray, arrow::error::ArrowError> {
            let _guard = self.metrics.row_filter_eval_time.timer();
            let mut num_selected = 0;

            // Initialize array of booleans indicating whether each row in the batch should be
            // deserialized
            let mut matches = arrow::array::builder::BooleanBufferBuilder::new(batch.num_rows());

            {
                let _guard = self.metrics.row_filter_eval_loop_time.timer();

                // Get the array of cells in the key column of this batch
                let candidates = batch
                    .column_by_name(self.key_column)
                    .expect("Missing key column")
                    .as_primitive_opt::<UInt64Type>()
                    .expect("key column is not a UInt64Array");

                if self.keys.len() <= 4 {
                    // TODO: tune this constant
                    // If there are few keys, check each candidate exhaustively against every key.
                    // This is faster than a binary search in this case
                    for candidate in candidates {
                        let candidate = candidate.expect("Null key in table");
                        let is_match = self.keys.iter().any(|key| key == &candidate);
                        num_selected += is_match as u64;
                        matches.append(is_match);
                    }
                } else {
                    // else, rely on keys being sorted, and check each candidate against keys by
                    // performing a binary search against the keys, which is faster given enough
                    // keys.
                    for candidate in candidates {
                        let candidate = candidate.expect("Null key in table");
                        let is_match = self.keys.binary_search(&candidate).is_ok();
                        num_selected += is_match as u64;
                        matches.append(is_match);
                    }
                }
            }

            // Update metrics with this batch's results
            let matches = matches.finish();
            self.metrics
                .rows_selected_by_row_filter
                .fetch_add(num_selected, Ordering::Relaxed);
            self.metrics.rows_pruned_by_row_filter.fetch_add(
                u64::try_from(matches.len()).expect("number of rows overflows u64") - num_selected,
                Ordering::Relaxed,
            );

            // Return for each row, whether it should be deserialized
            Ok(arrow::array::BooleanArray::new(matches, None))
        }
    }

    /// Configures a [`ParquetRecordBatchStreamBuilder`] to read only columns we are interested in,
    /// only rows matching the given keys, and with a limited number of results.
    struct Configurator {
        expected_schema: Arc<Schema>,
        key_column: &'static str,
        value_column: &'static str,
        keys: Arc<Vec<u64>>,
        limit: Option<usize>,
        metrics: Arc<TableScanMetrics>,
    }
    impl ReaderBuilderConfigurator for Configurator {
        fn configure<R: AsyncFileReader>(
            &self,
            mut reader_builder: ParquetRecordBatchStreamBuilder<R>,
        ) -> Result<ParquetRecordBatchStreamBuilder<R>> {
            // Check the schema of columns we are going to read matches our expectations
            let mut schema_projection = Vec::new();
            for field in self.expected_schema.fields() {
                let Some((column_idx, _)) = reader_builder.schema().column_with_name(field.name())
                else {
                    bail!("Missing column {} in table", field.name())
                };
                schema_projection.push(column_idx);
            }
            let projected_schema = reader_builder
                .schema()
                .project(&schema_projection)
                .expect("could not project schema");
            ensure!(
                projected_schema.fields() == self.expected_schema.fields(),
                "Unexpected schema: got {:#?} instead of {:#?}",
                projected_schema.fields(),
                self.expected_schema.fields()
            );

            // Only read these two columns (ie. not 'revrel_author_date' or 'path')
            let projection = projection_mask(
                reader_builder.parquet_schema(),
                [self.key_column, self.value_column],
            )
            .context("Could not project {} table for reading")?;
            reader_builder = reader_builder.with_projection(projection);

            // Further configure the reader builders to only return rows that
            // actually contain one of the keys in the input; then build readers and stream
            // their results.
            let row_filter = RowFilter::new(vec![Box::new(Predicate {
                // Don't read the other columns yet, we don't need them for filtering
                projection: projection_mask(reader_builder.parquet_schema(), [self.key_column])
                    .context("Could not project table for filtering")?,
                key_column: self.key_column,
                keys: Arc::clone(&self.keys),
                metrics: Arc::clone(&self.metrics),
            })]);
            reader_builder = reader_builder.with_row_filter(row_filter);

            // Limit the number of results to return
            if let Some(limit) = self.limit {
                reader_builder = reader_builder.with_limit(limit);
            }

            Ok(reader_builder)
        }
    }

    let scan_metrics = Arc::clone(&metrics);

    // Get a stream of batches of rows
    let (scan_init_metrics, stream) = table
        // Get Parquet reader builders configured to only read pages that *probably* contain
        // one of the keys in the query, using indices.
        .stream_for_keys(
            key_column,
            Arc::clone(&keys),
            Arc::new(Configurator {
                expected_schema,
                key_column,
                value_column,
                keys,
                limit,
                metrics,
            }),
        )
        .await
        .context("Could not start reading from table")?;

    Ok((scan_init_metrics, scan_metrics, stream))
}

pub struct ProvenanceService<
    G: SwhGraphWithProperties<Maps: swh_graph::properties::Maps> + Send + Sync + 'static,
> {
    pub db: ProvenanceDatabase,
    pub graph: G,
}

impl<G: SwhGraphWithProperties<Maps: swh_graph::properties::Maps> + Send + Sync + 'static>
    ProvenanceService<G>
{
    /// Given a list of SWHIDs, returns their ids, in any order
    ///
    /// TODO: if a SWHID can't be found, return others' node ids, and only error for that one.
    #[instrument(skip(self), fields(swhids=swhids.iter().map(AsRef::as_ref).join(", ")))]
    async fn node_id(&self, swhids: &[impl AsRef<str>]) -> Result<Result<Vec<u64>, tonic::Status>> {
        tracing::debug!(
            "Getting node id for {:?}",
            swhids.iter().map(AsRef::as_ref).collect::<Vec<_>>()
        );

        let mut node_ids = Vec::<u64>::new();
        for swhid in swhids {
            let swhid = swhid.as_ref();
            match self.graph.properties().node_id_from_string_swhid(swhid) {
                Ok(node_id) => node_ids.push(node_id.try_into().expect("Node id overflowed u64")),
                Err(NodeIdFromSwhidError::InvalidSwhid(_)) => {
                    return Ok(Err(tonic::Status::invalid_argument(format!(
                        "Unknown SWHID: {}",
                        swhid
                    ))))
                }
                Err(NodeIdFromSwhidError::UnknownSwhid(_)) => {
                    return Ok(Err(tonic::Status::not_found(format!(
                        "Unknown SWHID: {}",
                        swhid
                    ))))
                }
                Err(NodeIdFromSwhidError::InternalError(e)) => return Err(anyhow!("{}", e)),
            }
        }

        Ok(Ok(node_ids))
    }

    /// Given a RecordBatch with a column of `NodeId` and one of `Option<NodeId>`, converts
    /// all the node ids into SWHIDs and returns the pairs in any order.
    #[instrument(skip(self, node_id_batches))]
    async fn swhid(
        &self,
        node_id_batches: Vec<RecordBatch>,
        col1: &'static str,
        col2: &'static str,
    ) -> Result<Vec<(SWHID, Option<SWHID>)>> {
        tracing::debug!("Getting SWHIDs from node ids");
        let mut swhids =
            Vec::with_capacity(node_id_batches.iter().map(|batch| batch.num_rows()).sum());
        for batch in node_id_batches {
            swhids.extend(
                std::iter::zip(
                    batch
                        .column_by_name(col1)
                        .with_context(|| format!("Could not get '{}' column from batch", col1))?
                        .as_primitive_opt::<UInt64Type>()
                        .with_context(|| {
                            format!("Could not cast '{}' column as UInt64Array", col1)
                        })?
                        .into_iter(),
                    batch
                        .column_by_name(col2)
                        .with_context(|| format!("Could not get '{}' column from batch", col2))?
                        .as_primitive_opt::<UInt64Type>()
                        .with_context(|| {
                            format!("Could not cast '{}' column as UInt64Array", col2)
                        })?
                        .into_iter(),
                )
                .map(|(id1, id2)| {
                    let Some(id1) = id1 else {
                        panic!("Got null value for '{}'", col1)
                    };
                    (
                        self.graph
                            .properties()
                            .swhid(id1.try_into().expect("Node id overflowed usize")),
                        id2.map(|id2| {
                            self.graph
                                .properties()
                                .swhid(id2.try_into().expect("Node id overflowed usize"))
                        }),
                    )
                }),
            );
        }
        Ok(swhids)
    }

    /// Given a content SWHID, returns any of the revision/release that SWHID is in.
    #[instrument(skip(self))]
    pub async fn where_is_one(
        &self,
        swhid: String,
    ) -> Result<
        Result<
            (
                TableScanInitMetrics,
                TableScanMetrics,
                proto::WhereIsOneResult,
            ),
            tonic::Status,
        >,
    > {
        let node_ids = Arc::new(self.node_id(&[&swhid]).await??);

        if span_enabled!(Level::TRACE) {
            tracing::trace!("Query node ids: {:?}", node_ids)
        }

        tracing::debug!("Looking up c_in_r");

        // Start reading from the table
        let schema = Arc::new(Schema::new(vec![
            Field::new("cnt", DataType::UInt64, false),
            Field::new("revrel", DataType::UInt64, false),
            Field::new("path", DataType::Binary, false),
        ]));
        let limit = 1;
        let (scan_init_metrics, scan_metrics, c_in_r_stream) = query_x_in_y_table(
            &self.db.c_in_r,
            schema,
            "cnt",
            "revrel",
            node_ids,
            Some(limit),
        )
        .await
        .context("Could not query c_in_r")?;
        tracing::trace!("Got c_in_r_stream");
        tracing::debug!("Scan init metrics: {:#?}", scan_init_metrics);

        // Read batches of rows, stopping after the first one
        let mut remaining_rows = limit;
        let c_in_r_batches = c_in_r_stream
            .take_while(move |batch| {
                std::future::ready(match batch {
                    Ok(batch) => {
                        let res = remaining_rows > 0;
                        remaining_rows = remaining_rows.saturating_sub(batch.num_rows());
                        res
                    }
                    Err(_) => true,
                })
            })
            .collect::<FuturesUnordered<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        tracing::trace!("Got c_in_r_batches");
        if span_enabled!(Level::TRACE) {
            tracing::trace!("Anchor node ids: {:?}", c_in_r_batches,)
        }
        let scan_metrics =
            Arc::try_unwrap(scan_metrics).expect("Dangling reference to scan_metrics");
        tracing::debug!("Scan metrics: {:#?}", scan_metrics);

        // Translate results' node ids to SWHIDs
        // Note: c_in_r_batches may have more than one row; the above filter only guarantees there
        // is at most one RecordBatch.
        let mut anchors = self.swhid(c_in_r_batches, "cnt", "revrel").await?;

        // And return the first result
        if let Some((cnt, revrel)) = anchors.pop() {
            return Ok(Ok((
                scan_init_metrics,
                scan_metrics,
                proto::WhereIsOneResult {
                    swhid: cnt.to_string(),
                    anchor: revrel.map(|revrel| revrel.to_string()),
                    origin: None,
                },
            )));
        }

        /* TODO:
        tracing::debug!("Looking up c_in_d + d_in_r");
        */

        // No result
        Ok(Ok((
            scan_init_metrics,
            scan_metrics,
            proto::WhereIsOneResult {
                swhid,
                ..Default::default()
            },
        )))
    }
}
