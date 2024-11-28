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

#[derive(Default, Debug)]
pub struct Metrics {
    c_in_r_init: TableScanInitMetrics,
    c_in_r_scan: TableScanMetrics,
    c_in_d_init: TableScanInitMetrics,
    c_in_d_scan: TableScanMetrics,
    d_in_r_init: TableScanInitMetrics,
    d_in_r_scan: TableScanMetrics,
}

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
    table_name: &'static str,
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
        table_name: &'static str,
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
            .with_context(|| format!("Could not project {} table for reading", self.table_name))?;
            reader_builder = reader_builder.with_projection(projection);

            // Further configure the reader builders to only return rows that
            // actually contain one of the keys in the input; then build readers and stream
            // their results.
            let row_filter = RowFilter::new(vec![Box::new(Predicate {
                // Don't read the other columns yet, we don't need them for filtering
                projection: projection_mask(reader_builder.parquet_schema(), [self.key_column])
                    .with_context(|| {
                        format!("Could not project {} table for filtering", self.table_name)
                    })?,
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
                table_name,
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

/// Reads a stream of [`RecordBatch`], and stops once `limit` rows were obtained.
///
/// The total number of rows returns may be larger than `limit`, as it contains
/// the whole batch that reached the `limit` at the end.
async fn consume_batch_stream(
    stream: impl Stream<Item = Result<RecordBatch>>,
    limit: usize,
) -> Result<Vec<RecordBatch>> {
    let mut remaining_rows = limit;
    stream
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
        .collect()
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

    /// Given a [`NodeId`], returns a stream of records from the contents-in-revision table
    #[instrument(skip(self))]
    pub async fn query_c_in_r(
        &self,
        node_ids: Arc<Vec<NodeId>>,
        limit: Option<usize>,
    ) -> Result<(
        TableScanInitMetrics,
        Arc<TableScanMetrics>,
        impl Stream<Item = Result<RecordBatch>> + use<'_, G>,
    )> {
        tracing::debug!("Looking up c_in_r");

        // Start reading from the table
        let schema = Arc::new(Schema::new(vec![
            Field::new("cnt", DataType::UInt64, false),
            Field::new("revrel", DataType::UInt64, false),
            Field::new("path", DataType::Binary, false),
        ]));
        let (scan_init_metrics, scan_metrics, c_in_r_stream) = query_x_in_y_table(
            &self.db.c_in_r,
            schema,
            "c_in_d", // table name, for error messages
            "cnt",
            "revrel",
            node_ids,
            limit,
        )
        .await
        .context("Could not query c_in_r")?;
        tracing::trace!("Got c_in_r_stream");
        tracing::debug!("Scan init metrics: {:#?}", scan_init_metrics);

        Ok((scan_init_metrics, scan_metrics, c_in_r_stream))
    }

    /// Given a [`NodeId`], returns some records from the contents-in-revision table
    #[instrument(skip(self))]
    pub async fn query_c_in_r_one(
        &self,
        node_id: NodeId,
    ) -> Result<(TableScanInitMetrics, TableScanMetrics, Vec<RecordBatch>)> {
        let limit = 1;

        let (scan_init_metrics, scan_metrics, c_in_r_stream) = self
            .query_c_in_r(Arc::new(vec![node_id]), Some(limit))
            .await?;

        // Read batches of rows, stopping after the first one
        let batches = consume_batch_stream(c_in_r_stream, limit).await?;
        tracing::trace!("Got c_in_r_batches");
        if span_enabled!(Level::TRACE) {
            tracing::trace!("Anchor node ids: {:?}", batches)
        }
        let scan_metrics =
            Arc::try_unwrap(scan_metrics).expect("Dangling reference to scan_metrics");
        tracing::debug!("Scan metrics: {:#?}", scan_metrics);
        Ok((scan_init_metrics, scan_metrics, batches))
    }

    /// Given a [`NodeId`], returns a stream of records from the contents-in-directory table
    #[instrument(skip(self))]
    pub async fn query_c_in_d(
        &self,
        node_ids: Arc<Vec<NodeId>>,
    ) -> Result<(
        TableScanInitMetrics,
        Arc<TableScanMetrics>,
        impl Stream<Item = Result<RecordBatch>> + use<'_, G>,
    )> {
        tracing::debug!("Looking up c_in_d");

        // Start reading from the table
        let schema = Arc::new(Schema::new(vec![
            Field::new("cnt", DataType::UInt64, false),
            Field::new("dir", DataType::UInt64, false),
            Field::new("path", DataType::Binary, false),
        ]));
        let (scan_init_metrics, scan_metrics, c_in_d_stream) = query_x_in_y_table(
            &self.db.c_in_d,
            schema,
            "c_in_d", // table name, for error messages
            "cnt",
            "dir",
            node_ids,
            None, // no limit
        )
        .await
        .context("Could not query c_in_d")?;
        tracing::trace!("Got c_in_d_stream");
        tracing::debug!("Scan init metrics: {:#?}", scan_init_metrics);

        Ok((scan_init_metrics, scan_metrics, c_in_d_stream))
    }

    /// Given a [`NodeId`], returns some records from the contents-in-revision table
    #[instrument(skip(self))]
    pub async fn query_d_in_r(
        &self,
        node_ids: Arc<Vec<NodeId>>,
        limit: Option<usize>,
    ) -> Result<(
        TableScanInitMetrics,
        Arc<TableScanMetrics>,
        impl Stream<Item = Result<RecordBatch>> + use<'_, G>,
    )> {
        tracing::debug!("Looking up d_in_r");

        // Start reading from the table
        let schema = Arc::new(Schema::new(vec![
            Field::new("dir", DataType::UInt64, false),
            Field::new("revrel", DataType::UInt64, false),
            Field::new("path", DataType::Binary, false),
        ]));
        let (scan_init_metrics, scan_metrics, d_in_r_stream) = query_x_in_y_table(
            &self.db.d_in_r,
            schema,
            "d_in_r", // table name, for error messages
            "dir",
            "revrel",
            node_ids,
            limit,
        )
        .await
        .context("Could not query d_in_r")?;
        tracing::trace!("Got d_in_r_stream");
        tracing::debug!("Scan init metrics: {:#?}", scan_init_metrics);

        Ok((scan_init_metrics, scan_metrics, d_in_r_stream))
    }

    /// Given a [`NodeId`], returns some records from the contents-in-revision table
    #[instrument(skip(self))]
    pub async fn query_d_in_r_one(
        &self,
        node_id: NodeId,
    ) -> Result<(TableScanInitMetrics, TableScanMetrics, Vec<RecordBatch>)> {
        let limit = 1;

        let (scan_init_metrics, scan_metrics, d_in_r_stream) = self
            .query_d_in_r(Arc::new(vec![node_id]), Some(limit))
            .await?;

        // Read batches of rows, stopping after the first one
        let batches = consume_batch_stream(d_in_r_stream, limit).await?;
        tracing::trace!("Got d_in_r_batches");
        if span_enabled!(Level::TRACE) {
            tracing::trace!("Anchor node ids: {:?}", batches)
        }
        let scan_metrics =
            Arc::try_unwrap(scan_metrics).expect("Dangling reference to scan_metrics");
        tracing::debug!("Scan metrics: {:#?}", scan_metrics);
        Ok((scan_init_metrics, scan_metrics, batches))
    }

    /// Given a content SWHID, returns any of the revision/release that SWHID is in.
    #[instrument(skip(self))]
    pub async fn where_is_one(
        &self,
        swhid: String,
    ) -> Result<Result<(Metrics, proto::WhereIsOneResult), tonic::Status>> {
        let mut metrics = Metrics::default();
        let node_id = self
            .node_id(&[&swhid])
            .await??
            .pop()
            .expect("node_id returned empty Ok result");

        if span_enabled!(Level::TRACE) {
            tracing::trace!("Query node id: {}", node_id)
        }

        let (scan_init_metrics, scan_metrics, c_in_r_batches) =
            self.query_c_in_r_one(node_id).await?;
        metrics.c_in_r_init = scan_init_metrics;
        metrics.c_in_r_scan = scan_metrics;

        // Translate results' node ids to SWHIDs
        // Note: c_in_r_batches may have more than one row; the above filter only guarantees there
        // is at most one RecordBatch.
        let mut anchors = self.swhid(c_in_r_batches, "cnt", "revrel").await?;

        // And return the first result
        if let Some((cnt, revrel)) = anchors.pop() {
            return Ok(Ok((
                metrics,
                proto::WhereIsOneResult {
                    swhid: cnt.to_string(),
                    anchor: revrel.map(|revrel| revrel.to_string()),
                    origin: None,
                },
            )));
        }

        tracing::debug!("Looking up c_in_d + d_in_r");
        // First look up the list of directories
        let (c_in_d_scan_init_metrics, c_in_d_scan_metrics, mut c_in_d_batches) =
            self.query_c_in_d(Arc::new(vec![node_id])).await?;
        metrics.c_in_d_init = c_in_d_scan_init_metrics;
        while let Some(c_in_d_batch) = c_in_d_batches.next().await {
            let c_in_d_batch = c_in_d_batch?;
            let dirs = c_in_d_batch
                .column_by_name("dir")
                .context("Could not get 'dir' column from batch")?
                .as_primitive_opt::<UInt64Type>()
                .context("'dir' column is not UInt64Array")?;
            // For each directory...
            for dir in dirs {
                let dir = dir.expect("'dir' is null");
                // ...query the list of revisions this directory is in
                let (scan_init_metrics, scan_metrics, d_in_r_batches) =
                    self.query_d_in_r_one(dir).await?;
                metrics.d_in_r_init += scan_init_metrics;
                metrics.d_in_r_scan += scan_metrics;

                // Join the single content with the results from d_in_r.
                // XXX: This is going to be more complicated when this function adds support for
                // multiple contents at once!
                let Some(d_in_r_batch) = d_in_r_batches.into_iter().next() else {
                    // Shouldn't happen
                    tracing::error!(
                        "Directory {} is in no revision?!",
                        self.graph
                            .properties()
                            .swhid(dir.try_into().expect("Node id overflowed usize"))
                    );
                    continue;
                };
                let batch = RecordBatch::try_from_iter([
                    (
                        "cnt",
                        Arc::new(UInt64Array::from_iter(std::iter::repeat_n(
                            node_id,
                            d_in_r_batch.num_rows(),
                        ))) as _,
                    ),
                    (
                        "revrel",
                        Arc::clone(
                            d_in_r_batch
                                .column_by_name("revrel")
                                .context("Could not get 'revrel' column from batch")?,
                        ),
                    ),
                ])
                .context("Could not build RecordBatch of cnt and revrel")?;

                // Translate results' node ids to SWHIDs
                // Note: d_in_r_batches may have more than one row; the above filter only guarantees there
                // is at most one RecordBatch.
                let mut anchors = self.swhid(vec![batch], "cnt", "revrel").await?;

                // And return the first result
                if let Some((cnt, revrel)) = anchors.pop() {
                    metrics.c_in_d_scan += c_in_d_scan_metrics;
                    return Ok(Ok((
                        metrics,
                        proto::WhereIsOneResult {
                            swhid: cnt.to_string(),
                            anchor: revrel.map(|revrel| revrel.to_string()),
                            origin: None,
                        },
                    )));
                }
            }
        }
        metrics.c_in_d_scan += c_in_d_scan_metrics;

        // No result
        Ok(Ok((
            metrics,
            proto::WhereIsOneResult {
                swhid,
                ..Default::default()
            },
        )))
    }
}
