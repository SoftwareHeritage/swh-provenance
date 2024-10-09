// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, bail, ensure, Context, Result};
use arrow::array::*;
use arrow::datatypes::*;
use futures::stream::FuturesUnordered;
use futures::{Stream, StreamExt, TryStreamExt};
use itertools::Itertools;
use parquet::arrow::arrow_reader::{ArrowPredicate, ArrowPredicateFn, RowFilter};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::schema::types::SchemaDescriptor;
use swh_graph::SWHID;
use tracing::{instrument, span_enabled, Level};

use swh_graph::graph::SwhGraphWithProperties;
use swh_graph::properties;
use swh_graph::properties::NodeIdFromSwhidError;

use crate::database::types::Sha1Git;
use crate::database::{ProvenanceDatabase, ReaderBuilderConfigurator, Table};
use crate::proto;

pub type NodeId = u64;

fn decode_swhids_from_batch(batch: &RecordBatch) -> Result<impl Iterator<Item = SWHID> + '_> {
    assert_eq!(
        **batch.schema_ref(),
        Schema::new(vec![
            // TODO: allow more flexibility to the "type"
            Field::new(
                "type",
                DataType::Dictionary(DataType::Int8.into(), DataType::Utf8.into()),
                false
            ),
            Field::new("sha1_git", DataType::FixedSizeBinary(20), false),
        ])
    );

    // See https://docs.rs/datafusion/latest/datafusion/common/arrow/array/struct.DictionaryArray.html
    // for a description of what dictionary arrays are.
    // In order to avoid parsing the same strings over and over, we only parse the
    // values (there are few of them), and keep the keys untouched.
    //
    // Unwrap won't panic because we checked the schema does not allow NULLs
    let node_type_str_dict = batch.column(0).as_dictionary::<Int8Type>();
    let node_type_keys = node_type_str_dict.keys_iter().map(Option::unwrap);
    let node_type_values = node_type_str_dict
        .values()
        .as_string::<i32>()
        .into_iter()
        .map(Option::unwrap)
        .map(|key| {
            key.parse()
                .map_err(|node_type| anyhow!("Invalid node type: {node_type}"))
        })
        .collect::<Result<Vec<_>>>()?;

    // Unwrap won't panic because we checked the schema does not allow NULLs
    let sha1_gits = batch
        .column(1)
        .as_fixed_size_binary()
        .into_iter()
        .map(Option::unwrap);
    Ok(
        std::iter::zip(node_type_keys, sha1_gits).map(move |(node_type_key, sha1_git)| {
            SWHID {
                namespace_version: 1,
                node_type: *node_type_values
                    .get(node_type_key)
                    .expect("Could not find Arrow dictionary key in values array"),
                // .expect() won't panic because we checked the size in the schema
                hash: sha1_git.try_into().expect("Unexpected sha1_git size"),
            }
        }),
    )
}

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
/// ``keys`` must be sorted.
#[instrument(skip(table), fields(table=%table.path()))]
async fn query_x_in_y_table<'a>(
    table: &'a Table,
    expected_schema: Arc<Schema>,
    key_column: &'static str,
    value_column: &'static str,
    keys: Arc<Vec<u64>>,
) -> Result<impl Stream<Item = Result<RecordBatch>> + Send + 'a> {
    struct Predicate {
        projection: ProjectionMask,
        key_column: &'static str,
        keys: Arc<Vec<u64>>,
    }

    impl ArrowPredicate for Predicate {
        fn projection(&self) -> &ProjectionMask {
            &self.projection
        }
        fn evaluate(
            &mut self,
            batch: RecordBatch,
        ) -> Result<BooleanArray, arrow::error::ArrowError> {
            let mut matches = arrow::array::builder::BooleanBufferBuilder::new(batch.num_rows());
            for (i, key) in batch
                .column_by_name(self.key_column)
                .expect("Missing key column")
                .as_primitive_opt::<UInt64Type>()
                .expect("key column is not a UInt64Array")
                .into_iter()
                .enumerate()
            {
                let key = key.expect("Null key in table");
                matches.append(self.keys.binary_search(&key).is_ok());
            }
            Ok(arrow::array::BooleanArray::new(matches.finish(), None))
        }
    }

    struct Configurator {
        expected_schema: Arc<Schema>,
        key_column: &'static str,
        value_column: &'static str,
        keys: Arc<Vec<u64>>,
    }
    impl ReaderBuilderConfigurator for Configurator {
        fn configure<R: AsyncFileReader>(
            &self,
            reader_builder: ParquetRecordBatchStreamBuilder<R>,
        ) -> Result<ParquetRecordBatchStreamBuilder<R>> {
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

            // discard 'revrel_author_date' and 'path' columns
            let projection = projection_mask(
                reader_builder.parquet_schema(),
                [self.key_column, self.value_column],
            )
            .context("Could not project {} table for reading")?;
            let reader_builder = reader_builder.with_projection(projection);

            // Further configure the reader builders to only return rows that
            // actually contain one of the keys in the input; then build readers and stream
            // their results.
            let row_filter = RowFilter::new(vec![Box::new(Predicate {
                // Don't read the other columns yet, we don't need them for filtering
                projection: projection_mask(reader_builder.parquet_schema(), [self.key_column])
                    .context("Could not project table for filtering")?,
                key_column: self.key_column,
                keys: Arc::clone(&self.keys),
            })]);
            Ok(reader_builder.with_row_filter(row_filter))
        }
    }
    Ok(table
        // Get Parquet reader builders configured to only read pages that *probably* contain
        // one of the keys in the query, using indices.
        .filtered_record_batch_stream_builder(
            key_column,
            Arc::clone(&keys),
            Arc::new(Configurator {
                expected_schema,
                key_column,
                value_column,
                keys,
            }),
        )
        .await
        .context("Could not start reading from table")?)
}
async fn node_ids_from_swhids(
    table: &Table,
    swhids: &[impl AsRef<str>],
) -> Result<Result<Vec<u64>, tonic::Status>> {
    // Parse SWHIDs
    let mut parsed_swhids = Vec::new();
    for swhid in swhids {
        let swhid = swhid.as_ref();
        let Ok(parsed_swhid) = SWHID::from_str(swhid) else {
            return Ok(Err(tonic::Status::invalid_argument(format!(
                "{} is not a valid SWHID",
                swhid
            ))));
        };
        parsed_swhids.push(parsed_swhid);
    }

    // Split SWHIDs into columns
    let node_types: Vec<_> = parsed_swhids
        .iter()
        .map(|swhid| swhid.node_type.to_str())
        .collect();
    let mut sha1_gits: Vec<_> = parsed_swhids
        .iter()
        .map(|swhid| Sha1Git(swhid.hash))
        .collect();
    sha1_gits.sort_unstable();
    let sha1_gits = Arc::new(sha1_gits);
    let sha1_gits_set: HashSet<_> = sha1_gits.iter().map(|sha1_git| sha1_git.0).collect();
    if sha1_gits.len() != sha1_gits_set.len() {
        return Ok(Err(tonic::Status::unimplemented(
            "Duplicated SWHIDs in input",
        ))); // TODO
    }

    let expected_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::UInt64, false),
        Field::new(
            "type",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("sha1_git", DataType::FixedSizeBinary(20), false),
    ]));

    struct Configurator {
        expected_schema: Arc<Schema>,
        sha1_gits: Arc<Vec<Sha1Git>>,
    }
    impl ReaderBuilderConfigurator for Configurator {
        fn configure<R: AsyncFileReader>(
            &self,
            reader_builder: ParquetRecordBatchStreamBuilder<R>,
        ) -> Result<ParquetRecordBatchStreamBuilder<R>> {
            // Further configure the reader builders to only output to only return rows that
            // actually contain one of the SWHIds in the input; then build readers and stream
            // their results.
            ensure!(
                reader_builder.schema().fields() == self.expected_schema.fields(),
                "Unexpected schema for nodes tables: got {:#?} instead of {:#?}",
                reader_builder.schema().fields(),
                self.expected_schema.fields()
            );
            let sha1_gits = Arc::clone(&self.sha1_gits);
            let row_filter = RowFilter::new(vec![Box::new(ArrowPredicateFn::new(
                // Don't read the 'id' column yet, we don't need it for filtering
                projection_mask(reader_builder.parquet_schema(), ["type", "sha1_git"])
                    .context("Could not project nodes table")?,
                move |batch| {
                    // TODO: check 'type' column
                    let mut matches =
                        arrow::array::builder::BooleanBufferBuilder::new(batch.num_rows());
                    for (i, sha1_git) in batch
                        .column_by_name("sha1_git")
                        .expect("Missing column sha1_git")
                        .as_fixed_size_binary_opt()
                        .expect("'sha1_git' column is not a FixedSizeBinaryArray")
                        .into_iter()
                        .enumerate()
                    {
                        // Can't panic because we check the schema before applying this row
                        // filter
                        let sha1_git: [u8; 20] = sha1_git
                            .expect("null sha1_git in nodes table")
                            .try_into()
                            .expect("unexpected sha1_git length in nodes table");
                        matches.append(sha1_gits.binary_search(&Sha1Git(sha1_git)).is_ok());
                    }
                    Ok(arrow::array::BooleanArray::new(matches.finish(), None))
                },
            ))]);
            Ok(reader_builder.with_row_filter(row_filter))
        }
    }

    // Get Parquet reader builders configured to only read pages that *probably* contain
    // one of the SWHIDs in the query, using indices.
    // TODO: use node_type to prune based on statistics too
    let mut batches = table
        .filtered_record_batch_stream_builder(
            "sha1_git",
            Arc::clone(&sha1_gits),
            Arc::new(Configurator {
                expected_schema,
                sha1_gits,
            }),
        )
        .await?;

    // Read 'id' from batches and check which SWHIDs are not in the table
    let mut node_ids = Vec::new();
    let mut unknown_swhids: HashSet<_> = parsed_swhids.into_iter().collect();
    while let Some(batch) = batches.next().await {
        let batch = batch?;
        tracing::trace!("Got batch with {} rows", batch.num_rows());
        node_ids.extend(
            batch
                .project(&[0])
                // can't panic, we checked the schema
                .expect("Could not project batch to 'id' column")
                .column(0)
                .as_primitive_opt::<UInt64Type>()
                // can't panic, we checked the schema
                .expect("Could not cast node ids to UInt64Array")
                .into_iter()
                // can't panic, we checked the schema
                .map(|node_id| node_id.expect("Node id is null")),
        );
        let swhids_batch = &batch.project(&[1, 2]).expect(
            "Could not remove 'id' column before passing batch to decode_swhids_from_batch",
        );
        for swhid in decode_swhids_from_batch(swhids_batch)? {
            ensure!(
                unknown_swhids.remove(&swhid),
                "Database returned SWHID not in query: {swhid}"
            );
        }
    }

    if !unknown_swhids.is_empty() {
        return Ok(Err(tonic::Status::not_found(format!(
            "Unknown SWHIDs: {}",
            unknown_swhids
                .into_iter()
                .map(|swhid| swhid.to_string())
                .join(", ")
        ))));
    }

    Ok(Ok(node_ids))
}

pub struct ProvenanceService<G: SwhGraphWithProperties + Send + Sync + 'static>
where
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    pub db: ProvenanceDatabase,
    pub graph: Option<G>,
}

impl<G: SwhGraphWithProperties + Send + Sync + 'static> ProvenanceService<G>
where
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    #[instrument(skip(self), fields(swhids=swhids.iter().map(AsRef::as_ref).join(", ")))]
    async fn node_id(&self, swhids: &[impl AsRef<str>]) -> Result<Result<Vec<u64>, tonic::Status>> {
        tracing::debug!(
            "Getting node id for {:?}",
            swhids.iter().map(AsRef::as_ref).collect::<Vec<_>>()
        );

        match &self.graph {
            Some(graph) => {
                // Convert from SWHID to node id using the graph
                let mut node_ids = Vec::<u64>::new();
                for swhid in swhids {
                    let swhid = swhid.as_ref();
                    match graph.properties().node_id_from_string_swhid(swhid) {
                        Ok(node_id) => {
                            node_ids.push(node_id.try_into().expect("Node id overflowed u64"))
                        }
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
                        Err(NodeIdFromSwhidError::InternalError(e)) => {
                            return Err(anyhow!("{}", e))
                        }
                    }
                }

                Ok(Ok(node_ids))
            }
            None => node_ids_from_swhids(&self.db.node, swhids).await,
        }
    }

    #[instrument(skip(self, node_ids))]
    async fn swhid(&self, node_ids: Vec<RecordBatch>) -> Result<Vec<SWHID>> {
        tracing::debug!("Getting SWHIDs from node ids");
        match &self.graph {
            Some(graph) => {
                // Convert from node id to SWHID using the graph
                todo!("node to swhid");
                /*
                let batches = transaction
                    .db()
                    .ctx
                    .sql(&format!("SELECT id FROM '{node_ids}'", node_ids = node_ids))
                    .await?
                    .collect()
                    .await?;
                let mut node_ids: Vec<u64> = Vec::new();
                for batch in batches {
                    assert_eq!(
                        **batch.schema_ref(),
                        Schema::new(vec![Field::new("id", DataType::UInt64, false)])
                    );
                    // Unwrap won't panic because we checked the schema does not allow NULLs
                    node_ids.extend(
                        batch
                            .column(0)
                            .as_primitive::<UInt64Type>()
                            .into_iter()
                            .map(Option::<u64>::unwrap),
                    );
                }
                Ok(node_ids
                    .into_iter()
                    .map(|node_id| {
                        graph
                            .properties()
                            .swhid(node_id.try_into().expect("Node id overflowed usize"))
                    })
                    .collect())*/
            }
            None => {
                // Convert from node id to SWHID using a JOIN
                todo!("node to swhid");
                /*
                let mut swhids = Vec::new()

                for batch in transaction
                    .db()
                    .ctx
                    .sql(&format!(
                        "
                        SELECT type, sha1_git
                        FROM node
                        INNER JOIN {node_ids}
                            ON (node.id={node_ids}.id)
                        ",
                        node_ids = node_ids,
                    ))
                    .await?
                    .collect()
                    .await?
                {
                    swhids.extend(decode_swhids_from_batch(&batch)?);
                }
                Ok(swhids)
                */
            }
        }
    }

    #[instrument(skip(self))]
    pub async fn whereis(
        &self,
        swhid: String,
    ) -> Result<Result<proto::WhereIsOneResult, tonic::Status>> {
        let node_ids = Arc::new(self.node_id(&[&swhid]).await??);

        if span_enabled!(Level::TRACE) {
            tracing::trace!("Query node ids: {:?}", node_ids)
        }

        tracing::debug!("Looking up c_in_r");
        let schema = Arc::new(Schema::new(vec![
            Field::new("cnt", DataType::UInt64, false),
            Field::new("revrel", DataType::UInt64, false),
            Field::new("path", DataType::Binary, false),
        ]));
        let c_in_r_batches = query_x_in_y_table(&self.db.c_in_r, schema, "cnt", "revrel", node_ids)
            .await
            .context("Could not query c_in_r")?
            .collect::<FuturesUnordered<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        if span_enabled!(Level::TRACE) {
            tracing::trace!("Anchor node ids: {:?}", c_in_r_batches,)
        }
        let mut anchors = self.swhid(c_in_r_batches).await?;
        if let Some(anchor) = anchors.pop() {
            return Ok(Ok(proto::WhereIsOneResult {
                swhid,
                anchor: Some(anchor.to_string()),
                origin: None,
            }));
        }

        /* TODO
        tracing::debug!("Looking up c_in_d + d_in_r");
        let row: Option<AnchorRow> = self
            .fetch_one(format!(
                "
                SELECT first_value(revrel) AS revrel
                FROM d_in_r
                INNER JOIN c_in_d USING (dir)
                INNER JOIN {node_ids} ON ({node_ids}.id=c_in_d.cnt)
                GROUP BY {node_ids}.id
                ",
                node_ids = node_ids
            ))
            .await
            .context("Failed to query c_in_d + d_in_r")?;
        if let Some(row) = row {
            let anchor = self.swhid(row.revrel).await?;
            return Ok(Ok(proto::WhereIsOneResult {
                swhid,
                anchor: Some(anchor.to_string()),
                origin: None,
            }));
        }

        tracing::debug!("Got no result");
        */

        // No result
        Ok(Ok(proto::WhereIsOneResult {
            swhid,
            ..Default::default()
        }))
    }
}
