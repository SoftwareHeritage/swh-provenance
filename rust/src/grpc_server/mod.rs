// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, ensure, Context, Result};
use ar_row::deserialize::ArRowDeserialize;
use ar_row_derive::ArRowDeserialize;
use arrow::array::*;
use arrow::datatypes::*;
use futures::stream::FuturesUnordered;
use itertools::Itertools;
use sentry::integrations::anyhow::capture_anyhow;
use swh_graph::SWHID;
use tonic::transport::Server;
use tonic::{Request, Response};
use tonic_middleware::MiddlewareFor;
use tracing::{instrument, span_enabled, Level};

use swh_graph::graph::SwhGraphWithProperties;
use swh_graph::properties;
use swh_graph::properties::NodeIdFromSwhidError;

use crate::database::{ProvenanceDatabase, TemporaryTable, Transaction};

pub type NodeId = u64;

pub mod proto {
    tonic::include_proto!("swh.provenance");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("swhprovenance_descriptor");
}

use proto::provenance_service_server::ProvenanceServiceServer;

mod metrics;

struct ProvenanceServiceInner<G: SwhGraphWithProperties + Send + Sync + 'static>
where
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    db: ProvenanceDatabase,
    graph: Option<G>,
}

impl<G: SwhGraphWithProperties + Send + Sync + 'static> ProvenanceServiceInner<G>
where
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    #[instrument(skip(self, transaction), fields(swhids=swhids.iter().map(AsRef::as_ref).join(", ")))]
    async fn node_id<'a>(
        &self,
        transaction: &Transaction<'a>,
        swhids: &[impl AsRef<str>],
    ) -> Result<Result<TemporaryTable<'a>, tonic::Status>> {
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

                // Insert node ids in a new temporary table
                Ok(Ok(transaction
                    .create_table_from_batch(
                        "query_node",
                        RecordBatch::try_new(
                            Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)])),
                            vec![Arc::new(UInt64Array::from(node_ids))],
                        )
                        .expect("Could not build query_node RecordBatch"),
                    )
                    .await
                    .expect("Could not create query_node table")))
            }
            None => {
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
                let sha1_gits: Vec<_> = parsed_swhids.iter().map(|swhid| &swhid.hash).collect();

                // Insert SWHIDs into a temporary table
                let query_swhids = transaction
                    .create_table_from_batch(
                        "query_swhid",
                        RecordBatch::try_new(
                            Arc::new(Schema::new(vec![
                                Field::new("swhid_id", DataType::UInt64, false), // used to find
                                // unknown SWHIDs
                                Field::new("type", DataType::Utf8, false),
                                Field::new("sha1_git", DataType::FixedSizeBinary(20), false),
                            ])),
                            vec![
                                Arc::new(UInt64Array::from(Vec::from_iter(
                                    0..(swhids.len() as u64),
                                ))),
                                Arc::new(StringArray::from(node_types)),
                                Arc::new(FixedSizeBinaryArray::from(sha1_gits)),
                            ],
                        )
                        .expect("Could not create query_swhid RecordBatch"),
                    )
                    .await
                    .context("Could not create query_swhid table")?;

                // Convert from SWHID to node id using a JOIN, into a new temporary table
                let query_nodes = transaction.create_table_from_query(
                    "query_node",
                    &format!(
                        "
                        SELECT id, swhid_id
                        FROM node
                        RIGHT JOIN '{query_swhids}'
                            ON (node.type={query_swhids}.type AND node.sha1_git={query_swhids}.sha1_git)
                        ",
                        query_swhids=query_swhids,
                    )
                )
                .await
                .context("Failed to get id from SWHID")?;

                let mut unknown_swhids = Vec::new();
                for batch in transaction
                    .db()
                    .ctx
                    .sql(&format!(
                        "SELECT swhid_id FROM '{query_nodes}' WHERE id IS NULL",
                        query_nodes = query_nodes
                    ))
                    .await
                    .context("Could not check unknown SWHIDs")?
                    .collect()
                    .await?
                {
                    assert_eq!(
                        **batch.schema_ref(),
                        Schema::new(vec![Field::new("swhid_id", DataType::UInt64, false)])
                    );
                    for swhid_id in batch.column(0).as_primitive::<UInt64Type>() {
                        unknown_swhids.push(swhids.get(swhid_id.unwrap() as usize).unwrap());
                    }
                }

                if !unknown_swhids.is_empty() {
                    return Ok(Err(tonic::Status::not_found(format!(
                        "Unknown SWHIDs: {}",
                        unknown_swhids.into_iter().map(AsRef::as_ref).join(", ")
                    ))));
                }

                Ok(Ok(query_nodes))
            }
        }
    }

    #[instrument(skip(self, transaction, node_ids))]
    async fn swhid(
        &self,
        transaction: &Transaction<'_>,
        node_ids: TemporaryTable<'_>,
    ) -> Result<Vec<SWHID>> {
        tracing::debug!("Getting SWHIDs from node ids");
        match &self.graph {
            Some(graph) => {
                // Convert from node id to SWHID using the graph
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
                    .collect())
            }
            None => {
                // Convert from node id to SWHID using a JOIN
                let mut swhids = Vec::new();

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
                    for (node_type_key, sha1_git) in std::iter::zip(node_type_keys, sha1_gits) {
                        swhids.push(SWHID {
                            namespace_version: 1,
                            node_type: *node_type_values
                                .get(node_type_key)
                                .expect("Could not find Arrow dictionary key in values array"),
                            // .expect() won't panic because we checked the size in the schema
                            hash: sha1_git.try_into().expect("Unexpected sha1_git size"),
                        });
                    }
                }
                Ok(swhids)
            }
        }
    }

    #[instrument(skip(self))]
    async fn whereis(
        &self,
        swhid: String,
    ) -> Result<Result<proto::WhereIsOneResult, tonic::Status>> {
        let transaction = self.db.transaction();

        let node_ids = self.node_id(&transaction, &[&swhid]).await??;

        if span_enabled!(Level::TRACE) {
            tracing::trace!(
                "Query node ids: {:?}",
                transaction
                    .db()
                    .ctx
                    .sql(&format!("SELECT id FROM '{node_ids}'", node_ids = node_ids))
                    .await?
                    .collect()
                    .await?
            )
        }

        tracing::debug!("Looking up c_in_r");
        #[derive(ArRowDeserialize, Clone, Default)]
        struct AnchorRow {
            revrel: u64,
        }
        let revrel = transaction
            .create_table_from_query(
                "anchors",
                &format!(
                    "
                    SELECT revrel AS id
                    FROM {node_ids},  c_in_r
                    WHERE {node_ids}.id=c_in_r.cnt
                    LIMIT 1
                    ",
                    node_ids = node_ids
                ),
            )
            .await
            .context("Failed to query c_in_r")?;
        if span_enabled!(Level::TRACE) {
            tracing::trace!(
                "Anchor node ids: {:?}",
                transaction
                    .db()
                    .ctx
                    .sql(&format!("SELECT id FROM '{revrel}'", revrel = revrel))
                    .await?
                    .collect()
                    .await?
            )
        }
        let mut anchors = self.swhid(&transaction, revrel).await?;
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

pub struct ProvenanceService<G: SwhGraphWithProperties + Send + Sync + 'static>(
    Arc<ProvenanceServiceInner<G>>,
)
where
    <G as SwhGraphWithProperties>::Maps: properties::Maps;

impl<G: SwhGraphWithProperties + Send + Sync + 'static> ProvenanceService<G>
where
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    pub fn new(db: ProvenanceDatabase, graph: Option<G>) -> Self {
        Self(Arc::new(ProvenanceServiceInner { db, graph }))
    }
}

impl<G: SwhGraphWithProperties + Send + Sync + 'static> Clone for ProvenanceService<G>
where
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

#[tonic::async_trait]
impl<G: SwhGraphWithProperties + Send + Sync + 'static>
    proto::provenance_service_server::ProvenanceService for ProvenanceService<G>
where
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    #[instrument(skip(self, request), err(level = Level::INFO))]
    async fn where_is_one(
        &self,
        request: Request<proto::WhereIsOneRequest>,
    ) -> TonicResult<proto::WhereIsOneResult> {
        tracing::info!("{:?}", request.get_ref());

        match self.0.whereis(request.into_inner().swhid).await {
            Ok(Ok(result)) => Ok(Response::new(result)),
            Ok(Err(e)) => Err(e), // client error
            Err(e) => {
                // server error
                tracing::error!("{:?}", e);
                capture_anyhow(&e); // redundant with tracing::error!
                Err(tonic::Status::internal(e.to_string()))
            }
        }
    }

    // TODO: When impl_trait_in_assoc_type is stabilized, replace this with:
    // type WhereAreOneStream = FuturesUnordered<impl Future<Output = Result<proto::WhereIsOneResult, tonic::Status>>;
    // to avoid the dynamic dispatch
    type WhereAreOneStream = Box<
        dyn futures::Stream<Item = Result<proto::WhereIsOneResult, tonic::Status>> + Unpin + Send,
    >;
    #[instrument(skip(self, request), err(level = Level::INFO))]
    async fn where_are_one(
        &self,
        request: Request<proto::WhereAreOneRequest>,
    ) -> TonicResult<Self::WhereAreOneStream> {
        tracing::info!("{:?}", request.get_ref());

        let whereis_service = self.clone(); // Need to clone because we return from this function
                                            // before the work is done
        Ok(Response::new(Box::new(
            request
                .into_inner()
                .swhid
                .into_iter()
                .map(move |swhid| {
                    let whereis_service: ProvenanceService<G> = whereis_service.clone(); // ditto
                    async move {
                        match whereis_service.0.whereis(swhid).await {
                            Ok(Ok(result)) => Ok(result),
                            Ok(Err(e)) => Err(e), // client error
                            Err(e) => {
                                // server error
                                tracing::error!("{:?}", e);
                                capture_anyhow(&e); // redundant with tracing::error!
                                Err(tonic::Status::internal(e.to_string()))
                            }
                        }
                    }
                })
                .collect::<FuturesUnordered<_>>(), // Run each request concurrently
        )))
    }
}

type TonicResult<T> = Result<tonic::Response<T>, tonic::Status>;

pub async fn serve<G: SwhGraphWithProperties + Send + Sync + 'static>(
    db: ProvenanceDatabase,
    graph: Option<G>,
    bind_addr: std::net::SocketAddr,
    statsd_client: cadence::StatsdClient,
) -> Result<(), tonic::transport::Error>
where
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<ProvenanceServiceServer<ProvenanceService<G>>>()
        .await;

    #[cfg(not(feature = "sentry"))]
    let mut builder = Server::builder();
    #[cfg(feature = "sentry")]
    let mut builder =
        Server::builder().layer(::sentry::integrations::tower::NewSentryLayer::new_from_top());
    builder
        .add_service(MiddlewareFor::new(
            ProvenanceServiceServer::new(ProvenanceService::new(db, graph)),
            metrics::MetricsMiddleware::new(statsd_client),
        ))
        .add_service(health_service)
        .add_service(
            tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
                .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
                .build_v1()
                .expect("Could not load v1 reflection service"),
        )
        .add_service(
            tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
                .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
                .build_v1alpha()
                .expect("Could not load v1alpha reflection service"),
        )
        .serve(bind_addr)
        .await?;

    Ok(())
}
