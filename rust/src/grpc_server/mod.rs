// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use anyhow::{anyhow, ensure, Context, Result};
use ar_row::deserialize::ArRowDeserialize;
use ar_row_derive::ArRowDeserialize;
use futures::stream::FuturesUnordered;
use sentry::integrations::anyhow::capture_anyhow;
use swh_graph::SWHID;
use tonic::transport::Server;
use tonic::{Request, Response};
use tonic_middleware::MiddlewareFor;
use tracing::{instrument, Level};

use crate::database::ProvenanceDatabase;

pub type NodeId = u64;

pub mod proto {
    tonic::include_proto!("swh.provenance");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("swhprovenance_descriptor");
}

use proto::provenance_service_server::ProvenanceServiceServer;

mod metrics;

#[derive(Clone)]
pub struct ProvenanceService {
    db: Arc<ProvenanceDatabase>,
}

impl ProvenanceService {
    pub fn new(db: ProvenanceDatabase) -> Self {
        Self { db: Arc::new(db) }
    }

    #[instrument(skip(self, query))]
    async fn fetch_one<T: ArRowDeserialize>(&self, query: impl AsRef<str>) -> Result<Option<T>> {
        tracing::debug!("fetch_one: {}", query.as_ref());
        let mut batches = self
            .db
            .ctx
            .sql(query.as_ref())
            .await
            .context("Query failed")?
            .collect()
            .await
            .context("Could not get query result")?;
        ensure!(
            batches.len() <= 1,
            "Expected 0 or 1 batch, got {}",
            batches.len()
        );
        if let Some(batch) = batches.pop() {
            let mut rows = T::from_record_batch(batch).context("Could not parse query result")?;
            ensure!(rows.len() == 1, "Expected 1 node, got {}", rows.len());
            return Ok(Some(rows.pop().unwrap()));
        }

        // No results
        Ok(None)
    }

    #[instrument(skip(self))]
    async fn node_id(&self, swhid: &str) -> Result<Result<NodeId, tonic::Status>> {
        tracing::debug!("Getting node id for {}", swhid);
        let Ok(swhid) = swh_graph::SWHID::try_from(swhid) else {
            return Ok(Err(tonic::Status::invalid_argument(format!(
                "{} is not a valid SWHID",
                swhid
            ))));
        };

        // TODO: use the MPH, it's faster than querying the node table
        #[derive(ArRowDeserialize, Clone, Default)]
        struct NodeRow {
            id: u64,
        }

        let row: NodeRow = self
            .fetch_one(format!(
                "
                SELECT id FROM node
                WHERE
                    type = '{}'
                    AND sha1_git = ARROW_CAST(decode('{}', 'base64'), 'FixedSizeBinary(20)')
                LIMIT 1
                ",
                swhid.node_type,
                base64_simd::STANDARD_NO_PAD.encode_to_string(swhid.hash)
            ))
            .await
            .context("Failed to get id from SWHID")?
            .ok_or_else(|| tonic::Status::not_found(format!("Unknown SWHID: {}", swhid)))?;

        Ok(Ok(row.id))
    }

    #[instrument(skip(self))]
    async fn swhid(&self, node_id: NodeId) -> Result<SWHID> {
        tracing::debug!("Getting SWHID from for {}", node_id);
        // TODO: use the MPH, it's faster than querying the node table
        #[derive(ArRowDeserialize, Clone, Default)]
        struct NodeRow {
            r#type: String,
            sha1_git: Box<[u8]>,
        }

        let row: NodeRow = self
            .fetch_one(format!(
                "SELECT type, ARROW_CAST(sha1_git, 'Binary') FROM node WHERE id = {} LIMIT 1",
                node_id,
            ))
            .await
            .context("Failed to get SWHID from id")?
            .with_context(|| format!("Unknown node id: {}", node_id))?;

        Ok(SWHID {
            namespace_version: 1,
            node_type: row
                .r#type
                .parse()
                .map_err(|node_type| anyhow!("Invalid node type in 'node' table: {node_type:?}"))?,
            hash: row
                .sha1_git
                .as_ref()
                .try_into()
                .context("Invalid sha1_git length in 'node' table")?,
        })
    }

    #[instrument(skip(self))]
    async fn whereis(
        &self,
        swhid: String,
    ) -> Result<Result<proto::WhereIsOneResult, tonic::Status>> {
        let node_id = self.node_id(&swhid).await??;

        tracing::debug!("Looking up c_in_r");
        #[derive(ArRowDeserialize, Clone, Default)]
        struct AnchorRow {
            revrel: u64,
        }
        let row: Option<AnchorRow> = self
            .fetch_one(format!(
                "SELECT revrel FROM c_in_r WHERE cnt = {} LIMIT 1",
                node_id
            ))
            .await
            .context("Failed to query c_in_r")?;
        if let Some(row) = row {
            let anchor = self.swhid(row.revrel).await?;
            return Ok(Ok(proto::WhereIsOneResult {
                swhid,
                anchor: Some(anchor.to_string()),
                origin: None,
            }));
        }

        tracing::debug!("Looking up c_in_d + d_in_r");
        let row: Option<AnchorRow> = self
            .fetch_one(format!(
                "SELECT revrel FROM d_in_r INNER JOIN c_in_d USING (dir) WHERE cnt = {} LIMIT 1",
                node_id
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

        // No result
        Ok(Ok(proto::WhereIsOneResult {
            swhid,
            ..Default::default()
        }))
    }
}

#[tonic::async_trait]
impl proto::provenance_service_server::ProvenanceService for ProvenanceService {
    #[instrument(skip(self, request), err(level = Level::INFO))]
    async fn where_is_one(
        &self,
        request: Request<proto::WhereIsOneRequest>,
    ) -> TonicResult<proto::WhereIsOneResult> {
        tracing::info!("{:?}", request.get_ref());

        match self.whereis(request.into_inner().swhid).await {
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
                    let whereis_service = whereis_service.clone(); // ditto
                    async move {
                        match whereis_service.whereis(swhid).await {
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

pub async fn serve(
    db: ProvenanceDatabase,
    bind_addr: std::net::SocketAddr,
    statsd_client: cadence::StatsdClient,
) -> Result<(), tonic::transport::Error> {
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<ProvenanceServiceServer<ProvenanceService>>()
        .await;

    #[cfg(not(feature = "sentry"))]
    let mut builder = Server::builder();
    #[cfg(feature = "sentry")]
    let mut builder =
        Server::builder().layer(::sentry::integrations::tower::NewSentryLayer::new_from_top());
    builder
        .add_service(MiddlewareFor::new(
            ProvenanceServiceServer::new(ProvenanceService::new(db)),
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
