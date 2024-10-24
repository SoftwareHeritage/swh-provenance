// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use anyhow::Result;
use futures::stream::FuturesUnordered;
use sentry::integrations::anyhow::capture_anyhow;
use tonic::transport::Server;
use tonic::{Request, Response};
use tonic_middleware::MiddlewareFor;
use tracing::{instrument, Level};

use swh_graph::properties;

use crate::database::ProvenanceDatabase;
use crate::proto;
use crate::proto::provenance_service_server::ProvenanceServiceServer;
use crate::queries::ProvenanceService;
use crate::GraphProperties;

pub type NodeId = u64;

mod metrics;

pub struct ProvenanceServiceWrapper<MAPS: properties::Maps + Sync + Send + 'static>(
    Arc<ProvenanceService<MAPS>>,
);

impl<MAPS: properties::Maps + Sync + Send + 'static> ProvenanceServiceWrapper<MAPS> {
    pub fn new(db: ProvenanceDatabase, graph_properties: GraphProperties<MAPS>) -> Self {
        Self(Arc::new(ProvenanceService {
            db,
            graph_properties,
        }))
    }
}

impl<MAPS: properties::Maps + Sync + Send + 'static> Clone for ProvenanceServiceWrapper<MAPS> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

#[tonic::async_trait]
impl<MAPS: properties::Maps + Sync + Send + 'static>
    proto::provenance_service_server::ProvenanceService for ProvenanceServiceWrapper<MAPS>
{
    #[instrument(skip(self, request), err(level = Level::INFO))]
    async fn where_is_one(
        &self,
        request: Request<proto::WhereIsOneRequest>,
    ) -> TonicResult<proto::WhereIsOneResult> {
        tracing::info!("{:?}", request.get_ref());

        match self.0.where_is_one(request.into_inner().swhid).await {
            Ok(Ok((_scan_init_metrics, _scan_metrics, result))) => Ok(Response::new(result)),
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
                    let whereis_service: ProvenanceServiceWrapper<MAPS> = whereis_service.clone(); // ditto
                    async move {
                        match whereis_service.0.where_is_one(swhid).await {
                            Ok(Ok((_scan_init_metrics, _scan_metrics, result))) => Ok(result),
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

pub async fn serve<MAPS: properties::Maps + Sync + Send + 'static>(
    db: ProvenanceDatabase,
    graph_properties: GraphProperties<MAPS>,
    bind_addr: std::net::SocketAddr,
    statsd_client: cadence::StatsdClient,
) -> Result<(), tonic::transport::Error> {
    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
    health_reporter
        .set_serving::<ProvenanceServiceServer<ProvenanceServiceWrapper<MAPS>>>()
        .await;

    #[cfg(not(feature = "sentry"))]
    let mut builder = Server::builder();
    #[cfg(feature = "sentry")]
    let mut builder =
        Server::builder().layer(::sentry::integrations::tower::NewSentryLayer::new_from_top());
    builder
        .add_service(MiddlewareFor::new(
            ProvenanceServiceServer::new(ProvenanceServiceWrapper::new(db, graph_properties)),
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
