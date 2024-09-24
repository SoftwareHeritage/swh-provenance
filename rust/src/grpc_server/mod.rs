// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;
use tokio::sync::mpsc;

use anyhow::Result;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use sentry::integrations::anyhow::capture_anyhow;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response};
use tonic_middleware::MiddlewareFor;
use tracing::{instrument, Level};

use crate::database::ProvenanceDatabase;

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

    async fn whereis(&self, swhid: String) -> Result<proto::WhereIsOneResult> {
        todo!()
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
        todo!()
    }

    type WhereAreOneStream = ReceiverStream<Result<proto::WhereIsOneResult, tonic::Status>>;
    #[instrument(skip(self, request), err(level = Level::INFO))]
    async fn where_are_one(
        &self,
        request: Request<proto::WhereAreOneRequest>,
    ) -> TonicResult<Self::WhereAreOneStream> {
        tracing::info!("{:?}", request.get_ref());

        let whereis_service = self.clone(); // Need to clone because we return from this function
                                            // before the work is done
        let (tx, rx) = mpsc::channel(1_000);
        tokio::spawn(async move {
            request
                .into_inner()
                .swhid
                .into_iter()
                .map(|swhid| async {
                    whereis_service.whereis(swhid).await.map_err(|e| {
                        capture_anyhow(&e);
                        tonic::Status::unknown(e.to_string())
                    })
                })
                .collect::<FuturesUnordered<_>>()
                .fold(Ok(()), |prev, result| async {
                    match prev {
                        Err(e) => Err(e),
                        Ok(()) => tx.send(result).await,
                    }
                })
                .await
                .map_err(|_: mpsc::error::SendError<_>| {
                    tracing::debug!("Client disconnected before sending all results")
                })
        });

        Ok(Response::new(ReceiverStream::new(rx)))

        /* TODO: When impl_trait_in_assoc_type is stabilized, we can replace the code above with:
        Ok(Response::new(
            request
                .get_ref()
                .swhid
                .into_iter()
                .map(|swhid| self.whereis(swhid))
                .collect::<FuturesUnordered<_>>(), // Run each request concurrently
        ))*/
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
