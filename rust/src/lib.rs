// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

#![doc = include_str!("../README.md")]

use swh_graph::properties;
use swh_graph::SwhGraphProperties;

pub mod database;
#[cfg(feature = "grpc-server")]
pub mod grpc_server;
pub mod queries;
pub mod sentry;
pub mod statsd;

pub mod proto {
    tonic::include_proto!("swh.provenance");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("swhprovenance_descriptor");
}

type GraphProperties<MAPS> = SwhGraphProperties<
    MAPS,
    properties::NoTimestamps,
    properties::NoPersons,
    properties::NoContents,
    properties::NoStrings,
    properties::NoLabelNames,
>;
