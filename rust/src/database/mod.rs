// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Parquet backend for the Provenance service

use std::sync::Arc;

use anyhow::{Context, Result};
use url::Url;

mod caching_parquet_reader;
pub mod metrics;
mod pooled_reader;
mod reader;
mod table;
pub use table::*;
pub mod types;

pub struct ProvenanceDatabase {
    pub node: Table,
    pub c_in_d: Table,
    pub d_in_r: Table,
    pub c_in_r: Table,
}

impl ProvenanceDatabase {
    pub async fn new(base_url: &Url) -> Result<Self> {
        let (store, path) = object_store::parse_url(base_url)
            .with_context(|| format!("Invalid provenance database URL: {}", base_url))?;
        let store = store.into();
        let (node, c_in_d, d_in_r, c_in_r) = futures::join!(
            Table::new(Arc::clone(&store), path.child("nodes"), Some("id")),
            Table::new(
                Arc::clone(&store),
                path.child("contents_in_frontier_directories"),
                Some("cnt"),
            ),
            Table::new(
                Arc::clone(&store),
                path.child("frontier_directories_in_revisions"),
                Some("dir"),
            ),
            Table::new(
                Arc::clone(&store),
                path.child("contents_in_revisions_without_frontiers"),
                Some("cnt"),
            ),
        );

        Ok(Self {
            node: node.context("Could not initialize 'nodes' table")?,
            c_in_d: c_in_d.context("Could not initialize 'c_in_d' table")?,
            d_in_r: d_in_r.context("Could not initialize 'd_in_r' table")?,
            c_in_r: c_in_r.context("Could not initialize 'c_in_r' table")?,
        })
    }
}
