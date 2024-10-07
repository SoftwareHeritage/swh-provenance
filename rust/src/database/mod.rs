// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

/// Parquet backend for the Provenance service
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use url::Url;

mod pooled_reader;
mod reader;
mod table;
pub use table::*;

pub struct ProvenanceDatabase {
    pub node: Table,
    pub c_in_d: Table,
    pub d_in_r: Table,
    pub c_in_r: Table,
}

impl ProvenanceDatabase {
    pub async fn new(base_url: &Url) -> Result<Self> {
        let (store, path) = object_store::parse_url(base_url);
        let (node, c_in_d, d_in_r, c_in_r) = join!(
            Table::new(store, Path::from([path, "nodes"])),
            Table::new(
                store,
                Path::from([path, "contents_in_frontier_directories"])
            ),
            Table::new(
                store,
                Path::from([path, "frontier_directories_in_revisions"])
            ),
            Table::new(
                store,
                Path::from([path, "contents_in_revisions_without_frontiers"])
            ),
        );

        Ok(Self {
            node: node?,
            c_in_d: c_in_d?,
            d_in_r: d_in_r?,
            c_in_r: c_in_r?,
        })
    }
}
