// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Parquet backend for the Provenance service

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use parquet_aramid::Table;
use url::Url;

pub(crate) mod types;

pub struct ProvenanceDatabase {
    pub url: Url,
    pub node: Table,
    pub c_in_d: Table,
    pub d_in_r: Table,
    pub c_in_r: Table,
}

impl ProvenanceDatabase {
    pub async fn new(base_url: Url, base_ef_indexes_path: &Path) -> Result<Self> {
        let (store, path) = object_store::parse_url(&base_url)
            .with_context(|| format!("Invalid provenance database URL: {}", base_url))?;
        let store = store.into();
        let (node, c_in_d, d_in_r, c_in_r) = futures::join!(
            Table::new(
                Arc::clone(&store),
                path.child("nodes"),
                base_ef_indexes_path.join("nodes"),
            ),
            Table::new(
                Arc::clone(&store),
                path.child("contents_in_frontier_directories"),
                base_ef_indexes_path.join("contents_in_frontier_directories"),
            ),
            Table::new(
                Arc::clone(&store),
                path.child("frontier_directories_in_revisions"),
                base_ef_indexes_path.join("frontier_directories_in_revisions"),
            ),
            Table::new(
                Arc::clone(&store),
                path.child("contents_in_revisions_without_frontiers"),
                base_ef_indexes_path.join("contents_in_revisions_without_frontiers"),
            ),
        );

        Ok(Self {
            url: base_url,
            node: node.context("Could not initialize 'nodes' table")?,
            c_in_d: c_in_d.context("Could not initialize 'c_in_d' table")?,
            d_in_r: d_in_r.context("Could not initialize 'd_in_r' table")?,
            c_in_r: c_in_r.context("Could not initialize 'c_in_r' table")?,
        })
    }

    pub fn mmap_ef_indexes(&self) -> Result<()> {
        std::thread::scope(|s| {
            let node = std::thread::Builder::new()
                .name("load_index_node".to_string())
                .spawn_scoped(s, || self.node.mmap_ef_index("id"))
                .expect("could not spawn load_index_node");
            let c_in_d = std::thread::Builder::new()
                .name("load_index_c_in_d".to_string())
                .spawn_scoped(s, || self.c_in_d.mmap_ef_index("cnt"))
                .expect("could not spawn load_index_c_in_d");
            let d_in_r = std::thread::Builder::new()
                .name("load_index_d_in_r".to_string())
                .spawn_scoped(s, || self.d_in_r.mmap_ef_index("dir"))
                .expect("could not spawn load_index_d_in_r");
            let c_in_r = std::thread::Builder::new()
                .name("load_index_c_in_r".to_string())
                .spawn_scoped(s, || self.c_in_r.mmap_ef_index("cnt"))
                .expect("could not spawn load_index_c_in_r");

            node.join()
                .expect("could not join node")
                .context("Could not mmap index for 'nodes' table")?;
            c_in_d
                .join()
                .expect("could not join c_in_d")
                .context("Could not mmap index for 'contents_in_frontier_directories' table")?;
            d_in_r
                .join()
                .expect("could not join d_in_r")
                .context("Could not mmap index for 'frontier_directories_in_revisions' table")?;
            c_in_r.join().expect("could not join c_in_r").context(
                "Could not mmap index for 'contents_in_revisions_without_frontiers' table",
            )?;
            Ok(())
        })
    }
}
