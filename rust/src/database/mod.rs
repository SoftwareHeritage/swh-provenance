// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Parquet backend for the Provenance service

use std::hash::Hash;
use std::sync::Arc;

use anyhow::{bail, ensure, Context, Result};
use object_store::path::Path;
use url::Url;

mod pooled_reader;
mod reader;
mod table;
pub use table::*;

pub trait IndexKey: parquet::data_type::AsBytes + Hash + Eq + Clone {
    /// Returns whether the key may be in the column chunk based on its statistics
    fn check_column_chunk(
        &self,
        column_chunk_statistics: &parquet::file::statistics::Statistics,
    ) -> bool;
    /// Given a page index, returns page ids within the index that may contain this key, as a
    /// boolean array.
    ///
    /// Returns `None` when it cannot prune (ie. when all rows would be selected)
    fn check_page_index(
        &self,
        index: &parquet::file::page_index::index::Index,
    ) -> Result<Option<arrow::array::BooleanArray>>;
}

#[derive(Hash, PartialEq, Eq, Clone, Debug)]
pub struct Sha1Git(pub [u8; 20]);
impl parquet::data_type::AsBytes for Sha1Git {
    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
impl IndexKey for Sha1Git {
    fn check_column_chunk(
        &self,
        column_chunk_statistics: &parquet::file::statistics::Statistics,
    ) -> bool {
        // Should we even bother implementing this? Assuming a random distribution of SWHIDs among
        // row groups, and the default row group size, it's very unlikely we can prune a row group
        // based on statistics.
        true
    }
    fn check_page_index(
        &self,
        index: &parquet::file::page_index::index::Index,
    ) -> Result<Option<arrow::array::BooleanArray>> {
        use parquet::file::page_index::index::Index::*;
        use parquet::file::page_index::index::*;

        match index {
            NONE => {
                // No page index, we can't use it to prune
                Ok(None)
            }
            FIXED_LEN_BYTE_ARRAY(NativeIndex { indexes, .. }) => {
                let mut matches = arrow::array::builder::BooleanBufferBuilder::new(indexes.len());
                for PageIndex { min, max, .. } in indexes {
                    if let Some(min) = min {
                        ensure!(
                            min.len() == 20,
                            "Unexpected length of sha1_git value: {}",
                            min.len()
                        );
                        if &self.0[..] < min.data() {
                            matches.append(false);
                            continue;
                        }
                    }
                    if let Some(max) = max {
                        ensure!(
                            max.len() == 20,
                            "Unexpected length of sha1_git value: {}",
                            max.len()
                        );
                        if &self.0[..] > max.data() {
                            matches.append(false);
                            continue;
                        }
                    }
                    matches.append(true);
                }
                Ok(Some(matches.finish().into()))
            }
            _ => bail!("Unsupported page index type for Sha1Git: {index:?}"),
        }
    }
}

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
            Table::new(Arc::clone(&store), path.child("nodes")),
            Table::new(
                Arc::clone(&store),
                path.child("contents_in_frontier_directories")
            ),
            Table::new(
                Arc::clone(&store),
                path.child("frontier_directories_in_revisions")
            ),
            Table::new(
                Arc::clone(&store),
                path.child("contents_in_revisions_without_frontiers")
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
