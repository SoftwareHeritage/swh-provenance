// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::io::Read;
use std::path::PathBuf;

use anyhow::{Context, Result};

use swh_graph::graph::SwhGraphWithProperties;
use swh_graph::properties;
use swh_graph::SwhGraphProperties;

use crate::database::ProvenanceDatabase;
use crate::graph::MockSwhGraph;

pub fn load_graph_properties(
    path: PathBuf,
) -> Result<impl SwhGraphWithProperties<Maps: properties::Maps>> {
    let node_count_path = path.with_extension("nodes.count.txt");
    let mut num_nodes = String::new();
    std::fs::File::open(&node_count_path)
        .with_context(|| format!("Could not open {}", node_count_path.display()))?
        .read_to_string(&mut num_nodes)
        .with_context(|| format!("Could not read {}", node_count_path.display()))?;
    let num_nodes = num_nodes.strip_suffix('\n').unwrap_or(&num_nodes);
    let num_nodes = num_nodes.parse().with_context(|| {
        format!(
            "Could not parse content of {} as an integer",
            node_count_path.display()
        )
    })?;
    let properties = SwhGraphProperties::new(path.clone(), num_nodes)
        .load_maps::<swh_graph::mph::DynMphf>()
        .context("Could not load graph maps")?;
    log::info!("Graph loaded");
    Ok(MockSwhGraph {
        path,
        num_nodes,
        properties,
    })
}

pub async fn load_database(
    database_url: url::Url,
    indexes_path: PathBuf,
) -> Result<ProvenanceDatabase> {
    let db = ProvenanceDatabase::new(database_url, &indexes_path)
        .await
        .context("Could not initialize provenance database")?;
    db.mmap_ef_indexes()
        .context("Could not mmap Elias-Fano indexes")?;
    log::info!("Database loaded");
    Ok(db)
}
