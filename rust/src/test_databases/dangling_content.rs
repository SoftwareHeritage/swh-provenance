// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::create_dir_all;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use dataset_writer::{ParallelDatasetWriter, ParquetTableWriter};
use sux::prelude::BitVec;
use swh_graph::graph::*;
use swh_graph::graph_builder::BuiltGraph;
use swh_graph::swhid;

use swh_provenance_db_build::filters::NodeFilter;
use swh_provenance_db_build::x_in_y_dataset::{
    cnt_in_dir_schema, cnt_in_dir_writer_properties, cnt_in_revrel_schema,
    cnt_in_revrel_writer_properties, dir_in_revrel_schema, dir_in_revrel_writer_properties,
    revrel_in_ori_schema, revrel_in_ori_writer_properties,
};

/// Builds a small graph where one content is in no revision
///
/// ```
/// rev0 -> dir1 -> cnt3
///
/// dir4 -> cnt5
/// ```
pub fn gen_graph() -> BuiltGraph {
    use swh_graph::graph_builder::GraphBuilder;
    use swh_graph::labels::{Permission};
    use swh_graph::swhid;
    let mut builder = GraphBuilder::default();

    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000000))
        .unwrap()
        .message(b"Initial commit".to_vec())
        .author(b"0".to_vec())
        .author_timestamp(1111122220, 120)
        .committer(b"0".to_vec())
        .committer_timestamp(1111122220, 120)
        .done();
    builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000001))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000002))
        .unwrap()
        .is_skipped_content(false)
        .content_length(1337)
        .done();
    builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000003))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000004))
        .unwrap()
        .is_skipped_content(false)
        .content_length(1337)
        .done();
    builder.arc(0, 1);
    builder.dir_arc(1, 2, Permission::Content, b"README.md".to_vec());
    builder.dir_arc(3, 4, Permission::Content, b"parser.c".to_vec());
    builder.done().expect("Could not build graph")
}

pub fn gen_database(path: PathBuf) -> Result<()> {
    let graph = gen_graph();

    // Build a placedholder for max_leaf_timestamps.bin, which normally contains
    // {dir: max(min(timestamp(rev) for rev in ancestors(cnt)) for cnt in descendants(dir))},
    let max_timestamps = [
        i64::MIN,
        1, // swh:1:dir:0000000000000000000000000000000000000001
        i64::MIN,
        i64::MIN, // swh:1:dir:0000000000000000000000000000000000000003
        i64::MIN,
    ];

    // Build set of frontier directories, which would be stored in frontier_directories/*.parquet
    // in the real pipeline
    let mut frontier_directories = BitVec::new(graph.num_nodes());
    for swhid in [
        swhid!(swh:1:dir:0000000000000000000000000000000000000001),
    ] {
        let node_id = graph.properties().node_id(swhid).expect("unknown SWHID");
        frontier_directories.set(node_id, true);
    }

    // contents-in-revisions
    let c_in_r = path.join("contents_in_revisions_without_frontiers");
    let c_in_r_schema = (
        Arc::new(cnt_in_revrel_schema()),
        cnt_in_revrel_writer_properties(&graph).build(),
    );
    create_dir_all(&c_in_r).with_context(|| format!("Could not create {}", c_in_r.display()))?;
    let writer = ParallelDatasetWriter::<ParquetTableWriter<_>>::with_schema(c_in_r, c_in_r_schema)
        .context("Could not create contents_in_revisions_without_frontiers writer")?;
    swh_provenance_db_build::contents_in_revisions::write_revisions_from_contents(
        &graph,
        NodeFilter::All,
        None, // reachable nodes
        &frontier_directories,
        writer,
    )
    .context("Could not generate contents_in_revisions_without_frontiers")?;

    // contents-in-directories
    let c_in_d = path.join("contents_in_frontier_directories");
    let c_in_d_schema = (
        Arc::new(cnt_in_dir_schema()),
        cnt_in_dir_writer_properties(&graph).build(),
    );
    create_dir_all(&c_in_d).with_context(|| format!("Could not create {}", c_in_d.display()))?;
    let writer = ParallelDatasetWriter::<ParquetTableWriter<_>>::with_schema(c_in_d, c_in_d_schema)
        .context("Could not create contents_in_frontier_directories writer")?;
    swh_provenance_db_build::contents_in_directories::write_directories_from_contents(
        &graph,
        &frontier_directories,
        writer,
    )
    .context("Could not generate contents_in_frontier_directories")?;

    // directories-in-revisions
    let d_in_r = path.join("frontier_directories_in_revisions");
    let d_in_r_schema = (
        Arc::new(dir_in_revrel_schema()),
        dir_in_revrel_writer_properties(&graph).build(),
    );
    create_dir_all(&d_in_r).with_context(|| format!("Could not create {}", d_in_r.display()))?;
    let writer = ParallelDatasetWriter::<ParquetTableWriter<_>>::with_schema(d_in_r, d_in_r_schema)
        .context("Could not create frontier_directories_in_revisions writer")?;
    swh_provenance_db_build::directories_in_revisions::write_revisions_from_frontier_directories(
        &graph,
        &max_timestamps[..],
        NodeFilter::All,
        None, // reachable nodes
        &frontier_directories,
        writer,
    )
    .context("Could not generate frontier_directories_in_revisions")?;

    // revisions-in-origins
    let r_in_o = path.join("revisions_in_origins");
    let r_in_o_schema = (
        Arc::new(revrel_in_ori_schema()),
        revrel_in_ori_writer_properties(&graph).build(),
    );
    create_dir_all(&r_in_o).with_context(|| format!("Could not create {}", r_in_o.display()))?;
    let writer = ParallelDatasetWriter::<ParquetTableWriter<_>>::with_schema(r_in_o, r_in_o_schema)
        .context("Could not create revisions_in_origins writer")?;
    swh_provenance_db_build::revisions_in_origins::main(&graph, NodeFilter::All, writer)
        .context("Could not generate frontier_directories_in_revisions")?;

    let graph_path = path.join("graph.json");
    let file = std::fs::File::create(&graph_path)
        .with_context(|| format!("Could not create {}", graph_path.display()))?;
    let mut serializer = serde_json::Serializer::new(BufWriter::new(file));
    swh_graph::serde::serialize_with_labels_and_maps(&mut serializer, &graph)
        .with_context(|| format!("Could not serialize to {}", graph_path.display()))?;

    Ok(())
}
