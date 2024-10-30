// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::create_dir_all;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use dataset_writer::{
    ParallelDatasetWriter, ParquetTableWriter, ParquetTableWriterConfig, TableWriter,
};
use sux::prelude::BitVec;
use swh_graph::graph::*;
use swh_graph::graph_builder::BuiltGraph;
use swh_graph::swhid;

use swh_graph_provenance::filters::NodeFilter;
use swh_graph_provenance::x_in_y_dataset::{
    cnt_in_dir_schema, cnt_in_dir_writer_properties, cnt_in_revrel_schema,
    cnt_in_revrel_writer_properties, dir_in_revrel_schema, dir_in_revrel_writer_properties,
    CntInDirTableBuilder, DirInRevrelTableBuilder,
};

pub fn gen_graph() -> BuiltGraph {
    // Same example graph as swh-graph. The following code was generated with `cargo run
    // --all-features --bin swh-graph-convert -- -i swh/graph/example_dataset/compressed/example -o
    // /dev/stdout --output-format graph-builder`
    use swh_graph::graph_builder::GraphBuilder;
    use swh_graph::labels::{Permission, VisitStatus};
    use swh_graph::swhid;
    let mut builder = GraphBuilder::default();

    builder
        .node(swhid!(swh:1:ori:8f50d3f60eae370ddbf85c86219c55108a350165))
        .unwrap()
        .message(b"https://example.com/swh/graph2".to_vec())
        .done();
    builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000022))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:ori:83404f995118bd25774f4ac14422a8f175e7a054))
        .unwrap()
        .message(b"https://example.com/swh/graph".to_vec())
        .done();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000009))
        .unwrap()
        .message(b"Add parser".to_vec())
        .author(b"2".to_vec())
        .author_timestamp(1111144440, 120)
        .committer(b"2".to_vec())
        .committer_timestamp(1111155550, 120)
        .done();
    builder
        .node(swhid!(swh:1:rel:0000000000000000000000000000000000000010))
        .unwrap()
        .message(b"Version 1.0".to_vec())
        .tag_name(b"v1.0".to_vec())
        .author(b"0".to_vec())
        .author_timestamp(1234567890, 120)
        .done();
    builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000020))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000003))
        .unwrap()
        .message(b"Initial commit".to_vec())
        .author(b"0".to_vec())
        .author_timestamp(1111122220, 120)
        .committer(b"0".to_vec())
        .committer_timestamp(1111122220, 120)
        .done();
    builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000002))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000005))
        .unwrap()
        .is_skipped_content(false)
        .content_length(1337)
        .done();
    builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000006))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000004))
        .unwrap()
        .is_skipped_content(false)
        .content_length(404)
        .done();
    builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000001))
        .unwrap()
        .is_skipped_content(false)
        .content_length(42)
        .done();
    builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000008))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000007))
        .unwrap()
        .is_skipped_content(false)
        .content_length(666)
        .done();
    builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000012))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000011))
        .unwrap()
        .is_skipped_content(false)
        .content_length(313)
        .done();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000013))
        .unwrap()
        .message(b"Add tests".to_vec())
        .author(b"0".to_vec())
        .author_timestamp(1111166660, 120)
        .committer(b"2".to_vec())
        .committer_timestamp(1111166660, 120)
        .done();
    builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000016))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000015))
        .unwrap()
        .is_skipped_content(true)
        .content_length(404)
        .done();
    builder
        .node(swhid!(swh:1:rel:0000000000000000000000000000000000000021))
        .unwrap()
        .message(b"Version 2.0 but with no author".to_vec())
        .tag_name(b"v2.0-anonymous".to_vec())
        .done();
    builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000018))
        .unwrap()
        .message(b"Refactor codebase".to_vec())
        .author(b"1".to_vec())
        .author_timestamp(1111177770, 0)
        .committer(b"0".to_vec())
        .committer_timestamp(1111177770, 0)
        .done();
    builder
        .node(swhid!(swh:1:rel:0000000000000000000000000000000000000019))
        .unwrap()
        .message(b"Version 2.0".to_vec())
        .tag_name(b"v2.0".to_vec())
        .author(b"2".to_vec())
        .done();
    builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000017))
        .unwrap()
        .done();
    builder
        .node(swhid!(swh:1:cnt:0000000000000000000000000000000000000014))
        .unwrap()
        .is_skipped_content(false)
        .content_length(14)
        .done();

    builder.ori_arc(0, 1, VisitStatus::Full, 1367900441);
    builder.snp_arc(1, 3, b"refs/heads/master".to_vec());
    builder.snp_arc(1, 4, b"refs/tags/v1.0".to_vec());
    builder.snp_arc(1, 19, b"refs/tags/v2.0-anonymous".to_vec());
    builder.ori_arc(2, 5, VisitStatus::Full, 1367900441);
    builder.arc(3, 6);
    builder.arc(3, 12);
    builder.arc(4, 3);
    builder.snp_arc(5, 3, b"refs/heads/master".to_vec());
    builder.snp_arc(5, 4, b"refs/tags/v1.0".to_vec());
    builder.arc(6, 7);
    builder.dir_arc(7, 11, Permission::Content, b"README.md".to_vec());
    builder.dir_arc(9, 8, Permission::Content, b"parser.c".to_vec());
    builder.dir_arc(9, 10, Permission::Content, b"README.md".to_vec());
    builder.dir_arc(12, 9, Permission::ExecutableContent, b"tests".to_vec());
    builder.dir_arc(12, 11, Permission::Content, b"README.md".to_vec());
    builder.dir_arc(12, 13, Permission::Content, b"parser.c".to_vec());
    builder.dir_arc(
        14,
        12,
        Permission::ExecutableContent,
        b"oldproject".to_vec(),
    );
    builder.dir_arc(14, 15, Permission::Content, b"README.md".to_vec());
    builder.arc(16, 3);
    builder.arc(16, 14);
    builder.dir_arc(17, 18, Permission::Content, b"TODO.txt".to_vec());
    builder.arc(19, 20);
    builder.arc(20, 16);
    builder.arc(20, 22);
    builder.arc(21, 20);
    builder.dir_arc(22, 17, Permission::ExecutableContent, b"old".to_vec());
    builder.dir_arc(22, 23, Permission::Content, b"TODO.txt".to_vec());
    builder.done().expect("Could not build graph")
}

pub fn gen_database(path: PathBuf) -> Result<()> {
    let graph = gen_graph();

    let mut frontier_directories = BitVec::new(graph.num_nodes());
    for swhid in [
        swhid!(swh:1:dir:0000000000000000000000000000000000000006),
        swhid!(swh:1:dir:0000000000000000000000000000000000000008),
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
    swh_graph_provenance::contents_in_revisions::write_revisions_from_contents(
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
    let writer = ParquetTableWriter::<CntInDirTableBuilder>::new(
        c_in_d.join("0.parquet"),
        c_in_d_schema,
        ParquetTableWriterConfig::default(),
    )
    .context("Could not create contents_in_frontier_directories writer")?;
    writer
        .close()
        .context("Could not close contents_in_frontier_directories writer")?;

    // directories-in-revisions
    let d_in_r = path.join("frontier_directories_in_revisions");
    let d_in_r_schema = (
        Arc::new(dir_in_revrel_schema()),
        dir_in_revrel_writer_properties(&graph).build(),
    );
    create_dir_all(&d_in_r).with_context(|| format!("Could not create {}", d_in_r.display()))?;
    let writer = ParquetTableWriter::<DirInRevrelTableBuilder>::new(
        d_in_r.join("0.parquet"),
        d_in_r_schema,
        ParquetTableWriterConfig::default(),
    )
    .context("Could not create frontier_directories_in_revisions writer")?;
    writer
        .close()
        .context("Could not close frontier_directories_in_revisions writer")?;

    let graph_path = path.join("graph.json");
    let file = std::fs::File::create(&graph_path)
        .with_context(|| format!("Could not create {}", graph_path.display()))?;
    let mut serializer = serde_json::Serializer::new(BufWriter::new(file));
    swh_graph::serde::serialize_with_labels_and_maps(&mut serializer, &graph)
        .with_context(|| format!("Could not serialize to {}", graph_path.display()))?;

    Ok(())
}
