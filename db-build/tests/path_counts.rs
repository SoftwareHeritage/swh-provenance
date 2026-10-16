// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::HashMap;

use anyhow::Result;
use pretty_assertions::assert_eq;

use swh_graph::graph::NodeId;
use swh_graph::graph_builder::GraphBuilder;
use swh_graph::swhid;

use swh_graph_stdlib::labeling::Labels;

use swh_provenance_db_build::filters::NodeFilter;
use swh_provenance_db_build::path_counts::count_paths_to_revisions;

macro_rules! map {
    ($($k:expr => $v:expr),* $(,)?) => {{
        vec![$(($k, $v),)*].into_iter().collect::<HashMap<_, _>>()
    }};
}

fn count_paths(
    graph: &swh_graph::graph_builder::BuiltGraph,
    node_filter: NodeFilter,
    nodes: &[NodeId],
) -> Result<HashMap<NodeId, f64>> {
    let labels = count_paths_to_revisions(graph, node_filter, nodes.iter().copied())?;
    Ok(nodes
        .iter()
        .map(|&node| (node, labels.get(node).copied().unwrap_or(f64::NAN)))
        .collect())
}

#[test]
fn test_paths_to_revisions() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let snp0 = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000000))?
        .done();
    let rev1 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000001))?
        .done();
    let rev2 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000002))?
        .done();
    let rel3 = builder
        .node(swhid!(swh:1:rel:0000000000000000000000000000000000000003))?
        .done();
    let rev4 = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000004))?
        .done();
    let dir5 = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000005))?
        .done();
    let dir6 = builder
        .node(swhid!(swh:1:dir:0000000000000000000000000000000000000006))?
        .done();

    /*
     * snp0 --> rev1 --> rev4 --+
     *    \           \        +--> dir5
     *     \           +--------------> dir6
     *      \
     *       +--> rel3 --> rev2 ------+
     *
     * rev4 is the parent of rev1, and both rev4 and rev2 host dir5
     */
    builder.arc(snp0, rev1);
    builder.arc(snp0, rel3);
    builder.arc(rel3, rev2);
    builder.arc(rev1, rev4);
    builder.arc(rev4, dir5);
    builder.arc(rev2, dir5);
    builder.arc(rev1, dir6);

    let graph = builder.done()?;

    // Forward topological order (ori->snp->rel->rev->dir->cnt)
    let toposorted_nodes = [snp0, rel3, rev1, rev2, rev4, dir6, dir5];

    let heads = count_paths(&graph, NodeFilter::Heads, &toposorted_nodes)?;
    let all = count_paths(&graph, NodeFilter::All, &toposorted_nodes)?;

    assert_eq!(
        heads,
        map!(
            snp0 => 0.,
            // rel3 and rev2 are heads; dir5 gets rev2 + rev2->rel3 + rev4->rev1
            rel3 => 1.,
            rev1 => 1.,
            rev2 => 2.,
            rev4 => 1., // only through its head child rev1
            dir6 => 1.,
            dir5 => 3.,
        ),
    );

    assert_eq!(
        all,
        map!(
            snp0 => 0.,
            rel3 => 1.,
            rev1 => 1.,
            rev2 => 2.,
            rev4 => 2., // itself, and its child rev1
            dir6 => 1.,
            dir5 => 4., // rev4 + rev4->rev1 + rev2 + rev2->rel3
        ),
    );

    Ok(())
}
