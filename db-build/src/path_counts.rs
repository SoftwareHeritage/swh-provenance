// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::convert::Infallible;

use anyhow::{Context, Result};

use swh_graph::graph::*;
use swh_graph::views::Transposed;

use swh_graph_stdlib::labeling::{DenseLabels, MapReduceBuilder, MapReducer};

use crate::filters::{is_root_revrel, NodeFilter};

/// Counts paths from each node to any revision/release matching a [`NodeFilter`],
/// walking over arcs in the reverse direction (from directories up to revisions)
///
/// Matching revisions/releases get a single trivial path to themselves;
/// other nodes get the sum of the paths of the revisions/releases pointing to them.
pub struct RevrelPathCountingMapReducer<'g, G> {
    /// The non-transposed graph, as [`is_root_revrel`] must follow forward arcs
    graph: &'g G,
    node_filter: NodeFilter,
}

impl<'g, G> RevrelPathCountingMapReducer<'g, G> {
    pub fn new(graph: &'g G, node_filter: NodeFilter) -> Self {
        Self { graph, node_filter }
    }
}

impl<'g, G> MapReducer for RevrelPathCountingMapReducer<'g, G>
where
    G: SwhBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    type Label = f64;
    type Error = Infallible;

    fn map(&mut self, node: NodeId) -> Result<Option<f64>, Infallible> {
        Ok(Some(
            if is_root_revrel(self.graph, self.node_filter, node) {
                1.
            } else {
                0.
            },
        ))
    }

    fn reduce<'a, I: Iterator<Item = &'a f64>>(
        &mut self,
        first_label: f64,
        other_labels: I,
    ) -> Result<Option<f64>, Infallible>
    where
        f64: 'a,
    {
        Ok(Some(first_label + other_labels.copied().sum::<f64>()))
    }
}

/// Counts the number of paths from each node to any revision/release matching `node_filter`,
/// including paths going through intermediate revisions/releases
///
/// Counts may be large enough to overflow long integers, so they are computed
/// with double-precision floating point numbers.
///
/// Paths walk over arcs in the reverse direction, so `toposorted_nodes` must be
/// in forward topological order (ori->snp->rel->rev->dir->cnt), as returned by the toposort
/// command: each node is processed after the revisions/releases pointing to it.
pub fn count_paths_to_revisions<G>(
    graph: &G,
    node_filter: NodeFilter,
    toposorted_nodes: impl Iterator<Item = NodeId>,
) -> Result<DenseLabels<f64>>
where
    G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties + Sync + Send,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut mapreduce = MapReduceBuilder::new_dense(
        Transposed(graph),
        RevrelPathCountingMapReducer::new(graph, node_filter),
    )
    .num_nodes(graph.num_nodes())
    .cheap_clones(true)
    .keep_labels(true)
    .build()
    .context("Could not build MapReduce")?;

    mapreduce
        .run_in_topological_order(toposorted_nodes)
        .unwrap(); // Error type is Infallible

    mapreduce.take_labels().context("Could not take labels")
}
