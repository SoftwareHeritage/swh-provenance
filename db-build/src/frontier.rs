// Copyright (C) 2024-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::{bail, Result};
//use std::collections::HashSet;
use rapidhash::RapidHashSet;
use smallvec::SmallVec;
use sux::prelude::BitVec;
use sux::traits::{BitVecOps, BitVecOpsMut};
//use swh_graph_stdlib::collections::{AdaptiveNodeSet, NodeSet, ReadNodeSet};

use swh_graph::graph::*;
use swh_graph::labels::LabelNameId;
use swh_graph::NodeType;
use swh_graph_stdlib::collections::PathStack;

enum Chunk {
    Empty,
    VerySparse(SmallVec<[u32; 8]>), // fits in the same space as HashSet<u16>
    Sparse(RapidHashSet<u32>),
    Dense(BitVec<Box<[usize]>>),
}

impl Chunk {
    const LEN: usize = 1 << ChunkedNodeSet::NUM_LOW_BITS;
    const PROMOTION_THRESHOLD: usize = Self::LEN / 64; // TODO: tune this constant

    fn insert(&mut self, value: u32) {
        assert!(usize::try_from(value).unwrap() < Self::LEN);
        match self {
            Chunk::Empty => {
                let mut vec = SmallVec::new();
                vec.push(value);
                *self = Chunk::VerySparse(vec);
            }
            Chunk::VerySparse(vec) => {
                assert_eq!(vec.capacity(), 8);
                if !vec.contains(&value) {
                    if vec.len() == 8 {
                        let mut set: RapidHashSet<_> = vec.iter().copied().collect();
                        set.insert(value);
                        *self = Chunk::Sparse(set);
                    } else {
                        vec.push(value);
                        assert_eq!(vec.capacity(), 8);
                    }
                }
            }
            Chunk::Sparse(set) => {
                let is_new = set.insert(value);
                if is_new && set.len() >= Self::PROMOTION_THRESHOLD {
                    let mut bitvec = BitVec::new(Self::LEN);
                    for &value in set.iter() {
                        bitvec.set(value.try_into().unwrap(), true);
                    }
                    *self = Chunk::Dense(bitvec.into());
                }
            }
            Chunk::Dense(bitvec) => {
                bitvec.set(value.try_into().unwrap(), true);
            }
        }
    }

    fn contains(&self, value: u32) -> bool {
        match self {
            Chunk::Empty => false,
            Chunk::VerySparse(vec) => vec.contains(&value),
            Chunk::Sparse(set) => set.contains(&value),
            Chunk::Dense(bitvec) => bitvec.get(value.try_into().unwrap()),
        }
    }
}

struct ChunkedNodeSet {
    num_nodes: usize,
    chunks: Vec<Chunk>,
}

impl ChunkedNodeSet {
    const NUM_LOW_BITS: usize = 24;
    const MASK: usize = (1 << Self::NUM_LOW_BITS) - 1;

    fn new(num_nodes: usize) -> Self {
        Self {
            num_nodes,
            chunks: (0..num_nodes.div_ceil(Chunk::LEN))
                .map(|_| Chunk::Empty)
                .collect(),
        }
    }

    #[inline(always)]
    fn split(value: usize) -> (usize, u32) {
        let high = value >> Self::NUM_LOW_BITS;
        let low = (value & Self::MASK) as u32;
        (high, low)
    }

    fn insert(&mut self, value: usize) {
        assert!(value < self.num_nodes);
        let (high, low) = Self::split(value);
        self.chunks[high].insert(low)
    }

    fn contains(&self, value: usize) -> bool {
        if value >= self.num_nodes {
            return false;
        }
        let (high, low) = Self::split(value);
        self.chunks[high].contains(low)
    }
}

/// Yielded by `dfs_with_path` to allow building a path as a `Vec<u8>` only when needed
pub struct PathParts<'a> {
    parts: &'a [LabelNameId],
    path_to_directory: bool,
}

impl PathParts<'_> {
    pub fn build_path<G>(&self, graph: &G) -> Vec<u8>
    where
        G: SwhGraphWithProperties,
        <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    {
        let mut path = Vec::with_capacity(self.parts.len() * 2 + 1);
        for &part in self.parts.iter() {
            path.extend(graph.properties().label_name(part));
            path.push(b'/');
        }
        if !self.path_to_directory {
            path.pop();
        }
        path
    }
}

/// Traverses backward from a directory or content, calling the callbacks on any directory
/// or revision node found.
///
/// `reachable_nodes` is the set of all nodes reachable from a head revision/release
/// (nodes not in the set are ignored); or `None` if all nodes are reachable.
///
/// If `on_directory` returns `false`, the directory's predecessors are ignored.
///
/// FIXME: `on_directory` is always called on the `root`, even if the `root` is a content
pub fn backward_dfs_with_path<G>(
    graph: &G,
    reachable_nodes: Option<&BitVec>,
    mut on_directory: impl FnMut(NodeId, PathParts) -> Result<bool>,
    mut on_revrel: impl FnMut(NodeId, PathParts) -> Result<()>,
    root: NodeId,
) -> Result<()>
where
    G: SwhLabeledBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: swh_graph::properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let reachable = |node| match reachable_nodes {
        Some(reachable_nodes) => reachable_nodes.get(node),
        None => true, // All nodes are reachable
    };
    if !reachable(root) {
        return Ok(());
    }

    let mut visited = ChunkedNodeSet::new(graph.num_nodes());
    let mut stack = Vec::new();
    let mut path_stack = PathStack::new();

    let root_is_directory = match graph.properties().node_type(root) {
        NodeType::Content => false,
        NodeType::Directory => true,
        _ => panic!(
            "backward_dfs_with_path called started from {}",
            graph.properties().swhid(root)
        ),
    };

    stack.push(root);
    path_stack.push([]);
    visited.insert(root);

    while let Some(node) = stack.pop() {
        let path_parts: Vec<_> = path_stack.pop().unwrap().collect();

        let should_recurse = on_directory(
            node,
            PathParts {
                parts: &path_parts,
                path_to_directory: root_is_directory,
            },
        )?;

        if should_recurse {
            // Look for frontiers in subdirectories
            for (pred, labels) in graph.untyped_labeled_predecessors(node) {
                if !reachable(pred) || visited.contains(pred) {
                    continue;
                }

                visited.insert(pred);

                match graph.properties().node_type(pred) {
                    NodeType::Directory => {
                        // If the same subdir/file is present in a directory twice under the same name,
                        // pick any name to represent both.
                        let Some(first_label) = labels.into_iter().next() else {
                            bail!(
                                "{} <- {} has no labels",
                                graph.properties().swhid(node),
                                graph.properties().swhid(pred),
                            )
                        };

                        // This is a dir->* arc, so its label is necessarily a DirEntry
                        let first_label: swh_graph::labels::DirEntry = first_label.into();

                        stack.push(pred);
                        path_stack.push(path_parts.iter().copied());
                        path_stack.push_filename(first_label.label_name_id());
                    }

                    NodeType::Revision | NodeType::Release => {
                        on_revrel(
                            pred,
                            PathParts {
                                parts: &path_parts,
                                path_to_directory: root_is_directory,
                            },
                        )?;

                        if graph.properties().node_type(pred) == NodeType::Revision {
                            for predpred in graph.predecessors(pred) {
                                if graph.properties().node_type(predpred) == NodeType::Release {
                                    on_revrel(
                                        predpred,
                                        PathParts {
                                            parts: &path_parts,
                                            path_to_directory: root_is_directory,
                                        },
                                    )?;
                                }
                            }
                        }
                    }
                    _ => (),
                }
            }
        }
    }

    Ok(())
}
