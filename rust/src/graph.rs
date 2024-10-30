// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::path::{Path, PathBuf};

use swh_graph::graph::*;
use swh_graph::properties;

/// An implementation of [`SwhGraph`] that only provides access to its properties
pub struct MockSwhGraph<P> {
    pub num_nodes: usize,
    pub path: PathBuf,
    pub properties: P,
}

impl<P> SwhGraph for MockSwhGraph<P> {
    fn path(&self) -> &Path {
        self.path.as_path()
    }

    fn is_transposed(&self) -> bool {
        false
    }

    fn num_nodes(&self) -> usize {
        self.num_nodes
    }

    fn num_arcs(&self) -> u64 {
        0
    }

    fn has_arc(&self, _src_node_id: NodeId, _dst_node_id: NodeId) -> bool {
        false
    }
}

impl<
        MAPS: properties::MaybeMaps,
        TIMESTAMPS: properties::MaybeTimestamps,
        PERSONS: properties::MaybePersons,
        CONTENTS: properties::MaybeContents,
        STRINGS: properties::MaybeStrings,
        LABELNAMES: properties::MaybeLabelNames,
    > SwhGraphWithProperties
    for MockSwhGraph<
        properties::SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>,
    >
{
    type Maps = MAPS;
    type Timestamps = TIMESTAMPS;
    type Persons = PERSONS;
    type Contents = CONTENTS;
    type Strings = STRINGS;
    type LabelNames = LABELNAMES;

    fn properties(
        &self,
    ) -> &properties::SwhGraphProperties<MAPS, TIMESTAMPS, PERSONS, CONTENTS, STRINGS, LABELNAMES>
    {
        &self.properties
    }
}
