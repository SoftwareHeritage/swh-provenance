// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{bail, Context, Result};
use clap::Parser;
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use mimalloc::MiMalloc;
use rapidhash::RapidHashSet;
use rayon::prelude::*;

use swh_graph::graph::*;
use swh_graph::mph::DynMphf;
use swh_graph::views::Subgraph;
use swh_graph::{NodeConstraint, NodeType};

use swh_provenance_db_build::filters::NodeFilter;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
/// For each directory, counts the number of revisions matching the node filter it is in
struct Args {
    graph_path: PathBuf,
    #[arg(long, default_value = "*")]
    /// Types of nodes to consider
    node_types: NodeConstraint,
    #[arg(value_enum)]
    #[arg(long, default_value_t = NodeFilter::Heads)]
    /// Subset of revisions and releases that paths may end at
    node_filter: NodeFilter,
    #[arg(long)]
    /// Directory where to write the output, as Parquet files with one row per directory.
    ///
    /// This is the slowest to write, but it interoperates with generic tools
    parquet_out: Option<PathBuf>,
    #[arg(long)]
    /// Directory where to write the output, as mmappable f64 array files
    ///
    /// This is a reasonably efficient, but it allows very efficient point-queries
    /// from node ids (not SWHIDs).
    array_out: Option<PathBuf>,
    #[arg(long)]
    /// Maximum number of bytes in a thread's output Parquet buffer,
    /// before it is flushed to disk
    thread_buffer_size: Option<usize>,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    if args.parquet_out.is_none() && args.array_out.is_none() {
        log::warn!(
            "Called with neither --parquet-out nor --array-out, so results won't be written."
        );
    }

    log::info!("Loading graph");
    let graph = swh_graph::graph::SwhBidirectionalGraph::new(args.graph_path)
        .context("Could not load graph")?
        .init_properties()
        .load_properties(|props| props.load_maps::<DynMphf>())
        .context("Could not load maps")?;
    let graph = Subgraph::with_node_constraint(&graph, args.node_types);

    let counts = count_revisions_of_directories(&graph, args.node_filter)?;

    let (r1, r2) = rayon::join(
        || -> Result<_> {
            if let Some(parquet_out) = args.parquet_out {
                write_parquet(&graph, &counts, &parquet_out, args.thread_buffer_size).with_context(
                    || format!("Could not write Parquet table to {}", parquet_out.display()),
                )
            } else {
                Ok(())
            }
        },
        || -> Result<_> {
            if let Some(array_out) = args.array_out {
                write_arrays(&graph, &counts, &array_out)
                    .with_context(|| format!("Could not write arrays to {}", array_out.display()))
            } else {
                Ok(())
            }
        },
    );

    r1?;
    r2?;

    Ok(())
}

fn count_revisions_of_directories<G>(graph: &G, node_filter: NodeFilter) -> Result<Vec<u64>>
where
    G: SwhForwardGraph + SwhBackwardGraph + SwhGraphWithProperties + Send + Sync,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut pl = concurrent_progress_logger!(
        item_name = "node",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(graph.num_nodes()),
    );
    pl.start("Visiting revisions' directories...");

    let counts: Vec<_> = (0..graph.num_nodes()).map(|_| AtomicU64::new(0)).collect();
    swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes())
        .into_par_iter()
        .try_for_each_init(
            || pl.clone(),
            |pl, revrel| -> Result<()> {
                pl.light_update();
                if !swh_provenance_db_build::filters::is_root_revrel(graph, node_filter, revrel) {
                    return Ok(());
                }
                let mut todo = vec![revrel];
                let mut seen = RapidHashSet::default();
                while let Some(node) = todo.pop() {
                    for succ in graph.successors(node) {
                        let is_new = seen.insert(succ);
                        if !is_new {
                            continue;
                        }
                        match graph.properties().node_type(succ) {
                            NodeType::Content => continue,
                            NodeType::Directory => {
                                counts[succ].fetch_add(1, Ordering::Relaxed);
                            }
                            NodeType::Revision => {
                                if graph.properties().node_type(node) != NodeType::Release {
                                    // submodule
                                    continue;
                                }
                            }
                            NodeType::Release
                                if graph.properties().node_type(node) == NodeType::Release => {}
                            _ => bail!(
                                "Unexpected successor {} of {}",
                                graph.properties().swhid(succ),
                                graph.properties().swhid(node)
                            ),
                        }

                        todo.push(succ);
                    }
                }
                Ok(())
            },
        )?;

    // should compile to no-op
    let counts: Vec<_> = counts.into_iter().map(AtomicU64::into_inner).collect();

    Ok(counts)
}

fn write_arrays<G>(graph: &G, counts: &[u64], dir: &Path) -> Result<()>
where
    G: SwhGraph + Sync,
{
    std::fs::create_dir_all(dir)
        .with_context(|| format!("Could not create directory {}", dir.display()))?;
    let bytes_per_count = 8; // f64
    let mut pl = concurrent_progress_logger!(
        display_memory = true,
        local_speed = true,
        item_name = "byte",
        expected_updates = Some(graph.num_nodes() * bytes_per_count),
    );
    pl.start("Writing arrays...");

    let mut paths_to_revisions_writer = open(dir.join("revisions.bin"))?;
    (0..graph.num_nodes()).try_for_each(|node| -> Result<_> {
        paths_to_revisions_writer.write_all(&counts[node].to_be_bytes())?;
        pl.update_with_count(bytes_per_count);
        Ok(())
    })?;
    paths_to_revisions_writer.flush()?;

    pl.done();

    Ok(())
}

fn open(path: PathBuf) -> Result<BufWriter<File>> {
    File::create(&path)
        .with_context(|| format!("Could not create {}", path.display()))
        .map(BufWriter::new)
}

use parquet_table::*;

mod parquet_table {
    use super::*;

    use std::sync::Arc;

    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use dataset_writer::{ParallelDatasetWriter, ParquetTableWriter, StructArrayBuilder};
    use parquet::basic::{Compression, Encoding, ZstdLevel};
    use parquet::file::properties::{EnabledStatistics, WriterProperties};

    use swh_graph::NodeType;

    #[derive(Debug)]
    pub struct OutputBuilder {
        swhid: StringBuilder,
        node: UInt64Builder,
        revisions: UInt64Builder,
    }

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("swhid", DataType::Utf8, false),
            Field::new("node", DataType::UInt64, false),
            Field::new("revisions", DataType::UInt64, false),
        ])
    }

    impl Default for OutputBuilder {
        fn default() -> Self {
            Self {
                swhid: StringBuilder::default(), // TODO: don't allocate validity buffer
                node: UInt64Builder::new_from_buffer(
                    Default::default(),
                    None, // Values are not nullable -> validity buffer not needed
                ),
                revisions: UInt64Builder::new_from_buffer(
                    Default::default(),
                    None, // ditto
                ),
            }
        }
    }

    impl StructArrayBuilder for OutputBuilder {
        fn len(&self) -> usize {
            self.swhid.len()
        }

        fn buffer_size(&self) -> usize {
            // swhid + u64 + f64
            self.len() * (50 + 8 + 8)
        }

        fn finish(&mut self) -> Result<StructArray> {
            let columns: Vec<Arc<dyn Array>> = vec![
                Arc::new(self.swhid.finish()),
                Arc::new(self.node.finish()),
                Arc::new(self.revisions.finish()),
            ];

            Ok(StructArray::new(
                schema().fields().clone(),
                columns,
                None, // nulls
            ))
        }
    }

    fn writer_properties<G: SwhGraph>(graph: &G) -> parquet::file::properties::WriterProperties {
        WriterProperties::builder()
            // Allows filtering on node type
            .set_column_statistics_enabled("swhid".into(), EnabledStatistics::Page)
            // Allows looking for specific nodes
            .set_column_bloom_filter_enabled("swhid".into(), true)
            // Compress, as it's mostly an hexadecimal string
            .set_column_compression(
                "swhid".into(),
                Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            )
            // Allows looking for specific nodes in the node-ordered file, as files are
            // roughly sorted by node id
            .set_column_statistics_enabled("node".into(), EnabledStatistics::Page)
            // efficient encoding as nodes are roughly sorted by id
            .set_column_encoding("node".into(), Encoding::DELTA_BINARY_PACKED)
            // zstd is still beneficial
            .set_column_compression(
                "node".into(),
                Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            )
            .set_column_compression(
                "revisions".into(),
                Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            )
            // Allows filtering only nodes with large/small values
            .set_column_statistics_enabled("revisions".into(), EnabledStatistics::Page)
            .set_key_value_metadata(Some(swh_provenance_db_build::parquet_metadata(graph)))
            // 10× the default value.
            .set_max_row_group_size(10 * 1024 * 1024)
            .build()
    }

    pub(super) fn write_parquet<G>(
        graph: &G,
        counts: &[u64],
        dir: &Path,
        thread_buffer_size: Option<usize>,
    ) -> Result<()>
    where
        G: SwhGraph + SwhGraphWithProperties + Sync,
        <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    {
        let mut dataset_writer =
            ParallelDatasetWriter::<ParquetTableWriter<OutputBuilder>>::with_schema(
                dir.to_path_buf(),
                (Arc::new(schema()), writer_properties(graph)),
            )?;
        if let Some(autoflush_buffer_size) = thread_buffer_size {
            dataset_writer.config.autoflush_buffer_size = Some(autoflush_buffer_size);
        }

        let mut pl = concurrent_progress_logger!(
            display_memory = true,
            local_speed = true,
            item_name = "directory",
            expected_updates = Some(graph.num_nodes()),
        );
        pl.start("Writing Parquet...");

        (0..graph.num_nodes()).into_par_iter().try_for_each_init(
            || (pl.clone(), dataset_writer.get_thread_writer().unwrap()),
            |(pl, writer), node| -> Result<()> {
                if graph.properties().node_type(node) == NodeType::Directory {
                    let count = counts[node];
                    if count != u64::MAX {
                        let builder = writer.builder()?;
                        builder
                            .swhid
                            .append_value(graph.properties().swhid(node).to_string());
                        builder
                            .node
                            .append_value(u64::try_from(node).expect("Node id overflowed u64"));
                        builder.revisions.append_value(count);
                    }
                }
                pl.light_update();
                Ok(())
            },
        )?;

        dataset_writer.close()?;

        pl.done();

        Ok(())
    }
}
