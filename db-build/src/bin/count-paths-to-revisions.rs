// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{ensure, Context, Result};
use clap::Parser;
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use mimalloc::MiMalloc;

use swh_graph::graph::*;
use swh_graph::mph::DynMphf;
use swh_graph::views::Subgraph;
use swh_graph::NodeConstraint;

use swh_graph_stdlib::labeling::{DenseLabels, Labels};
use swh_graph_topology::generations::{Direction, GenerationsReader, TypeAwareGenerationsReader};

use swh_provenance_db_build::filters::NodeFilter;
use swh_provenance_db_build::path_counts::count_paths_to_revisions;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Parser, Debug)]
/// Counts the number of paths from each directory to any revision or release
/// matching the node filter
///
/// Paths walk over arcs in the reverse direction (dir <- rev), so each node gets the sum
/// of the path counts of the revisions/releases pointing to it, directly or indirectly.
/// Each matching revision/release counts as a single trivial path to itself.
///
/// Counts in the output may be large enough to overflow long integers, so they are computed
/// with double-precision floating point number and written as such.
///
/// This requires a forward topological order as input
/// (ori->snp->rel->rev->dir->cnt, as returned by the toposort command).
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
    /// Path to the .bitstream file produced by the 'generations' executable
    topological_order: PathBuf,
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

    let toposorted_nodes_reader = GenerationsReader::new(&args.topological_order)
        .with_context(|| format!("Could not open {}", args.topological_order.display()))?;
    ensure!(
        toposorted_nodes_reader.properties().direction() == Direction::Forward,
        "count-paths-to-revisions requires a forward topological order \
         (ori->snp->rel->rev->dir->cnt), but {} is {}",
        args.topological_order.display(),
        toposorted_nodes_reader.properties().direction(),
    );
    let toposorted_nodes_reader =
        TypeAwareGenerationsReader::new(toposorted_nodes_reader, &graph, args.node_types)?;
    let toposorted_nodes = toposorted_nodes_reader
        .iter_nodes()
        .with_context(|| {
            format!(
                "Could not iter nodes from {}",
                args.topological_order.display()
            )
        })?
        .map(|(_generation, node)| node);

    let labels = count_paths_to_revisions(&graph, args.node_filter, toposorted_nodes)?;

    let (r1, r2) = rayon::join(
        || -> Result<_> {
            if let Some(parquet_out) = args.parquet_out {
                write_parquet(&graph, &labels, &parquet_out, args.thread_buffer_size).with_context(
                    || format!("Could not write Parquet table to {}", parquet_out.display()),
                )
            } else {
                Ok(())
            }
        },
        || -> Result<_> {
            if let Some(array_out) = args.array_out {
                write_arrays(&graph, &labels, &array_out)
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

fn write_arrays<G>(graph: &G, labels: &DenseLabels<f64>, dir: &Path) -> Result<()>
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

    let mut paths_to_revisions_writer = open(dir.join("paths_to_revisions.bin"))?;
    (0..graph.num_nodes()).try_for_each(|node| -> Result<_> {
        let label = labels.get(node).copied().unwrap_or(f64::NAN);
        paths_to_revisions_writer.write_all(&label.to_be_bytes())?;
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
    use rayon::prelude::*;

    use swh_graph::NodeType;

    #[derive(Debug)]
    pub struct OutputBuilder {
        swhid: StringBuilder,
        node: UInt64Builder,
        paths_to_revisions: Float64Builder,
    }

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("swhid", DataType::Utf8, false),
            Field::new("node", DataType::UInt64, false),
            Field::new("paths_to_revisions", DataType::Float64, false),
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
                paths_to_revisions: Float64Builder::new_from_buffer(
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
                Arc::new(self.paths_to_revisions.finish()),
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
                "paths_to_revisions".into(),
                Compression::ZSTD(ZstdLevel::try_new(3).unwrap()),
            )
            // Allows filtering only nodes with large/small values
            .set_column_statistics_enabled("paths_to_revisions".into(), EnabledStatistics::Page)
            .set_key_value_metadata(Some(swh_provenance_db_build::parquet_metadata(graph)))
            // 10× the default value.
            .set_max_row_group_size(10 * 1024 * 1024)
            .build()
    }

    pub(super) fn write_parquet<G>(
        graph: &G,
        labels: &DenseLabels<f64>,
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
                    if let Some(label) = labels.get(node) {
                        let builder = writer.builder()?;
                        builder
                            .swhid
                            .append_value(graph.properties().swhid(node).to_string());
                        builder
                            .node
                            .append_value(u64::try_from(node).expect("Node id overflowed u64"));
                        builder.paths_to_revisions.append_value(*label);
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
