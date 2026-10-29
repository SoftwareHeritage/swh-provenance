// Copyright (C) 2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;

use anyhow::{bail, ensure, Context, Result};
use clap::Parser;
use dsi_progress_logger::{concurrent_progress_logger, progress_logger, ProgressLog};
use mimalloc::MiMalloc;
use rapidhash::RapidHashSet;
use rayon::prelude::*;
use value_traits::slices::SliceByValue;

use swh_graph::graph::*;
use swh_graph::mph::DynMphf;
use swh_graph::utils::mmap::NumberMmap;
use swh_graph::NodeType;
use swh_graph_stdlib::collections::{AdaptiveNodeSet, NodeSet, ReadNodeSet};

use swh_provenance_db_build::filters::{is_root_revrel, NodeFilter};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum CutFilter {
    All,
    Every10,
    Every100,
    Every1k,
    Every10k,
    Every100k,
    Every1m,
    Every10m,
    Every100m,
}

impl CutFilter {
    fn matches<G>(self, _graph: &G, node: NodeId) -> bool {
        use CutFilter::*;

        match self {
            All => true,
            Every10 => node % 10 == 0,
            Every100 => node % 100 == 0,
            Every1k => node % 1_000 == 0,
            Every10k => node % 10_000 == 0,
            Every100k => node % 100_000 == 0,
            Every1m => node % 1_000_000 == 0,
            Every10m => node % 10_000_000 == 0,
            Every100m => node % 100_000_000 == 0,
        }
    }
}

#[derive(Debug)]
pub struct GenerationEvaluation {
    pub dir_to_cnt: u64,
    pub dir_to_rev: u64,
    pub cnt_to_rev_without_dir: u64,
}

#[derive(Parser, Debug)]
/// Looks for the best generation (ie. depth) of directories to use as a cut
///
/// to break combinatorial explosion of (cnt, rev) pairs into the minimal number of
/// (cnt, dir) and (dir, rev) pairs (and remaining (cnt, rev) pairs with no path through
/// a dir of the cut).
struct Args {
    graph_path: PathBuf,
    #[arg(long)]
    /// Path to a topological order
    generations: PathBuf,
    #[arg(long)]
    dir_to_rev_counts: PathBuf,
    #[arg(value_enum)]
    #[arg(long, default_value_t = NodeFilter::Heads)]
    /// Subset of revisions and releases to traverse from
    node_filter: NodeFilter,
    #[arg(long, default_value = "all")]
    cut_filter: CutFilter,
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20"
    )]
    generation_numbers: Vec<usize>,
    #[arg(long)]
    skip_dir_to_cnt_count: bool,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    log::info!("Loading graph");
    let graph = swh_graph::graph::SwhBidirectionalGraph::new(args.graph_path)
        .context("Could not load graph")?
        .init_properties()
        .load_properties(|props| props.load_maps::<DynMphf>())
        .context("Could not load maps")?;
    log::info!("Graph loaded.");

    let generations = swh_graph_topology::generations::GenerationsReader::new(&args.generations)
        .with_context(|| {
            format!(
                "Could not open generations from {}",
                args.generations.display()
            )
        })?;

    let dir_to_rev_counts =
        NumberMmap::<byteorder::BigEndian, u64, _>::new(&args.dir_to_rev_counts, graph.num_nodes())
            .context("Could not mmap dir_to_rev_counts")?;

    let mut pl = progress_logger!(
        item_name = "generation",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(args.generation_numbers.len()),
    );
    pl.start("Evaluating generations...");

    let mut remaining_generations: HashSet<_> = args.generation_numbers.iter().copied().collect();

    let mut evals = vec![];
    for (generation_id, generation) in generations
        .iter_generations()
        .context("Could not iterate generations")?
        .enumerate()
    {
        if args.generation_numbers.contains(&generation_id) {
            remaining_generations.remove(&generation_id);
            let eval = evaluate_generation(
                &graph,
                generation_id,
                generation,
                &dir_to_rev_counts,
                args.node_filter,
                args.cut_filter,
                args.skip_dir_to_cnt_count,
            )
            .with_context(|| format!("Could not evaluate generation {generation_id}"))?;
            evals.push(eval);
            for (i, eval) in evals.iter().enumerate() {
                println!("Generation {}: {eval:#?}", args.generation_numbers[i]);
            }
        }
        pl.update();
        if remaining_generations.is_empty() {
            break;
        }
    }

    pl.done();

    Ok(())
}

fn evaluate_generation<G>(
    graph: &G,
    generation_id: usize,
    generation: impl Iterator<Item = NodeId>,
    dir_to_rev_counts: &(impl SliceByValue<Value = u64> + Send + Sync),
    node_filter: NodeFilter,
    cut_filter: CutFilter,
    skip_dir_to_cnt_count: bool,
) -> Result<GenerationEvaluation>
where
    G: SwhBackwardGraph + SwhForwardGraph + SwhGraphWithProperties + Send + Sync + 'static,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    log::info!("Reading generation {generation_id}...");
    let generation: Vec<_> = generation.collect();

    ensure!(
        dir_to_rev_counts.len() == graph.num_nodes(),
        "Expected dir_to_rev_counts has length {} but graph has {} nodes",
        dir_to_rev_counts.len(),
        graph.num_nodes()
    );

    log::info!("Computing dir_to_rev...");
    let dir_to_rev = generation
        .par_iter()
        .map(|&node| {
            if graph.properties().node_type(node) == NodeType::Directory
                && cut_filter.matches(graph, node)
            {
                dir_to_rev_counts
                    .get_value(node)
                    .expect("dir_to_rev_counts too short")
            } else {
                0
            }
        })
        .sum();

    let mut pl = concurrent_progress_logger!(
        item_name = "dir",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(generation.len()),
    );
    pl.start("Computing dir_to_cnt...");
    let dir_to_cnt = if skip_dir_to_cnt_count {
        0
    } else {
        generation
            .par_iter()
            .map_with(pl.clone(), |pl, &node| {
                pl.light_update();
                if graph.properties().node_type(node) == NodeType::Directory
                    && cut_filter.matches(graph, node)
                {
                    count_dir_to_cnt(graph, node).expect("count_dir_to_cnt failed")
                } else {
                    0
                }
            })
            .sum()
    };
    pl.done();

    let mut pl = concurrent_progress_logger!(
        item_name = "node",
        display_memory = true,
        local_speed = true,
        expected_updates = Some(graph.num_nodes()),
    );
    pl.start("Computing cnt_to_rev_without_dir...");
    let cnt_to_rev_without_dir =
        swh_graph::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes())
            .map_with(pl.clone(), |thread_pl, root| -> Result<u64> {
                thread_pl.light_update();
                if is_root_revrel(graph, node_filter, root) {
                    if let Some(root_dir) = swh_graph_stdlib::find_root_dir(graph, root)
                        .context("Could not pick root directory")?
                    {
                        return count_short_rev_to_cnt(graph, root_dir, generation_id);
                    }
                }
                Ok(0)
            })
            .reduce(|| Ok(0), |left, right| Ok(left? + right?))?;

    pl.done();

    log::info!("Visits done, finishing output");

    Ok(GenerationEvaluation {
        dir_to_cnt,
        dir_to_rev,
        cnt_to_rev_without_dir,
    })
}

fn count_dir_to_cnt<G>(graph: &G, root_dir: NodeId) -> Result<u64>
where
    G: SwhForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let mut todo = vec![root_dir];
    let mut seen = RapidHashSet::default();

    let mut num_cnt = 0;
    while let Some(node) = todo.pop() {
        for succ in graph.successors(node) {
            let is_new = seen.insert(succ);
            if !is_new {
                continue;
            }
            match graph.properties().node_type(succ) {
                NodeType::Revision => {
                    // submodule
                    continue;
                }
                NodeType::Directory => {}
                NodeType::Content => num_cnt += 1,
                _ => bail!("Unexpected node type: {}", graph.properties().swhid(node)),
            }
            todo.push(succ);
        }
    }

    Ok(num_cnt)
}

fn count_short_rev_to_cnt<G>(graph: &G, root_dir: NodeId, max_depth: usize) -> Result<u64>
where
    G: SwhForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let marker = usize::MAX;
    let mut todo: VecDeque<_> = [root_dir, marker].into_iter().collect();
    let mut seen = AdaptiveNodeSet::new(graph.num_nodes());

    let mut depth = 0;
    let mut num_cnt = 0;
    while let Some(node) = todo.pop_front() {
        if node == marker {
            depth += 1;
            if depth >= max_depth {
                break;
            }
            todo.push_back(marker);
        } else {
            for succ in graph.successors(node) {
                if seen.contains(succ) {
                    continue;
                }
                seen.insert(succ);
                match graph.properties().node_type(succ) {
                    NodeType::Revision => {
                        // submodule
                        continue;
                    }
                    NodeType::Directory => {}
                    NodeType::Content => num_cnt += 1,
                    _ => bail!("Unexpected node type: {}", graph.properties().swhid(node)),
                }
                todo.push_back(succ);
            }
        }
    }

    Ok(num_cnt)
}
