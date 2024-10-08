// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

/// Parquet backend for the Provenance service
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::DataFusionError;
use datafusion::config::ConfigOptions;
use datafusion::datasource::file_format::parquet::ParquetFormatFactory;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::prelude::{SessionConfig, SessionContext};

mod caching_parquet_format;
use caching_parquet_format::CachingParquetFormatFactory;
mod pooled_reader;
use pooled_reader::ParquetFileReaderPool;
mod caching_parquet_reader;
use caching_parquet_reader::CachingParquetFileReaderFactory;
mod transaction;
pub use transaction::{TemporaryTable, Transaction};

struct ReplaceHashJoinWithNestedLoopJoin;

impl ReplaceHashJoinWithNestedLoopJoin {
    fn visit(
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Transformed<Arc<dyn ExecutionPlan>>, DataFusionError> {
        if let Some(old_plan) = plan.as_any().downcast_ref() {
            let HashJoinExec {
                left,
                right,
                on,
                filter,
                join_type,
                projection,
                null_equals_null,
                ..
            } = old_plan;
            if *null_equals_null {
                // FIXME: what is the semantics of NestedLoopJoinExec with regard to nulls?
                // Does it actually only support null_equals_null=true?
                return Err(DataFusionError::NotImplemented(
                    "ReplaceHashJoinWithNestedLoopJoin does not support null_equals_null=true"
                        .into(),
                ));
            }
            if !on.is_empty() {
                // FIXME: we really need to implement this
                return Err(DataFusionError::NotImplemented(
                    "ReplaceHashJoinWithNestedLoopJoin does not support on={on:?}".into(),
                ));
            }
            let mut new_plan: Arc<dyn ExecutionPlan> = Arc::new(NestedLoopJoinExec::try_new(
                left.clone(),
                right.clone(),
                filter.clone(),
                join_type,
            )?);
            // HashJoin has a built-in projection, but NestedLoopJoinExec does not, so we need to
            // add this extra node in the query plan.
            if let Some(column_indexes) = projection {
                let mut expr: Vec<(Arc<dyn PhysicalExpr>, _)> = Vec::new();
                for &column_index in column_indexes {
                    let field = old_plan.join_schema.field(column_index);
                    expr.push((
                        Arc::new(Column::new(field.name(), column_index)) as _,
                        field.name().clone(),
                    ));
                }
                new_plan = Arc::new(ProjectionExec::try_new(expr, new_plan)?)
            }
            Ok(Transformed::yes(new_plan))
        } else {
            Ok(Transformed::no(plan))
        }
    }
}

impl PhysicalOptimizerRule for ReplaceHashJoinWithNestedLoopJoin {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        plan.transform(Self::visit).data()
    }

    fn name(&self) -> &str {
        "replace_hash_join_with_nested_loop_join"
    }
    fn schema_check(&self) -> bool {
        true
    }
}

pub struct ProvenanceDatabase {
    pub ctx: SessionContext,
    last_txid: AtomicU64,
}

impl ProvenanceDatabase {
    pub async fn new(path: impl AsRef<Path>, cache_parquet: bool) -> Result<Self> {
        let path = path.as_ref();

        let config =
            SessionConfig::new().set_bool("datafusion.execution.parquet.pushdown_filters", true);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_physical_optimizer_rule(Arc::new(ReplaceHashJoinWithNestedLoopJoin))
            .build();
        let ctx = SessionContext::new_with_state(state);

        // Use the same underlying ParquetFormatFactory so they share their configuration
        let parquet_format_factory = ctx
            .state_ref()
            .read()
            .get_file_format_factory("parquet")
            .context("Could not get Parquet File Format")?;
        assert!(
            ctx.state_ref()
                .read()
                .get_file_format_factory("parquet")
                .unwrap()
                .as_any()
                .downcast_ref::<ParquetFormatFactory>()
                .is_some(),
            "unexpected type of parquet factory"
        );
        let caching_parquet_format_factory = Arc::new(CachingParquetFormatFactory::new(
            parquet_format_factory,
            CachingParquetFileReaderFactory::new,
        ));

        if cache_parquet {
            ctx.state_ref()
                .write()
                .register_file_format(
                    caching_parquet_format_factory,
                    true, // overwrite
                )
                .context("Could not register CachingParquetFormatFactory")?;
        }

        {
            let ctx = &ctx;
            futures::future::join_all(
                [
                    ("node", "nodes"),
                    ("c_in_d", "contents_in_frontier_directories"),
                    ("d_in_r", "frontier_directories_in_revisions"),
                    ("c_in_r", "contents_in_revisions_without_frontiers"),
                ]
                .into_iter()
                .map(|(table_name, dir_name)| async move {
                    let table_path = path.join(dir_name);
                    let Ok(table_path) = table_path.clone().into_os_string().into_string() else {
                        bail!("Could not parse table path {table_path:?} as UTF8");
                    };
                    ctx.sql(&format!(
                        "
                    CREATE EXTERNAL TABLE {}
                    STORED AS PARQUET
                    LOCATION '{}'
                    ",
                        table_name, table_path,
                    ))
                    .await
                    .with_context(|| {
                        format!(
                            "Could not register '{}' table from {}",
                            table_name, table_path
                        )
                    })
                }),
            )
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        }

        Ok(Self {
            ctx,
            last_txid: AtomicU64::new(0),
        })
    }

    pub fn transaction(&self) -> Transaction<'_> {
        Transaction::new(self, self.last_txid.fetch_add(1, Ordering::Relaxed))
    }
}
