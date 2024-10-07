// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Weak;

use anyhow::Result;
use datafusion::common::arrow::record_batch::RecordBatch;
use tracing::{span_enabled, Level};

use super::ProvenanceDatabase;

pub struct Transaction<'a> {
    db: &'a ProvenanceDatabase,
    id: u64,
    temporary_tables: Vec<Weak<TemporaryTable<'a>>>,
    last_table_id: AtomicU64,
}

impl<'a> Transaction<'a> {
    pub(super) fn new(db: &'a ProvenanceDatabase, id: u64) -> Self {
        Self {
            db,
            id,
            temporary_tables: Vec::new(),
            last_table_id: AtomicU64::new(0),
        }
    }

    pub async fn create_table_from_batch(
        &self,
        friendly_name: &str,
        data: RecordBatch,
    ) -> Result<TemporaryTable<'a>> {
        let table_id = self.last_table_id.fetch_add(1, Ordering::Relaxed);
        let table_name = format!("{}_{}_{}", friendly_name, self.id, table_id);
        self.db.ctx.register_batch(&table_name, data)?;
        Ok(TemporaryTable {
            name: table_name,
            marker: PhantomData,
        })
    }

    pub async fn create_table_from_query(
        &self,
        friendly_name: &str,
        query: &str,
    ) -> Result<TemporaryTable<'a>> {
        let table_id = self.last_table_id.fetch_add(1, Ordering::Relaxed);
        let table_name = format!("{}_{}_{}", friendly_name, self.id, table_id);
        if span_enabled!(Level::TRACE) {
            tracing::trace!("Creating {} table with:", table_name);
            for batch in self
                .db
                .ctx
                .sql(&format!("EXPLAIN {}", query))
                .await?
                .collect()
                .await?
            {
                tracing::trace!("{:?}", batch);
            }
        }
        self.db
            .ctx
            .sql(&format!("CREATE TABLE '{}' AS {}", table_name, query))
            .await?;
        Ok(TemporaryTable {
            name: table_name,
            marker: PhantomData,
        })
    }

    pub async fn close(&mut self) {
        futures::future::join_all(self.temporary_tables.iter().flat_map(Weak::upgrade).map(
            |table| {
                let ctx = self.db.ctx.clone();
                async move { ctx.sql(&format!("DROP TABLE '{}'", table.name)).await }
            },
        ))
        .await;
        self.temporary_tables.clear();
    }

    pub fn db(&self) -> &ProvenanceDatabase {
        self.db
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.temporary_tables.is_empty() {
            tracing::debug!("Transaction {} was dropped before being closed", self.id);
            futures::executor::block_on(self.close());
        }
    }
}

#[derive(Clone)]
pub struct TemporaryTable<'a> {
    name: String,
    /// Ensures at compile time that the TemporaryTable does not outlive the transaction,
    marker: PhantomData<&'a Transaction<'a>>,
}

impl<'a> Display for TemporaryTable<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}
