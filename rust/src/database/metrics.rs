// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::atomic::AtomicU64;

use parquet_aramid::metrics::Timing;

#[derive(Debug, Default)]
pub struct TableScanMetrics {
    pub rows_pruned_by_row_filter: AtomicU64,
    pub rows_selected_by_row_filter: AtomicU64,

    pub row_filter_eval_time: Timing,
    pub row_filter_eval_loop_time: Timing,
}
