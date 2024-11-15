// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::atomic::{AtomicU64, Ordering};

use parquet_aramid::metrics::Timing;

#[derive(Debug, Default)]
pub struct TableScanMetrics {
    pub rows_pruned_by_row_filter: AtomicU64,
    pub rows_selected_by_row_filter: AtomicU64,

    pub row_filter_eval_time: Timing,
    pub row_filter_eval_loop_time: Timing,
}

impl std::ops::AddAssign<&Self> for TableScanMetrics {
    fn add_assign(&mut self, rhs: &Self) {
        self.rows_pruned_by_row_filter.fetch_add(
            rhs.rows_pruned_by_row_filter.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.rows_selected_by_row_filter.fetch_add(
            rhs.rows_selected_by_row_filter.load(Ordering::SeqCst),
            Ordering::SeqCst,
        );

        self.row_filter_eval_time
            .add(rhs.row_filter_eval_time.get());
        self.row_filter_eval_loop_time
            .add(rhs.row_filter_eval_loop_time.get());
    }
}
impl<T: AsRef<Self>> std::ops::AddAssign<T> for TableScanMetrics {
    fn add_assign(&mut self, rhs: T) {
        *self += rhs.as_ref();
    }
}
impl std::ops::AddAssign<Self> for TableScanMetrics {
    fn add_assign(&mut self, rhs: Self) {
        *self += &rhs;
    }
}
impl std::ops::Add<&Self> for TableScanMetrics {
    type Output = Self;

    fn add(mut self, rhs: &Self) -> Self::Output {
        self += rhs;
        self
    }
}
impl<T: AsRef<Self>> std::ops::Add<T> for TableScanMetrics {
    type Output = Self;

    fn add(self, rhs: T) -> Self::Output {
        self + rhs.as_ref()
    }
}
impl std::ops::Add for TableScanMetrics {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        self + &rhs
    }
}
impl std::iter::Sum for TableScanMetrics {
    fn sum<I: std::iter::Iterator<Item = Self>>(it: I) -> Self {
        let mut sum = Self::default();
        for item in it {
            sum += item;
        }
        sum
    }
}
