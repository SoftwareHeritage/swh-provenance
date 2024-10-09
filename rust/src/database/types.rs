// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::hash::Hash;

use anyhow::{Context, Result};
use arrow::array::*;
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::*;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::metadata::{ParquetColumnIndex, ParquetOffsetIndex};

pub trait IndexKey: parquet::data_type::AsBytes + Hash + Eq + Clone {
    /// Returns whether the key may be in the column chunk based on its statistics
    ///
    /// Returns `None` when it cannot prune (ie. when all rows would be selected)
    fn check_column_chunk(
        keys: &[Self],
        statistics_converter: &StatisticsConverter,
        row_groups_metadata: &[RowGroupMetaData],
    ) -> Result<Option<BooleanBuffer>>;
    /// Given a page index, returns page ids within the index that may contain this key, as a
    /// boolean array.
    ///
    /// Returns `None` when it cannot prune (ie. when all rows would be selected)
    fn check_page_index<'a, I: IntoIterator<Item = &'a usize> + Copy>(
        keys: &[Self],
        statistics_converter: &StatisticsConverter<'a>,
        column_page_index: &ParquetColumnIndex,
        column_offset_index: &ParquetOffsetIndex,
        row_group_indices: I,
    ) -> Result<Option<BooleanBuffer>>;
}

#[derive(Hash, PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub struct Sha1Git(pub [u8; 20]);
impl parquet::data_type::AsBytes for Sha1Git {
    fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
impl IndexKey for Sha1Git {
    fn check_column_chunk(
        keys: &[Self],
        statistics_converter: &StatisticsConverter,
        row_groups_metadata: &[RowGroupMetaData],
    ) -> Result<Option<BooleanBuffer>> {
        // Should we even bother implementing this? Assuming a random distribution of SWHIDs among
        // row groups, and the default row group size, it's very unlikely we can prune a row group
        // based on statistics.
        Ok(None)
    }
    fn check_page_index<'a, I: IntoIterator<Item = &'a usize> + Copy>(
        keys: &[Self],
        statistics_converter: &StatisticsConverter<'a>,
        column_page_index: &ParquetColumnIndex,
        column_offset_index: &ParquetOffsetIndex,
        row_group_indices: I,
    ) -> Result<Option<BooleanBuffer>> {
        let min_key = keys
            .iter()
            .min()
            .cloned()
            .context("check_page_index got empty set of keys")?;
        let max_key = keys
            .iter()
            .max()
            .cloned()
            .context("check_page_index got empty set of keys")?;

        let data_page_mins = statistics_converter
            .data_page_mins(column_page_index, column_offset_index, row_group_indices)
            .context("Could not get row group statistics")?;
        let data_page_maxes = statistics_converter
            .data_page_maxes(column_page_index, column_offset_index, row_group_indices)
            .context("Could not get row group statistics")?;
        Ok(Some(
            arrow::compute::and(
                // Discard row groups whose smallest value is greater than the largest key
                &BooleanArray::from_unary(
                    data_page_mins
                        .as_fixed_size_binary_opt()
                        .context("Could not interpret statistics as FixedSizeBinaryArray")?,
                    |data_page_min| data_page_min <= &max_key.0[..],
                ),
                // Discard row groups whose largest value is less than the smallest key
                &BooleanArray::from_unary(
                    data_page_maxes
                        .as_fixed_size_binary_opt()
                        .context("Could not interpret statistics as FixedSizeBinaryArray")?,
                    |data_page_max| data_page_max >= &min_key.0[..],
                ),
            )
            .context("Could not build boolean array")?
            .into_parts().0,
        ))
    }
}

impl IndexKey for u64 {
    fn check_column_chunk(
        keys: &[Self],
        statistics_converter: &StatisticsConverter,
        row_groups_metadata: &[RowGroupMetaData],
    ) -> Result<Option<BooleanBuffer>> {
        let min_key = keys
            .iter()
            .min()
            .cloned()
            .context("check_column_chunk got empty set of keys")?;
        let max_key = keys
            .iter()
            .max()
            .cloned()
            .context("check_column_chunk got empty set of keys")?;

        let row_group_mins = statistics_converter
            .row_group_mins(row_groups_metadata)
            .context("Could not get row group statistics")?;
        let row_group_maxes = statistics_converter
            .row_group_maxes(row_groups_metadata)
            .context("Could not get row group statistics")?;
        Ok(Some(
            arrow::compute::and(
                // Discard row groups whose smallest value is greater than the largest key
                &BooleanArray::from_unary(
                    row_group_mins
                        .as_primitive_opt::<UInt64Type>()
                        .context("Could not interpret statistics as UInt64Array")?,
                    |row_group_min| row_group_min <= max_key,
                ),
                // Discard row groups whose largest value is less than the smallest key
                &BooleanArray::from_unary(
                    row_group_maxes
                        .as_primitive_opt::<UInt64Type>()
                        .context("Could not interpret statistics as UInt64Array")?,
                    |row_group_max| row_group_max >= min_key,
                ),
            )
            .context("Could not build boolean array")?
            .into_parts().0,
        ))
    }
    fn check_page_index<'a, I: IntoIterator<Item = &'a usize> + Copy>(
        keys: &[Self],
        statistics_converter: &StatisticsConverter<'a>,
        column_page_index: &ParquetColumnIndex,
        column_offset_index: &ParquetOffsetIndex,
        row_group_indices: I,
    ) -> Result<Option<BooleanBuffer>> {
        let min_key = keys
            .iter()
            .min()
            .cloned()
            .context("check_page_index got empty set of keys")?;
        let max_key = keys
            .iter()
            .max()
            .cloned()
            .context("check_page_index got empty set of keys")?;

        let data_page_mins = statistics_converter
            .data_page_mins(column_page_index, column_offset_index, row_group_indices)
            .context("Could not get row group statistics")?;
        let data_page_maxes = statistics_converter
            .data_page_maxes(column_page_index, column_offset_index, row_group_indices)
            .context("Could not get row group statistics")?;
        Ok(Some(
            arrow::compute::and(
                // Discard row groups whose smallest value is greater than the largest key
                &BooleanArray::from_unary(
                    data_page_mins
                        .as_primitive_opt::<UInt64Type>()
                        .context("Could not interpret statistics as UInt64Array")?,
                    |data_page_min| data_page_min < max_key,
                ),
                // Discard row groups whose largest value is less than the smallest key
                &BooleanArray::from_unary(
                    data_page_maxes
                        .as_primitive_opt::<UInt64Type>()
                        .context("Could not interpret statistics as UInt64Array")?,
                    |data_page_max| data_page_max > min_key,
                ),
            )
            .context("Could not build boolean array")?
            .into_parts().0,
        ))
    }
}
