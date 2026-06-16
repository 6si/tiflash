// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Columns/ColumnDictionary.h>
#include <Columns/ColumnVector.h>
#include <Core/Field.h>

#include <vector>

namespace DB::DM
{

/**
 * EncodedGroupBy performs aggregation directly on dictionary-encoded data.
 *
 * Key insight: For a dictionary of N entries, aggregation reduces to array-indexed
 * accumulation. No hashing, no key comparison, no hash table probing.
 *
 * Example:
 *   Dictionary: {0: "US", 1: "UK", 2: "DE"}
 *   IDs: [0, 1, 0, 2, 0, 1]
 *   Values for SUM: [10, 20, 30, 40, 50, 60]
 *
 *   Result: agg[0]=90, agg[1]=80, agg[2]=40
 *   → {US: 90, UK: 80, DE: 40}
 *
 * Bail out to standard hash-based group-by if:
 * - Group-by column is not dictionary-encoded
 * - Cardinality exceeds threshold (too many groups for array-indexed approach)
 * - Multiple group-by columns with product of cardinalities > threshold
 */
class EncodedGroupBy
{
public:
    /// Maximum cardinality for encoded group-by (array-indexed approach)
    static constexpr size_t MAX_ENCODED_GROUPS = 4096;

    /// Aggregate function types supported in encoded mode
    enum class AggFunc
    {
        Sum,
        Count,
        Min,
        Max,
        Any,
    };

    /// Result of a single-column group-by with one aggregate
    struct GroupByResult
    {
        /// The dictionary entries (group keys), indexed by group ID
        const std::vector<Field> * group_keys = nullptr;

        /// Aggregated values, one per group (indexed by dictionary ID)
        std::vector<Field> aggregated_values;

        /// Row count per group
        std::vector<UInt64> group_counts;

        /// Whether encoded path was used
        bool used_encoded_path = false;

        /// Number of groups
        size_t num_groups = 0;
    };

    /// Result of group-by with multiple aggregates
    struct MultiAggResult
    {
        const std::vector<Field> * group_keys = nullptr;
        /// aggregated_values[agg_idx][group_id]
        std::vector<std::vector<Field>> aggregated_values;
        std::vector<UInt64> group_counts;
        bool used_encoded_path = false;
        size_t num_groups = 0;
    };

    /// Perform SUM group-by on dictionary-encoded group column with Int64 value column.
    static GroupByResult sumInt64(const ColumnDictionary & group_col, const PaddedPODArray<Int64> & values);

    /// Perform COUNT group-by on dictionary-encoded group column.
    static GroupByResult count(const ColumnDictionary & group_col);

    /// Perform MIN group-by on dictionary-encoded group column with Int64 value column.
    static GroupByResult minInt64(const ColumnDictionary & group_col, const PaddedPODArray<Int64> & values);

    /// Perform MAX group-by on dictionary-encoded group column with Int64 value column.
    static GroupByResult maxInt64(const ColumnDictionary & group_col, const PaddedPODArray<Int64> & values);

    /// Perform ANY (first value) group-by on dictionary-encoded group column.
    static GroupByResult anyInt64(const ColumnDictionary & group_col, const PaddedPODArray<Int64> & values);

    /// Perform multiple aggregates in a single pass over the data.
    static MultiAggResult multiAggregate(
        const ColumnDictionary & group_col,
        const std::vector<std::pair<AggFunc, const PaddedPODArray<Int64> *>> & agg_specs);

    /// Check if the column is suitable for encoded group-by.
    static bool isSuitableForEncodedGroupBy(const IColumn & column);

    /// Perform group-by with SUM on Float64 values.
    static GroupByResult sumFloat64(const ColumnDictionary & group_col, const PaddedPODArray<Float64> & values);
};

} // namespace DB::DM
