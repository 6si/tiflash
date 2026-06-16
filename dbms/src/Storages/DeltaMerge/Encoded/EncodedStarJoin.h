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
#include <Storages/DeltaMerge/Encoded/EncodedGroupBy.h>

#include <unordered_map>
#include <vector>

namespace DB::DM
{

/**
 * EncodedStarJoin implements a fused scan→join→aggregate pipeline on
 * dictionary-encoded fact table columns.
 *
 * For star-schema queries like:
 *   SELECT dim.name, SUM(fact.amount)
 *   FROM fact JOIN dim ON fact.dim_id = dim.id
 *   GROUP BY dim.name
 *
 * The traditional approach:
 *   1. Scan fact table (decode)
 *   2. Hash join with dimension table
 *   3. Hash group-by on result
 *
 * The encoded approach:
 *   1. Build hash table from dimension table (small, fits in cache)
 *   2. For dictionary-encoded fact column (dim_id), pre-compute join result
 *      for each dictionary entry
 *   3. Scan packed IDs — for each row, use dictionary ID to get:
 *      - Whether the row matches (join filter)
 *      - The dimension value (for group-by)
 *   4. Combine with encoded group-by in a single pass
 *
 * This is SingleStore's "killer feature" for star schemas.
 *
 * Requirements:
 *   - Join must be on integer column (or dictionary-encoded with integer values)
 *   - Join must be many-to-one (fact → dimension)
 *   - GROUP BY + aggregate must be present above the join
 */
class EncodedStarJoin
{
public:
    /// Pre-computed join result for a dictionary entry
    struct DictJoinEntry
    {
        bool matches = false; // Whether this dict entry joins successfully
        Int64 dimension_value = 0; // The joined dimension value (for group-by)
        UInt32 dimension_group_id = 0; // Group ID in the dimension result
    };

    /// Dimension table hash table (build side)
    struct DimensionHashTable
    {
        /// fact_key -> dimension_value mapping
        std::unordered_map<Int64, Int64> key_to_value;
        /// dimension_value -> group_id mapping (for group-by)
        std::unordered_map<Int64, UInt32> value_to_group_id;
        /// Distinct dimension values (group keys)
        std::vector<Field> group_keys;
        /// Number of distinct groups
        size_t num_groups = 0;
    };

    /// Result of a fused join + group-by + aggregate
    struct StarJoinResult
    {
        /// Group keys from the dimension table
        std::vector<Field> group_keys;
        /// Aggregated values per group
        std::vector<Field> aggregated_values;
        /// Row count per group
        std::vector<UInt64> group_counts;
        /// Number of groups
        size_t num_groups = 0;
        /// Whether encoded path was used
        bool used_encoded_path = false;
        /// Number of fact rows that joined successfully
        size_t rows_joined = 0;
        /// Number of fact rows that did not join (filtered out)
        size_t rows_not_joined = 0;
    };

    /// Build the dimension hash table from key and value arrays.
    static DimensionHashTable buildDimensionTable(
        const PaddedPODArray<Int64> & dim_keys,
        const PaddedPODArray<Int64> & dim_values);

    /// Build dimension table from key and string group values.
    static DimensionHashTable buildDimensionTableWithStringGroups(
        const PaddedPODArray<Int64> & dim_keys,
        const std::vector<Field> & dim_group_values);

    /// Pre-compute join results for each dictionary entry against the dimension table.
    static std::vector<DictJoinEntry> precomputeJoinForDictionary(
        const std::vector<Field> & fact_dictionary,
        const DimensionHashTable & dim_table);

    /// Execute fused join + group-by + SUM in a single pass.
    static StarJoinResult fusedJoinGroupBySum(
        const ColumnDictionary & fact_join_col,
        const PaddedPODArray<Int64> & fact_agg_values,
        const DimensionHashTable & dim_table);

    /// Execute fused join + group-by + COUNT.
    static StarJoinResult fusedJoinGroupByCount(
        const ColumnDictionary & fact_join_col,
        const DimensionHashTable & dim_table);

    /// Execute fused join + filter (without group-by).
    /// Returns IDs of fact rows that match the join condition.
    static IColumn::Filter computeJoinFilter(
        const ColumnDictionary & fact_join_col,
        const DimensionHashTable & dim_table);

    /// Check if an encoded star join is applicable.
    static bool isApplicable(
        const IColumn & fact_join_col,
        size_t dimension_size,
        bool has_group_by_above);
};

} // namespace DB::DM
