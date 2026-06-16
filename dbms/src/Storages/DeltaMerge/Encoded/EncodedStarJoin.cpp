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

#include <Storages/DeltaMerge/Encoded/EncodedStarJoin.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
} // namespace ErrorCodes
} // namespace DB

namespace DB::DM
{

EncodedStarJoin::DimensionHashTable EncodedStarJoin::buildDimensionTable(
    const PaddedPODArray<Int64> & dim_keys,
    const PaddedPODArray<Int64> & dim_values)
{
    DimensionHashTable table;

    for (size_t i = 0; i < dim_keys.size(); ++i)
    {
        Int64 key = dim_keys[i];
        Int64 value = dim_values[i];
        table.key_to_value[key] = value;

        if (table.value_to_group_id.find(value) == table.value_to_group_id.end())
        {
            UInt32 gid = static_cast<UInt32>(table.group_keys.size());
            table.value_to_group_id[value] = gid;
            table.group_keys.emplace_back(value);
        }
    }

    table.num_groups = table.group_keys.size();
    return table;
}

EncodedStarJoin::DimensionHashTable EncodedStarJoin::buildDimensionTableWithStringGroups(
    const PaddedPODArray<Int64> & dim_keys,
    const std::vector<Field> & dim_group_values)
{
    DimensionHashTable table;

    for (size_t i = 0; i < dim_keys.size(); ++i)
    {
        Int64 key = dim_keys[i];
        // Store key → index mapping (use the value as index for group lookup)
        table.key_to_value[key] = static_cast<Int64>(i);

        // Build group from the provided values
        Int64 group_value = static_cast<Int64>(i);
        if (table.value_to_group_id.find(group_value) == table.value_to_group_id.end())
        {
            UInt32 gid = static_cast<UInt32>(table.group_keys.size());
            table.value_to_group_id[group_value] = gid;
            if (i < dim_group_values.size())
                table.group_keys.push_back(dim_group_values[i]);
            else
                table.group_keys.emplace_back(group_value);
        }
    }

    table.num_groups = table.group_keys.size();
    return table;
}

std::vector<EncodedStarJoin::DictJoinEntry> EncodedStarJoin::precomputeJoinForDictionary(
    const std::vector<Field> & fact_dictionary,
    const DimensionHashTable & dim_table)
{
    std::vector<DictJoinEntry> entries(fact_dictionary.size());

    for (size_t i = 0; i < fact_dictionary.size(); ++i)
    {
        const auto & field = fact_dictionary[i];
        Int64 key = 0;

        // Extract integer key from Field
        if (field.getType() == Field::Types::Int64)
            key = field.get<Int64>();
        else if (field.getType() == Field::Types::UInt64)
            key = static_cast<Int64>(field.get<UInt64>());
        else
            continue; // Non-integer keys don't match

        auto it = dim_table.key_to_value.find(key);
        if (it != dim_table.key_to_value.end())
        {
            entries[i].matches = true;
            entries[i].dimension_value = it->second;

            auto git = dim_table.value_to_group_id.find(it->second);
            if (git != dim_table.value_to_group_id.end())
                entries[i].dimension_group_id = git->second;
        }
    }

    return entries;
}

EncodedStarJoin::StarJoinResult EncodedStarJoin::fusedJoinGroupBySum(
    const ColumnDictionary & fact_join_col,
    const PaddedPODArray<Int64> & fact_agg_values,
    const DimensionHashTable & dim_table)
{
    StarJoinResult result;
    result.used_encoded_path = true;
    result.group_keys = dim_table.group_keys;
    result.num_groups = dim_table.num_groups;
    result.rows_joined = 0;
    result.rows_not_joined = 0;

    const auto & ids = fact_join_col.getDictionaryIds();
    const auto & dictionary = fact_join_col.getDictionary();

    if (ids.size() != fact_agg_values.size())
        throw Exception("Fact join column and aggregate column have different sizes", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    // Phase 1: Pre-compute join result for each dictionary entry
    auto dict_join = precomputeJoinForDictionary(dictionary, dim_table);

    // Phase 2: Single pass — fused join + group-by + SUM
    std::vector<Int64> sums(dim_table.num_groups, 0);
    result.group_counts.resize(dim_table.num_groups, 0);

    for (size_t i = 0; i < ids.size(); ++i)
    {
        UInt32 dict_id = ids[i];
        const auto & join_entry = dict_join[dict_id];

        if (join_entry.matches)
        {
            UInt32 gid = join_entry.dimension_group_id;
            sums[gid] += fact_agg_values[i];
            result.group_counts[gid]++;
            result.rows_joined++;
        }
        else
        {
            result.rows_not_joined++;
        }
    }

    result.aggregated_values.reserve(dim_table.num_groups);
    for (size_t i = 0; i < dim_table.num_groups; ++i)
        result.aggregated_values.emplace_back(sums[i]);

    return result;
}

EncodedStarJoin::StarJoinResult EncodedStarJoin::fusedJoinGroupByCount(
    const ColumnDictionary & fact_join_col,
    const DimensionHashTable & dim_table)
{
    StarJoinResult result;
    result.used_encoded_path = true;
    result.group_keys = dim_table.group_keys;
    result.num_groups = dim_table.num_groups;
    result.rows_joined = 0;
    result.rows_not_joined = 0;

    const auto & ids = fact_join_col.getDictionaryIds();
    const auto & dictionary = fact_join_col.getDictionary();

    // Pre-compute join for each dictionary entry
    auto dict_join = precomputeJoinForDictionary(dictionary, dim_table);

    // Single pass — fused join + group-by + COUNT
    result.group_counts.resize(dim_table.num_groups, 0);

    for (size_t i = 0; i < ids.size(); ++i)
    {
        UInt32 dict_id = ids[i];
        const auto & join_entry = dict_join[dict_id];

        if (join_entry.matches)
        {
            result.group_counts[join_entry.dimension_group_id]++;
            result.rows_joined++;
        }
        else
        {
            result.rows_not_joined++;
        }
    }

    result.aggregated_values.reserve(dim_table.num_groups);
    for (size_t i = 0; i < dim_table.num_groups; ++i)
        result.aggregated_values.emplace_back(UInt64(result.group_counts[i]));

    return result;
}

IColumn::Filter EncodedStarJoin::computeJoinFilter(
    const ColumnDictionary & fact_join_col,
    const DimensionHashTable & dim_table)
{
    const auto & ids = fact_join_col.getDictionaryIds();
    const auto & dictionary = fact_join_col.getDictionary();

    // Pre-compute
    auto dict_join = precomputeJoinForDictionary(dictionary, dim_table);

    // Apply per-row via dictionary ID lookup
    IColumn::Filter filter(ids.size());
    for (size_t i = 0; i < ids.size(); ++i)
    {
        filter[i] = dict_join[ids[i]].matches ? 1 : 0;
    }
    return filter;
}

bool EncodedStarJoin::isApplicable(
    const IColumn & fact_join_col,
    size_t dimension_size,
    bool has_group_by_above)
{
    // Must be dictionary-encoded
    const auto * dict_col = dynamic_cast<const ColumnDictionary *>(&fact_join_col);
    if (!dict_col)
        return false;

    // Dimension must be small enough to fit in cache
    if (dimension_size > 100000)
        return false;

    // Should have group-by above for the fused path to be beneficial
    if (!has_group_by_above)
        return false;

    // Dictionary cardinality must be reasonable
    if (dict_col->getDictionarySize() > EncodedGroupBy::MAX_ENCODED_GROUPS)
        return false;

    return true;
}

} // namespace DB::DM
