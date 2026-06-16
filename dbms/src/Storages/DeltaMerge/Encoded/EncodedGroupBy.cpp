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

#include <Storages/DeltaMerge/Encoded/EncodedGroupBy.h>

#include <limits>

namespace DB
{
namespace ErrorCodes
{
extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
} // namespace ErrorCodes
} // namespace DB

namespace DB::DM
{

EncodedGroupBy::GroupByResult EncodedGroupBy::sumInt64(
    const ColumnDictionary & group_col,
    const PaddedPODArray<Int64> & values)
{
    GroupByResult result;
    const auto & ids = group_col.getDictionaryIds();
    const auto & dictionary = group_col.getDictionary();
    size_t num_groups = dictionary.size();

    if (ids.size() != values.size())
        throw Exception("Group column and value column have different sizes", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    result.group_keys = &dictionary;
    result.num_groups = num_groups;
    result.used_encoded_path = true;
    result.group_counts.resize(num_groups, 0);

    // Array-indexed accumulation — no hashing, no hash table probing
    std::vector<Int64> sums(num_groups, 0);
    for (size_t i = 0; i < ids.size(); ++i)
    {
        UInt32 gid = ids[i];
        sums[gid] += values[i];
        result.group_counts[gid]++;
    }

    result.aggregated_values.reserve(num_groups);
    for (size_t i = 0; i < num_groups; ++i)
        result.aggregated_values.emplace_back(sums[i]);

    return result;
}

EncodedGroupBy::GroupByResult EncodedGroupBy::count(const ColumnDictionary & group_col)
{
    GroupByResult result;
    const auto & ids = group_col.getDictionaryIds();
    const auto & dictionary = group_col.getDictionary();
    size_t num_groups = dictionary.size();

    result.group_keys = &dictionary;
    result.num_groups = num_groups;
    result.used_encoded_path = true;
    result.group_counts.resize(num_groups, 0);

    for (size_t i = 0; i < ids.size(); ++i)
    {
        result.group_counts[ids[i]]++;
    }

    result.aggregated_values.reserve(num_groups);
    for (size_t i = 0; i < num_groups; ++i)
        result.aggregated_values.emplace_back(UInt64(result.group_counts[i]));

    return result;
}

EncodedGroupBy::GroupByResult EncodedGroupBy::minInt64(
    const ColumnDictionary & group_col,
    const PaddedPODArray<Int64> & values)
{
    GroupByResult result;
    const auto & ids = group_col.getDictionaryIds();
    const auto & dictionary = group_col.getDictionary();
    size_t num_groups = dictionary.size();

    if (ids.size() != values.size())
        throw Exception("Group column and value column have different sizes", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    result.group_keys = &dictionary;
    result.num_groups = num_groups;
    result.used_encoded_path = true;
    result.group_counts.resize(num_groups, 0);

    std::vector<Int64> mins(num_groups, std::numeric_limits<Int64>::max());
    for (size_t i = 0; i < ids.size(); ++i)
    {
        UInt32 gid = ids[i];
        if (values[i] < mins[gid])
            mins[gid] = values[i];
        result.group_counts[gid]++;
    }

    result.aggregated_values.reserve(num_groups);
    for (size_t i = 0; i < num_groups; ++i)
        result.aggregated_values.emplace_back(mins[i]);

    return result;
}

EncodedGroupBy::GroupByResult EncodedGroupBy::maxInt64(
    const ColumnDictionary & group_col,
    const PaddedPODArray<Int64> & values)
{
    GroupByResult result;
    const auto & ids = group_col.getDictionaryIds();
    const auto & dictionary = group_col.getDictionary();
    size_t num_groups = dictionary.size();

    if (ids.size() != values.size())
        throw Exception("Group column and value column have different sizes", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    result.group_keys = &dictionary;
    result.num_groups = num_groups;
    result.used_encoded_path = true;
    result.group_counts.resize(num_groups, 0);

    std::vector<Int64> maxs(num_groups, std::numeric_limits<Int64>::min());
    for (size_t i = 0; i < ids.size(); ++i)
    {
        UInt32 gid = ids[i];
        if (values[i] > maxs[gid])
            maxs[gid] = values[i];
        result.group_counts[gid]++;
    }

    result.aggregated_values.reserve(num_groups);
    for (size_t i = 0; i < num_groups; ++i)
        result.aggregated_values.emplace_back(maxs[i]);

    return result;
}

EncodedGroupBy::GroupByResult EncodedGroupBy::anyInt64(
    const ColumnDictionary & group_col,
    const PaddedPODArray<Int64> & values)
{
    GroupByResult result;
    const auto & ids = group_col.getDictionaryIds();
    const auto & dictionary = group_col.getDictionary();
    size_t num_groups = dictionary.size();

    if (ids.size() != values.size())
        throw Exception("Group column and value column have different sizes", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    result.group_keys = &dictionary;
    result.num_groups = num_groups;
    result.used_encoded_path = true;
    result.group_counts.resize(num_groups, 0);

    std::vector<Int64> firsts(num_groups, 0);
    std::vector<bool> seen(num_groups, false);
    for (size_t i = 0; i < ids.size(); ++i)
    {
        UInt32 gid = ids[i];
        if (!seen[gid])
        {
            firsts[gid] = values[i];
            seen[gid] = true;
        }
        result.group_counts[gid]++;
    }

    result.aggregated_values.reserve(num_groups);
    for (size_t i = 0; i < num_groups; ++i)
        result.aggregated_values.emplace_back(firsts[i]);

    return result;
}

EncodedGroupBy::GroupByResult EncodedGroupBy::sumFloat64(
    const ColumnDictionary & group_col,
    const PaddedPODArray<Float64> & values)
{
    GroupByResult result;
    const auto & ids = group_col.getDictionaryIds();
    const auto & dictionary = group_col.getDictionary();
    size_t num_groups = dictionary.size();

    if (ids.size() != values.size())
        throw Exception("Group column and value column have different sizes", ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    result.group_keys = &dictionary;
    result.num_groups = num_groups;
    result.used_encoded_path = true;
    result.group_counts.resize(num_groups, 0);

    std::vector<Float64> sums(num_groups, 0.0);
    for (size_t i = 0; i < ids.size(); ++i)
    {
        UInt32 gid = ids[i];
        sums[gid] += values[i];
        result.group_counts[gid]++;
    }

    result.aggregated_values.reserve(num_groups);
    for (size_t i = 0; i < num_groups; ++i)
        result.aggregated_values.emplace_back(sums[i]);

    return result;
}

EncodedGroupBy::MultiAggResult EncodedGroupBy::multiAggregate(
    const ColumnDictionary & group_col,
    const std::vector<std::pair<AggFunc, const PaddedPODArray<Int64> *>> & agg_specs)
{
    MultiAggResult result;
    const auto & ids = group_col.getDictionaryIds();
    const auto & dictionary = group_col.getDictionary();
    size_t num_groups = dictionary.size();
    size_t num_aggs = agg_specs.size();

    result.group_keys = &dictionary;
    result.num_groups = num_groups;
    result.used_encoded_path = true;
    result.group_counts.resize(num_groups, 0);
    result.aggregated_values.resize(num_aggs);

    // Initialize aggregate state arrays
    std::vector<std::vector<Int64>> states(num_aggs);
    std::vector<std::vector<bool>> seen(num_aggs);
    for (size_t a = 0; a < num_aggs; ++a)
    {
        switch (agg_specs[a].first)
        {
        case AggFunc::Sum:
        case AggFunc::Count:
            states[a].resize(num_groups, 0);
            break;
        case AggFunc::Min:
            states[a].resize(num_groups, std::numeric_limits<Int64>::max());
            break;
        case AggFunc::Max:
            states[a].resize(num_groups, std::numeric_limits<Int64>::min());
            break;
        case AggFunc::Any:
            states[a].resize(num_groups, 0);
            seen[a].resize(num_groups, false);
            break;
        }
    }

    // Single pass over data
    for (size_t i = 0; i < ids.size(); ++i)
    {
        UInt32 gid = ids[i];
        result.group_counts[gid]++;

        for (size_t a = 0; a < num_aggs; ++a)
        {
            const auto * vals = agg_specs[a].second;
            Int64 val = vals ? (*vals)[i] : 1; // COUNT uses 1

            switch (agg_specs[a].first)
            {
            case AggFunc::Sum:
                states[a][gid] += val;
                break;
            case AggFunc::Count:
                states[a][gid]++;
                break;
            case AggFunc::Min:
                if (val < states[a][gid])
                    states[a][gid] = val;
                break;
            case AggFunc::Max:
                if (val > states[a][gid])
                    states[a][gid] = val;
                break;
            case AggFunc::Any:
                if (!seen[a][gid])
                {
                    states[a][gid] = val;
                    seen[a][gid] = true;
                }
                break;
            }
        }
    }

    // Convert states to Field arrays
    for (size_t a = 0; a < num_aggs; ++a)
    {
        result.aggregated_values[a].reserve(num_groups);
        for (size_t g = 0; g < num_groups; ++g)
            result.aggregated_values[a].emplace_back(states[a][g]);
    }

    return result;
}

bool EncodedGroupBy::isSuitableForEncodedGroupBy(const IColumn & column)
{
    if (const auto * dict_col = dynamic_cast<const ColumnDictionary *>(&column))
        return dict_col->getDictionarySize() <= MAX_ENCODED_GROUPS;
    return false;
}

} // namespace DB::DM
