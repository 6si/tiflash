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

#include <Columns/ColumnDictionary.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <unordered_map>

namespace DB
{

ColumnPtr ColumnDictionary::decode() const
{
    if (!value_type)
        throw Exception("ColumnDictionary: value_type is null, cannot decode", ErrorCodes::NOT_IMPLEMENTED);

    auto result = value_type->createColumn();
    result->reserve(ids.size());

    for (size_t i = 0; i < ids.size(); ++i)
    {
        UInt32 id = ids[i];
        if (id >= dictionary.size())
            throw Exception(
                fmt::format("ColumnDictionary::decode: ID {} exceeds dictionary size {}", id, dictionary.size()),
                ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
        result->insert(dictionary[id]);
    }

    return result->getPtr();
}

ColumnPtr ColumnDictionary::filter(const Filter & filt, ssize_t result_size_hint) const
{
    size_t count = ids.size();
    if (count != filt.size())
        throw Exception(
            "Size of filter doesn't match size of column",
            ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);

    PaddedPODArray<UInt32> new_ids;
    if (result_size_hint > 0)
        new_ids.reserve(result_size_hint);

    for (size_t i = 0; i < count; ++i)
    {
        if (filt[i])
            new_ids.push_back(ids[i]);
    }

    return ColumnDictionary::createMutable(dictionary, std::move(new_ids), value_type);
}

ColumnPtr ColumnDictionary::permute(const Permutation & perm, size_t limit) const
{
    size_t perm_size = perm.size();
    if (limit == 0)
        limit = perm_size;
    else
        limit = std::min(limit, perm_size);

    PaddedPODArray<UInt32> new_ids(limit);
    for (size_t i = 0; i < limit; ++i)
    {
        new_ids[i] = ids[perm[i]];
    }

    return ColumnDictionary::createMutable(dictionary, std::move(new_ids), value_type);
}

ColumnPtr ColumnDictionary::tryAutoEncode(
    const ColumnPtr & column,
    size_t min_rows,
    UInt32 max_dict_size)
{
    if (!column || column->size() < min_rows)
        return column;

    // Already dictionary-encoded
    if (column->isDictionaryEncoded())
        return column;

    // Handle Nullable(ColumnString): encode the nested column
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(column.get()))
    {
        const auto & nested = nullable->getNestedColumnPtr();
        auto encoded_nested = tryAutoEncode(nested, min_rows, max_dict_size);
        if (encoded_nested.get() != nested.get())
        {
            // Nested was encoded — wrap in Nullable again
            return ColumnNullable::create(encoded_nested, nullable->getNullMapColumnPtr());
        }
        return column;
    }

    const auto * col_str = typeid_cast<const ColumnString *>(column.get());
    if (!col_str)
        return column;

    const size_t num_rows = col_str->size();

    // For very low cardinality (common case: 5-50 entries), linear scan of a
    // small vector is faster than unordered_map due to no heap allocation per
    // node and better cache locality. Switch to hash map only above threshold.
    static constexpr size_t LINEAR_SCAN_THRESHOLD = 64;

    std::vector<Field> dict_entries;
    PaddedPODArray<UInt32> ids;
    ids.reserve(num_rows);

    // dict_refs holds StringRefs pointing into ColumnString's stable buffer.
    // Used for O(1) comparison during linear scan (no string copy needed).
    std::vector<StringRef> dict_refs;
    dict_refs.reserve(std::min(static_cast<size_t>(max_dict_size), LINEAR_SCAN_THRESHOLD));

    bool use_linear = true;
    std::unordered_map<StringRef, UInt32> dict_map;

    for (size_t i = 0; i < num_rows; ++i)
    {
        StringRef ref = col_str->getDataAt(i);

        if (use_linear)
        {
            // Linear scan for small dictionaries
            UInt32 found_id = static_cast<UInt32>(dict_refs.size());
            for (size_t d = 0; d < dict_refs.size(); ++d)
            {
                if (dict_refs[d] == ref)
                {
                    found_id = static_cast<UInt32>(d);
                    break;
                }
            }

            if (found_id < dict_refs.size())
            {
                ids.push_back(found_id);
            }
            else
            {
                if (dict_entries.size() >= max_dict_size)
                    return column;
                dict_refs.push_back(ref);
                dict_entries.emplace_back(String(ref.data, ref.size));
                ids.push_back(found_id);

                // Switch to hash map when linear scan becomes too expensive
                if (dict_refs.size() >= LINEAR_SCAN_THRESHOLD)
                {
                    use_linear = false;
                    for (size_t d = 0; d < dict_refs.size(); ++d)
                        dict_map[dict_refs[d]] = static_cast<UInt32>(d);
                }
            }
        }
        else
        {
            // Hash map for larger dictionaries
            auto it = dict_map.find(ref);
            if (it != dict_map.end())
            {
                ids.push_back(it->second);
            }
            else
            {
                if (dict_entries.size() >= max_dict_size)
                    return column;
                UInt32 new_id = static_cast<UInt32>(dict_entries.size());
                dict_map[ref] = new_id;
                dict_entries.emplace_back(String(ref.data, ref.size));
                ids.push_back(new_id);
            }
        }
    }

    return ColumnDictionary::createMutable(
        std::move(dict_entries),
        std::move(ids),
        std::make_shared<DataTypeString>());
}

} // namespace DB
