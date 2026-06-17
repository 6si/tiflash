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
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

ColumnPtr ColumnDictionary::decode() const
{
    if (!value_type)
        throw Exception("ColumnDictionary: value_type is null, cannot decode", ErrorCodes::NOT_IMPLEMENTED);

    // Create a mutable column of the appropriate type and populate it
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

    // Filter the IDs array, keep the dictionary intact
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

} // namespace DB
