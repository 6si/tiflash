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

#include <Storages/DeltaMerge/Encoded/DictionaryEncodingEngine.h>

namespace DB::DM
{

bool DictionaryEncodingEngine::isSuitableForDictionary(
    const IColumn & column,
    const DictionaryEncodingConfig & config)
{
    if (!config.enabled)
        return false;

    if (column.size() < config.min_rows_for_encoding)
        return false;

    size_t cardinality = computeCardinality(column, config.max_cardinality + 1);
    return cardinality <= config.max_cardinality;
}

size_t DictionaryEncodingEngine::computeCardinality(const IColumn & column, size_t max_check)
{
    size_t count = column.size();
    if (max_check == 0)
        max_check = count;

    std::vector<Field> distinct;

    for (size_t i = 0; i < count; ++i)
    {
        Field f;
        column.get(i, f);

        bool found = false;
        for (const auto & d : distinct)
        {
            if (d == f)
            {
                found = true;
                break;
            }
        }
        if (!found)
        {
            distinct.push_back(std::move(f));
            if (distinct.size() > max_check)
                return distinct.size();
        }
    }
    return distinct.size();
}

DictionaryEncodingEngine::EncodingResult DictionaryEncodingEngine::tryEncode(
    const IColumn & column,
    const DataTypePtr & type,
    const DictionaryEncodingConfig & config)
{
    EncodingResult result;
    result.was_encoded = false;
    result.column = column.getPtr();
    result.cardinality = 0;

    if (!config.enabled || column.size() < config.min_rows_for_encoding)
        return result;

    // Build dictionary using linear search (fine for cardinality ≤ 4096)
    std::vector<Field> dictionary;
    PaddedPODArray<UInt32> ids;
    ids.reserve(column.size());

    for (size_t i = 0; i < column.size(); ++i)
    {
        Field f;
        column.get(i, f);

        UInt32 id = 0;
        bool found = false;
        for (size_t d = 0; d < dictionary.size(); ++d)
        {
            if (dictionary[d] == f)
            {
                id = static_cast<UInt32>(d);
                found = true;
                break;
            }
        }

        if (!found)
        {
            if (dictionary.size() >= config.max_cardinality)
            {
                result.cardinality = dictionary.size();
                return result;
            }
            id = static_cast<UInt32>(dictionary.size());
            dictionary.push_back(std::move(f));
        }
        ids.push_back(id);
    }

    result.was_encoded = true;
    result.cardinality = dictionary.size();
    result.column = ColumnDictionary::createMutable(std::move(dictionary), std::move(ids), type);
    return result;
}

ColumnDictionary::MutablePtr DictionaryEncodingEngine::encodeStringColumn(
    const ColumnString & column,
    const DataTypePtr & type)
{
    std::vector<Field> dictionary;
    std::unordered_map<StringRef, UInt32> value_to_id;
    PaddedPODArray<UInt32> ids;
    ids.reserve(column.size());

    for (size_t i = 0; i < column.size(); ++i)
    {
        auto ref = column.getDataAt(i);
        auto it = value_to_id.find(ref);
        if (it == value_to_id.end())
        {
            UInt32 id = static_cast<UInt32>(dictionary.size());
            value_to_id[ref] = id;
            dictionary.emplace_back(ref.toString());
            ids.push_back(id);
        }
        else
        {
            ids.push_back(it->second);
        }
    }

    return ColumnDictionary::createMutable(std::move(dictionary), std::move(ids), type);
}

template <typename T>
ColumnDictionary::MutablePtr DictionaryEncodingEngine::encodeIntegerColumn(
    const ColumnVector<T> & column,
    const DataTypePtr & type)
{
    std::vector<Field> dictionary;
    std::unordered_map<T, UInt32> value_to_id;
    PaddedPODArray<UInt32> ids;
    ids.reserve(column.size());

    const auto & data = column.getData();
    for (size_t i = 0; i < data.size(); ++i)
    {
        T val = data[i];
        auto it = value_to_id.find(val);
        if (it == value_to_id.end())
        {
            UInt32 id = static_cast<UInt32>(dictionary.size());
            value_to_id[val] = id;
            dictionary.emplace_back(Int64(val));
            ids.push_back(id);
        }
        else
        {
            ids.push_back(it->second);
        }
    }

    return ColumnDictionary::createMutable(std::move(dictionary), std::move(ids), type);
}

bool DictionaryEncodingEngine::shouldUseDictionaryCompression(
    const IColumn & column,
    const DictionaryEncodingConfig & config)
{
    return isSuitableForDictionary(column, config);
}

// Explicit template instantiations
template ColumnDictionary::MutablePtr DictionaryEncodingEngine::encodeIntegerColumn<Int8>(const ColumnVector<Int8> &, const DataTypePtr &);
template ColumnDictionary::MutablePtr DictionaryEncodingEngine::encodeIntegerColumn<Int16>(const ColumnVector<Int16> &, const DataTypePtr &);
template ColumnDictionary::MutablePtr DictionaryEncodingEngine::encodeIntegerColumn<Int32>(const ColumnVector<Int32> &, const DataTypePtr &);
template ColumnDictionary::MutablePtr DictionaryEncodingEngine::encodeIntegerColumn<Int64>(const ColumnVector<Int64> &, const DataTypePtr &);
template ColumnDictionary::MutablePtr DictionaryEncodingEngine::encodeIntegerColumn<UInt8>(const ColumnVector<UInt8> &, const DataTypePtr &);
template ColumnDictionary::MutablePtr DictionaryEncodingEngine::encodeIntegerColumn<UInt16>(const ColumnVector<UInt16> &, const DataTypePtr &);
template ColumnDictionary::MutablePtr DictionaryEncodingEngine::encodeIntegerColumn<UInt32>(const ColumnVector<UInt32> &, const DataTypePtr &);
template ColumnDictionary::MutablePtr DictionaryEncodingEngine::encodeIntegerColumn<UInt64>(const ColumnVector<UInt64> &, const DataTypePtr &);

} // namespace DB::DM
