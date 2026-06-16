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

#include <Columns/IColumn.h>
#include <Common/PODArray.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <common/StringRef.h>

#include <cassert>
#include <vector>

namespace DB
{

/**
 * @brief ColumnDictionary holds encoded column data using dictionary encoding.
 *
 * Instead of storing raw values, it stores:
 *   - A dictionary (vector of unique values as Field)
 *   - A vector of dictionary IDs (one per row, indexing into the dictionary)
 *
 * This enables downstream operators (filter, group-by, join) to operate directly
 * on dictionary IDs without decoding. When decoding is needed, call `decode()`
 * to produce a regular IColumn.
 */
class ColumnDictionary final : public COWPtrHelper<IColumn, ColumnDictionary>
{
private:
    friend class COWPtrHelper<IColumn, ColumnDictionary>;

    /// Dictionary entries (unique values)
    std::vector<Field> dictionary;

    /// Per-row dictionary IDs
    PaddedPODArray<UInt32> ids;

    /// The data type of the dictionary entries (for decode)
    DataTypePtr value_type;

    ColumnDictionary(std::vector<Field> dictionary_, PaddedPODArray<UInt32> && ids_, DataTypePtr value_type_)
        : dictionary(std::move(dictionary_))
        , ids(std::move(ids_))
        , value_type(std::move(value_type_))
    {}

    /// Copy constructor used by COWPtrHelper::clone()
    ColumnDictionary(const ColumnDictionary & src)
        : dictionary(src.dictionary)
        , ids(src.ids.begin(), src.ids.end())
        , value_type(src.value_type)
    {}

public:
    /// Create a ColumnDictionary from a pre-built dictionary and ID array.
    /// Uses COWPtrHelper::create() which invokes the private constructor via friendship.
    static MutablePtr createMutable(
        std::vector<Field> dictionary_,
        PaddedPODArray<UInt32> && ids_,
        DataTypePtr value_type_)
    {
        return ColumnDictionary::create(std::move(dictionary_), std::move(ids_), std::move(value_type_));
    }

    const char * getFamilyName() const override { return "Dictionary"; }

    size_t size() const override { return ids.size(); }

    /// Access the raw dictionary ID array (for encoded operations)
    const PaddedPODArray<UInt32> & getDictionaryIds() const { return ids; }
    PaddedPODArray<UInt32> & getDictionaryIds() { return ids; }

    /// Access the dictionary entries
    const std::vector<Field> & getDictionary() const { return dictionary; }

    /// Number of distinct values in the dictionary
    size_t getDictionarySize() const { return dictionary.size(); }

    /// The value type of dictionary entries
    DataTypePtr getValueType() const { return value_type; }

    /// Decode this column into a regular column with all values materialized
    ColumnPtr decode() const;

    /// IColumn interface implementation
    Field operator[](size_t n) const override
    {
        assert(n < ids.size());
        return dictionary[ids[n]];
    }

    void get(size_t n, Field & res) const override
    {
        assert(n < ids.size());
        res = dictionary[ids[n]];
    }

    StringRef getDataAt(size_t /*n*/) const override
    {
        throw Exception("getDataAt not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertData(const char * /*pos*/, size_t /*length*/) override
    {
        throw Exception("insertData not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insert(const Field & /*x*/) override
    {
        throw Exception("insert not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertDefault() override { ids.push_back(0); }

    void insertFrom(const IColumn & /*src*/, size_t /*n*/) override
    {
        throw Exception("insertFrom not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void popBack(size_t n) override { ids.resize_assume_reserved(ids.size() - n); }

    StringRef serializeValueIntoArena(
        size_t /*n*/,
        Arena & /*arena*/,
        char const *& /*begin*/,
        const TiDB::TiDBCollatorPtr & /*collator*/,
        String & /*sort_key_container*/) const override
    {
        throw Exception("serializeValueIntoArena not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    const char * deserializeAndInsertFromArena(const char * /*pos*/, const TiDB::TiDBCollatorPtr &) override
    {
        throw Exception(
            "deserializeAndInsertFromArena not supported for ColumnDictionary",
            ErrorCodes::NOT_IMPLEMENTED);
    }

    void updateHashWithValue(size_t /*n*/, SipHash & /*hash*/, const TiDB::TiDBCollatorPtr &, String &) const override
    {
        throw Exception("updateHashWithValue not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void updateHashWithValues(
        IColumn::HashValues & /*hash_values*/,
        const TiDB::TiDBCollatorPtr &,
        String &) const override
    {
        throw Exception("updateHashWithValues not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void updateWeakHash32(WeakHash32 & /*hash*/, const TiDB::TiDBCollatorPtr &, String &) const override
    {
        throw Exception("updateWeakHash32 not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void updateWeakHash32(
        WeakHash32 & /*hash*/,
        const TiDB::TiDBCollatorPtr &,
        String &,
        const BlockSelective &) const override
    {
        throw Exception("updateWeakHash32 not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertRangeFrom(const IColumn & /*src*/, size_t /*start*/, size_t /*length*/) override
    {
        throw Exception("insertRangeFrom not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertManyFrom(const IColumn & /*src*/, size_t /*position*/, size_t /*length*/) override
    {
        throw Exception("insertManyFrom not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertDisjunctFrom(const IColumn & /*src*/, const std::vector<size_t> & /*position_vec*/) override
    {
        throw Exception("insertDisjunctFrom not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void insertManyDefaults(size_t length) override
    {
        for (size_t i = 0; i < length; ++i)
            ids.push_back(0);
    }

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;

    int compareAt(size_t /*n*/, size_t /*m*/, const IColumn & /*rhs*/, int /*nan_direction_hint*/) const override
    {
        throw Exception("compareAt not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void getPermutation(bool /*reverse*/, size_t /*limit*/, int /*nan_direction_hint*/, Permutation & /*res*/)
        const override
    {
        throw Exception("getPermutation not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    ColumnPtr replicateRange(size_t /*start_row*/, size_t /*end_row*/, const IColumn::Offsets & /*offsets*/) const override
    {
        throw Exception("replicateRange not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    MutableColumnPtr cloneResized(size_t new_size) const override
    {
        PaddedPODArray<UInt32> new_ids(ids.begin(), ids.end());
        new_ids.resize(new_size);
        return ColumnDictionary::createMutable(dictionary, std::move(new_ids), value_type);
    }

    size_t byteSize() const override { return ids.size() * sizeof(UInt32) + dictionary.size() * 16; }

    size_t allocatedBytes() const override { return ids.allocated_bytes() + dictionary.capacity() * 16; }

    void forEachSubcolumn(ColumnCallback) override {}

    ScatterColumns scatter(ColumnIndex /*num_columns*/, const Selector & /*selector*/) const override
    {
        throw Exception("scatter not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    ScatterColumns scatter(ColumnIndex /*num_columns*/, const Selector & /*selector*/, const BlockSelective & /*selective*/) const override
    {
        throw Exception("scatter not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void scatterTo(ScatterColumns & /*columns*/, const Selector & /*selector*/) const override
    {
        throw Exception("scatterTo not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void scatterTo(ScatterColumns & /*columns*/, const Selector & /*selector*/, const BlockSelective & /*selective*/) const override
    {
        throw Exception("scatterTo not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void gather(ColumnGathererStream & /*gatherer_stream*/) override
    {
        throw Exception("gather not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void getExtremes(Field & min, Field & max) const override
    {
        if (dictionary.empty())
        {
            min = Field();
            max = Field();
            return;
        }
        min = dictionary.front();
        max = dictionary.back();
    }

    /// Whether this column is dictionary-encoded (always true for this type)
    bool isDictionaryEncoded() const { return true; }
};

} // namespace DB
