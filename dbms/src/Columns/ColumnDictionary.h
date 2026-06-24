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
#include <Common/Arena.h>
#include <Common/HashTable/Hash.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>
#include <Common/WeakHash.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <common/StringRef.h>
#include <common/likely.h>

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
        , ids()
        , value_type(src.value_type)
    {
        ids.reserve(src.ids.size());
        for (size_t i = 0; i < src.ids.size(); ++i)
            ids.push_back(src.ids[i]);
    }

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

    const char * getFamilyName() const override { return "String"; }

    bool canBeInsideNullable() const override { return true; }

    bool isDictionaryEncoded() const override { return true; }

    IColumn::Ptr convertToFullColumnIfDictionary() const override { return decode(); }

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

    /// Try to auto-encode a ColumnString into a ColumnDictionary.
    /// Returns ColumnDictionary if cardinality <= max_dict_size, otherwise returns
    /// the original column unchanged. Handles Nullable(ColumnString) too.
    static ColumnPtr tryAutoEncode(
        const ColumnPtr & column,
        size_t min_rows = 256,
        UInt32 max_dict_size = 65536);

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

    StringRef getDataAt(size_t n) const override
    {
        assert(n < ids.size());
        const auto & val = dictionary[ids[n]];
        const auto & s = val.get<String>();
        return StringRef(s.data(), s.size());
    }

    void insertData(const char * pos, size_t length) override
    {
        String val(pos, length);
        UInt32 id = findOrAddEntry(Field(std::move(val)));
        ids.push_back(id);
    }

    void insert(const Field & x) override
    {
        UInt32 id = findOrAddEntry(x);
        ids.push_back(id);
    }

    void insertDefault() override { ids.push_back(0); }

    void insertFrom(const IColumn & src, size_t n) override
    {
        const auto * src_dict = typeid_cast<const ColumnDictionary *>(&src);
        if (src_dict)
        {
            Field val = src_dict->getDictionary()[src_dict->getDictionaryIds()[n]];
            UInt32 id = findOrAddEntry(val);
            ids.push_back(id);
        }
        else
        {
            Field val;
            src.get(n, val);
            UInt32 id = findOrAddEntry(val);
            ids.push_back(id);
        }
    }

    void popBack(size_t n) override { ids.resize_assume_reserved(ids.size() - n); }

    StringRef serializeValueIntoArena(
        size_t n,
        Arena & arena,
        char const *& begin,
        const TiDB::TiDBCollatorPtr & collator,
        String & sort_key_container) const override
    {
        assert(n < ids.size());
        const auto & s = dictionary[ids[n]].get<String>();
        const void * src = s.data();
        size_t string_size = s.size() + 1; // include trailing zero like ColumnString

        StringRef res;
        if (likely(collator != nullptr))
        {
            auto sort_key = collator->sortKeyFastPath(s.data(), s.size(), sort_key_container);
            string_size = sort_key.size;
            src = sort_key.data;
        }
        res.size = sizeof(string_size) + string_size;
        char * pos = arena.allocContinue(res.size, begin);
        std::memcpy(pos, &string_size, sizeof(string_size));
        if (string_size > 0)
            std::memcpy(pos + sizeof(string_size), src, string_size);
        res.data = pos;
        return res;
    }

    const char * deserializeAndInsertFromArena(const char * pos, const TiDB::TiDBCollatorPtr &) override
    {
        const size_t string_size = *reinterpret_cast<const size_t *>(pos);
        pos += sizeof(string_size);
        insertData(pos, string_size);
        return pos + string_size;
    }

    void updateHashWithValue(
        size_t n,
        SipHash & hash,
        const TiDB::TiDBCollatorPtr & collator,
        String & sort_key_container) const override
    {
        assert(n < ids.size());
        const auto & s = dictionary[ids[n]].get<String>();
        if (likely(collator != nullptr))
        {
            auto sort_key = collator->sortKeyFastPath(s.data(), s.size(), sort_key_container);
            size_t key_size = sort_key.size;
            hash.update(reinterpret_cast<const char *>(&key_size), sizeof(key_size));
            hash.update(sort_key.data, sort_key.size);
        }
        else
        {
            size_t str_size = s.size() + 1; // trailing zero
            hash.update(reinterpret_cast<const char *>(&str_size), sizeof(str_size));
            hash.update(s.data(), s.size() + 1);
        }
    }

    void updateHashWithValues(
        IColumn::HashValues & hash_values,
        const TiDB::TiDBCollatorPtr & collator,
        String & sort_key_container) const override
    {
        for (size_t i = 0; i < ids.size(); ++i)
            updateHashWithValue(i, hash_values[i], collator, sort_key_container);
    }

    void updateWeakHash32(
        WeakHash32 & hash,
        const TiDB::TiDBCollatorPtr & collator,
        String & sort_key_container) const override
    {
        auto & hash_data = hash.getData();
        // Pre-compute K hashes for dictionary entries, then N lookups by ID.
        // For K=5 and N=100M: 5 hashes + 100M array lookups (~50ms)
        // vs N hashes (~500ms) without pre-computation.
        size_t K = dictionary.size();
        std::vector<UInt32> dict_hashes(K);
        UInt32 seed = 0; // initial seed for first column in hash chain
        for (size_t d = 0; d < K; ++d)
        {
            const auto & s = dictionary[d].get<String>();
            if (likely(collator != nullptr))
            {
                auto sort_key = collator->sortKeyFastPath(s.data(), s.size(), sort_key_container);
                dict_hashes[d] = ::updateWeakHash32(
                    reinterpret_cast<const UInt8 *>(sort_key.data),
                    sort_key.size,
                    seed);
            }
            else
            {
                dict_hashes[d] = ::updateWeakHash32(
                    reinterpret_cast<const UInt8 *>(s.data()),
                    s.size(),
                    seed);
            }
        }
        // Check if hash_data has non-zero seeds (chained from previous columns)
        bool has_chain = false;
        for (size_t i = 0; i < std::min<size_t>(ids.size(), 4); ++i)
        {
            if (hash_data[i] != 0) { has_chain = true; break; }
        }
        if (has_chain)
        {
            // Chained: must combine pre-computed hash with existing seed per row
            for (size_t i = 0; i < ids.size(); ++i)
            {
                const auto & s = dictionary[ids[i]].get<String>();
                if (likely(collator != nullptr))
                {
                    auto sort_key = collator->sortKeyFastPath(s.data(), s.size(), sort_key_container);
                    hash_data[i] = ::updateWeakHash32(
                        reinterpret_cast<const UInt8 *>(sort_key.data),
                        sort_key.size,
                        hash_data[i]);
                }
                else
                {
                    hash_data[i] = ::updateWeakHash32(
                        reinterpret_cast<const UInt8 *>(s.data()),
                        s.size(),
                        hash_data[i]);
                }
            }
        }
        else
        {
            // No chain: use pre-computed dict hashes directly via ID lookup
            for (size_t i = 0; i < ids.size(); ++i)
                hash_data[i] = dict_hashes[ids[i]];
        }
    }

    void updateWeakHash32(
        WeakHash32 & hash,
        const TiDB::TiDBCollatorPtr & collator,
        String & sort_key_container,
        const BlockSelective & selective) const override
    {
        auto & hash_data = hash.getData();
        for (size_t idx = 0; idx < selective.size(); ++idx)
        {
            size_t i = selective[idx];
            const auto & s = dictionary[ids[i]].get<String>();
            if (likely(collator != nullptr))
            {
                auto sort_key = collator->sortKeyFastPath(s.data(), s.size(), sort_key_container);
                hash_data[idx] = ::updateWeakHash32(
                    reinterpret_cast<const UInt8 *>(sort_key.data),
                    sort_key.size,
                    hash_data[idx]);
            }
            else
            {
                hash_data[idx] = ::updateWeakHash32(
                    reinterpret_cast<const UInt8 *>(s.data()),
                    s.size(),
                    hash_data[idx]);
            }
        }
    }

    void insertRangeFrom(const IColumn & src, size_t start, size_t length) override
    {
        const auto * src_dict = typeid_cast<const ColumnDictionary *>(&src);
        if (src_dict && &src_dict->getDictionary() == &dictionary)
        {
            const auto & src_ids = src_dict->getDictionaryIds();
            ids.insert(src_ids.begin() + start, src_ids.begin() + start + length);
        }
        else
        {
            for (size_t i = start; i < start + length; ++i)
            {
                Field val;
                src.get(i, val);
                ids.push_back(findOrAddEntry(val));
            }
        }
    }

    void insertManyFrom(const IColumn & src, size_t position, size_t length) override
    {
        const auto * src_dict = typeid_cast<const ColumnDictionary *>(&src);
        if (src_dict && &src_dict->getDictionary() == &dictionary)
        {
            UInt32 id_val = src_dict->getDictionaryIds()[position];
            for (size_t i = 0; i < length; ++i)
                ids.push_back(id_val);
        }
        else
        {
            Field val;
            src.get(position, val);
            UInt32 id = findOrAddEntry(val);
            for (size_t i = 0; i < length; ++i)
                ids.push_back(id);
        }
    }

    void insertDisjunctFrom(const IColumn & src, const std::vector<size_t> & position_vec)
    {
        const auto * src_dict = typeid_cast<const ColumnDictionary *>(&src);
        if (src_dict && &src_dict->getDictionary() == &dictionary)
        {
            const auto & src_ids = src_dict->getDictionaryIds();
            for (auto pos : position_vec)
                ids.push_back(src_ids[pos]);
        }
        else
        {
            for (auto pos : position_vec)
            {
                Field val;
                src.get(pos, val);
                ids.push_back(findOrAddEntry(val));
            }
        }
    }

    void insertManyDefaults(size_t length) override
    {
        for (size_t i = 0; i < length; ++i)
            ids.push_back(0);
    }

    ColumnPtr filter(const Filter & filt, ssize_t result_size_hint) const override;
    ColumnPtr permute(const Permutation & perm, size_t limit) const override;

    int compareAt(size_t n, size_t m, const IColumn & rhs, int /*nan_direction_hint*/) const override
    {
        const auto & lhs_val = dictionary[ids[n]].get<String>();
        const auto * rhs_dict = typeid_cast<const ColumnDictionary *>(&rhs);
        if (rhs_dict)
        {
            const auto & rhs_val = rhs_dict->getDictionary()[rhs_dict->getDictionaryIds()[m]].get<String>();
            return lhs_val.compare(rhs_val);
        }
        StringRef rhs_ref = rhs.getDataAt(m);
        return lhs_val.compare(0, std::string::npos, rhs_ref.data, rhs_ref.size);
    }

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res)
        const override
    {
        auto materialized = decode();
        materialized->getPermutation(reverse, limit, nan_direction_hint, res);
    }

    ColumnPtr replicateRange(size_t start_row, size_t end_row, const IColumn::Offsets & offsets) const override
    {
        auto materialized = decode();
        return materialized->replicateRange(start_row, end_row, offsets);
    }

    MutableColumnPtr cloneResized(size_t new_size) const override
    {
        PaddedPODArray<UInt32> new_ids;
        size_t copy_count = std::min(ids.size(), new_size);
        new_ids.reserve(new_size);
        for (size_t i = 0; i < copy_count; ++i)
            new_ids.push_back(ids[i]);
        new_ids.resize(new_size);
        return ColumnDictionary::createMutable(dictionary, std::move(new_ids), value_type);
    }

    size_t byteSize() const override { return ids.size() * sizeof(UInt32) + dictionary.size() * 16; }

    size_t allocatedBytes() const override { return ids.allocated_bytes() + dictionary.capacity() * 16; }

    void forEachSubcolumn(ColumnCallback) override {}

    ScatterColumns scatter(ColumnIndex num_columns, const Selector & selector) const override
    {
        auto materialized = decode();
        return materialized->scatter(num_columns, selector);
    }

    ScatterColumns scatter(ColumnIndex num_columns, const Selector & selector, const BlockSelective & selective) const override
    {
        auto materialized = decode();
        return materialized->scatter(num_columns, selector, selective);
    }

    void scatterTo(ScatterColumns & columns, const Selector & selector) const override
    {
        auto materialized = decode();
        materialized->scatterTo(columns, selector);
    }

    void scatterTo(ScatterColumns & columns, const Selector & selector, const BlockSelective & selective) const override
    {
        auto materialized = decode();
        materialized->scatterTo(columns, selector, selective);
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

    // --- Pure virtual stubs required by master IColumn interface ---

    void insertSelectiveRangeFrom(
        const IColumn & /*src*/,
        const Offsets & /*selective_offsets*/,
        size_t /*start*/,
        size_t /*length*/) override
    {
        throw Exception("insertSelectiveRangeFrom not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    size_t serializeByteSize() const override
    {
        return decode()->serializeByteSize();
    }

    void countSerializeByteSize(PaddedPODArray<size_t> & byte_size) const override
    {
        decode()->countSerializeByteSize(byte_size);
    }

    void countSerializeByteSizeForCmp(
        PaddedPODArray<size_t> & byte_size,
        const NullMap * nullmap,
        const TiDB::TiDBCollatorPtr & collator) const override
    {
        decode()->countSerializeByteSizeForCmp(byte_size, nullmap, collator);
    }

    void countSerializeByteSizeForColumnArray(
        PaddedPODArray<size_t> & byte_size,
        const Offsets & array_offsets) const override
    {
        decode()->countSerializeByteSizeForColumnArray(byte_size, array_offsets);
    }

    void countSerializeByteSizeForCmpColumnArray(
        PaddedPODArray<size_t> & byte_size,
        const Offsets & array_offsets,
        const NullMap * nullmap,
        const TiDB::TiDBCollatorPtr & collator) const override
    {
        decode()->countSerializeByteSizeForCmpColumnArray(byte_size, array_offsets, nullmap, collator);
    }

    void serializeToPos(
        PaddedPODArray<char *> & pos,
        size_t start,
        size_t length,
        bool has_null) const override
    {
        decode()->serializeToPos(pos, start, length, has_null);
    }

    void serializeToPosForCmp(
        PaddedPODArray<char *> & pos,
        size_t start,
        size_t length,
        bool has_null,
        const NullMap * nullmap,
        const TiDB::TiDBCollatorPtr & collator,
        String * sort_key_container) const override
    {
        decode()->serializeToPosForCmp(pos, start, length, has_null, nullmap, collator, sort_key_container);
    }

    void serializeToPosForColumnArray(
        PaddedPODArray<char *> & pos,
        size_t start,
        size_t length,
        bool has_null,
        const Offsets & array_offsets) const override
    {
        decode()->serializeToPosForColumnArray(pos, start, length, has_null, array_offsets);
    }

    void serializeToPosForCmpColumnArray(
        PaddedPODArray<char *> & pos,
        size_t start,
        size_t length,
        bool has_null,
        const NullMap * nullmap,
        const Offsets & array_offsets,
        const TiDB::TiDBCollatorPtr & collator,
        String * sort_key_container) const override
    {
        decode()->serializeToPosForCmpColumnArray(pos, start, length, has_null, nullmap, array_offsets, collator, sort_key_container);
    }

    void deserializeAndInsertFromPos(PaddedPODArray<char *> & /*pos*/, bool /*use_nt_align_buffer*/) override
    {
        throw Exception("deserializeAndInsertFromPos not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void deserializeAndInsertFromPosForColumnArray(
        PaddedPODArray<char *> & /*pos*/,
        const Offsets & /*array_offsets*/,
        bool /*use_nt_align_buffer*/) override
    {
        throw Exception("deserializeAndInsertFromPosForColumnArray not supported for ColumnDictionary", ErrorCodes::NOT_IMPLEMENTED);
    }

    void flushNTAlignBuffer() override {}

    void deserializeAndAdvancePos(PaddedPODArray<char *> & pos) const override
    {
        decode()->deserializeAndAdvancePos(pos);
    }

    void deserializeAndAdvancePosForColumnArray(
        PaddedPODArray<char *> & pos,
        const Offsets & array_offsets) const override
    {
        decode()->deserializeAndAdvancePosForColumnArray(pos, array_offsets);
    }

private:
    UInt32 findOrAddEntry(const Field & val)
    {
        for (UInt32 i = 0; i < dictionary.size(); ++i)
        {
            if (dictionary[i] == val)
                return i;
        }
        dictionary.push_back(val);
        return static_cast<UInt32>(dictionary.size() - 1);
    }
};

} // namespace DB
