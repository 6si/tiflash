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

#include <Columns/ColumnConst.h>
#include <Columns/ColumnDictionary.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/ShreddedAttachmentCache.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/VectorWriter.h>
#include <Storages/DeltaMerge/Encoded/EncodedFilter.h>
#include <Storages/DeltaMerge/JsonShredding/JsonShredder.h>
#include <Storages/DeltaMerge/JsonShredding/JsonShreddedStore.h>
#include <TiDB/Decode/JsonBinary.h>
#include <TiDB/Decode/JsonPathExprRef.h>
#include <common/logger_useful.h>

namespace DB
{

/**
 * FunctionJsonShreddedFilter: Fused json_extract + comparison operator.
 *
 * Instead of:  json_extract(col, '$.event') → binary JSON → equals('purchase')
 * This does:   json_shredded_filter(col, '$.event', '=', binary_json_const) → UInt8
 *
 * For dictionary-encoded sub-columns, uses EncodedFilter (evaluates predicate
 * against dictionary entries once, then scans IDs with integer lookup).
 * For raw string sub-columns, does direct string comparison without JSON encoding.
 *
 * Falls back to normal path (json_extract + compare) when:
 *   - Column doesn't have a shredded attachment
 *   - Sub-column path not found in sidecar
 *   - Shredding is disabled for this query
 */
class FunctionJsonShreddedFilter : public IFunction
{
public:
    static constexpr auto name = "json_shredded_filter";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionJsonShreddedFilter>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 4; }
    bool useDefaultImplementationForConstants() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return makeNullable(std::make_shared<DataTypeUInt8>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        // Arguments: (json_col, path_const, op_const, value_const)
        // json_col: the raw JSON column (with potential shredded attachment on Block)
        // path_const: string like "$.event"
        // op_const: string like "eq", "ne", "like"
        // value_const: the binary JSON constant to compare against

        const size_t rows = block.rows();

        // Extract constants
        const auto * path_col = typeid_cast<const ColumnConst *>(block.getByPosition(arguments[1]).column.get());
        const auto * op_col = typeid_cast<const ColumnConst *>(block.getByPosition(arguments[2]).column.get());
        const auto * value_col = typeid_cast<const ColumnConst *>(block.getByPosition(arguments[3]).column.get());

        if (!path_col || !op_col || !value_col)
        {
            setAllNull(block, result, rows);
            return;
        }

        String path = path_col->getValue<String>();
        String op = op_col->getValue<String>();
        String value_binary_json = value_col->getValue<String>();

        // Strip "$." prefix from path for sidecar lookup
        String lookup_path = path;
        if (lookup_path.size() > 2 && lookup_path[0] == '$' && lookup_path[1] == '.')
            lookup_path = lookup_path.substr(2);

        // Get the shredded attachment from the Block's ColumnWithTypeAndName
        const auto & json_col_ref = block.getByPosition(arguments[0]);

        // Try to get attachment: first from the column itself, then from global cache.
        // The attachment may be lost during pipeline cross-thread handoff (read thread → pipeline thread)
        // because block reconstruction in operator chains doesn't always preserve custom fields.
        DM::ColumnShreddedAttachmentPtr attach_ptr = json_col_ref.shredded_attachment;
        // is_ngc sentinel means this column has real blob data — fall back to blob extraction.
        if (attach_ptr && attach_ptr->is_ngc)
        {
            block.getByPosition(result).column = blobFallbackFilter(json_col_ref, path, op, value_binary_json, rows);
            return;
        }
        if (!attach_ptr)
        {
            // Fallback: look up from global thread-safe cache by column name.
            attach_ptr = DM::ShreddedAttachmentCache::instance().findByColName(json_col_ref.name);
            // Ignore NGC sentinels from cache — they belong to a different DMFile's blob column.
            if (attach_ptr && attach_ptr->is_ngc)
                attach_ptr = nullptr;
        }

        if (!attach_ptr)
        {
            setAllNull(block, result, rows);
            return;
        }

        const auto & attach = *attach_ptr;
        if (!attach.isLazy() && !attach.data)
        {
            setAllNull(block, result, rows);
            return;
        }

        if (!attach.hasPath(lookup_path))
        {
            setAllNull(block, result, rows);
            return;
        }

        // Load the sub-column on demand (lazy mode)
        ColumnPtr sub_col;
        if (attach.isLazy())
        {
            auto full_col = DM::JsonShreddedStore::readSidecarColumn(
                attach.dmfile_path,
                attach.col_name,
                lookup_path);
            if (!full_col)
            {
                setAllNull(block, result, rows);
                return;
            }
            if (full_col->size() == attach.row_count)
                sub_col = full_col;
            else if (full_col->size() >= attach.row_offset + attach.row_count)
                sub_col = full_col->cut(attach.row_offset, attach.row_count);
            else
            {
                setAllNull(block, result, rows);
                return;
            }
        }
        else
        {
            // Eager mode (legacy): look up from pre-loaded data
            auto full_col = DM::JsonSubColumnReader::readPath(*attach.data, lookup_path);
            if (!full_col)
            {
                setAllNull(block, result, rows);
                return;
            }
            if (full_col->size() == attach.row_count)
                sub_col = full_col;
            else if (full_col->size() >= attach.row_offset + attach.row_count)
                sub_col = full_col->cut(attach.row_offset, attach.row_count);
            else
            {
                setAllNull(block, result, rows);
                return;
            }
        }

        // Apply MVCC filter if present (set when DMVersionFilter reduced the block's
        // row count below the sidecar row_count after MVCC filtering).
        if (sub_col && !attach.mvcc_filter.empty() && sub_col->size() == attach.row_count)
        {
            IColumn::Filter filter;
            filter.insert(filter.end(), attach.mvcc_filter.begin(), attach.mvcc_filter.end());
            sub_col = sub_col->filter(filter, rows);
        }

        if (!sub_col || sub_col->size() != rows)
        {
            setAllNull(block, result, rows);
            return;
        }

        // Decode the comparison value from binary JSON
        String compare_value = decodeBinaryJsonToString(value_binary_json);

        // Now perform the filter directly on the sub-column
        const auto * nullable_sub = typeid_cast<const ColumnNullable *>(sub_col.get());
        if (!nullable_sub)
        {
            setAllNull(block, result, rows);
            return;
        }

        const auto & sub_null_map = nullable_sub->getNullMapData();
        const auto & sub_nested = nullable_sub->getNestedColumn();

        // Dictionary-encoded path (fastest: pre-compute per dictionary entry, O(1) per row)
        if (const auto * dict_col = typeid_cast<const ColumnDictionary *>(&sub_nested))
        {
            block.getByPosition(result).column
                = applyDictionaryFilter(*dict_col, sub_null_map, op, compare_value, rows);
            return;
        }

        // Raw string comparison (no JSON encoding needed)
        if (const auto * str_col = typeid_cast<const ColumnString *>(&sub_nested))
        {
            block.getByPosition(result).column
                = applyStringFilter(*str_col, sub_null_map, op, compare_value, rows);
            return;
        }

        // Int64 comparison
        if (const auto * int_col = typeid_cast<const ColumnInt64 *>(&sub_nested))
        {
            block.getByPosition(result).column
                = applyInt64Filter(*int_col, sub_null_map, op, compare_value, rows);
            return;
        }

        // Float64 comparison
        if (const auto * float_col = typeid_cast<const ColumnFloat64 *>(&sub_nested))
        {
            block.getByPosition(result).column
                = applyFloat64Filter(*float_col, sub_null_map, op, compare_value, rows);
            return;
        }

        // UInt64 comparison
        if (const auto * uint_col = typeid_cast<const ColumnUInt64 *>(&sub_nested))
        {
            block.getByPosition(result).column
                = applyUInt64Filter(*uint_col, sub_null_map, op, compare_value, rows);
            return;
        }

        // Unsupported type — fall back
        setAllNull(block, result, rows);
    }

private:
    enum class FilterOp : UInt8 { EQ, NE, GT, GE, LT, LE, LIKE, UNKNOWN };

    static FilterOp parseOp(const String & op)
    {
        if (op == "eq") return FilterOp::EQ;
        if (op == "ne") return FilterOp::NE;
        if (op == "gt") return FilterOp::GT;
        if (op == "ge") return FilterOp::GE;
        if (op == "lt") return FilterOp::LT;
        if (op == "le") return FilterOp::LE;
        if (op == "like") return FilterOp::LIKE;
        return FilterOp::UNKNOWN;
    }

    /// Fallback for NGC DMFile blocks (no sidecar): extract the JSON path from the real
    /// blob column and compare row-by-row. Slower than sidecar path but always correct.
    static ColumnPtr blobFallbackFilter(
        const ColumnWithTypeAndName & json_col_ref,
        const String & path,
        const String & op,
        const String & value_binary_json,
        size_t rows)
    {
        auto result_data = ColumnUInt8::create(rows, 0);
        auto result_null = ColumnUInt8::create(rows, 1); // default: null (no match)
        auto & data = result_data->getData();
        auto & nulls = result_null->getData();

        String compare_value = decodeBinaryJsonToString(value_binary_json);

        // Unwrap Nullable wrapper to get the raw string column
        const IColumn * raw_col = json_col_ref.column.get();
        const ColumnNullable * nullable_col = typeid_cast<const ColumnNullable *>(raw_col);
        if (nullable_col)
            raw_col = &nullable_col->getNestedColumn();
        const auto * str_col = typeid_cast<const ColumnString *>(raw_col);
        if (!str_col)
            return ColumnNullable::create(std::move(result_data), std::move(result_null));

        auto path_exprs = DB::buildPathExprContainer(StringRef(path.data(), path.size()));
        const auto filter_op = parseOp(op);

        for (size_t i = 0; i < rows; ++i)
        {
            if (nullable_col && nullable_col->isNullAt(i))
                continue; // remains null

            auto json_ref = str_col->getDataAt(i);
            if (json_ref.size < 1)
                continue;
            JsonBinary json_bin(static_cast<UInt8>(json_ref.data[0]), StringRef(json_ref.data + 1, json_ref.size - 1));

            ColumnString::Chars_t buf;
            buf.reserve(64);
            {
                JsonBinary::JsonBinaryWriteBuffer json_write_buf(buf);
                bool matched = json_bin.extract(path_exprs, json_write_buf);
                if (!matched)
                    continue;
            }
            if (buf.empty())
                continue;

            String extracted(reinterpret_cast<const char *>(buf.data()), buf.size());
            String extracted_str = decodeBinaryJsonToString(extracted);

            nulls[i] = 0;
            switch (filter_op)
            {
            case FilterOp::EQ: data[i] = (extracted_str == compare_value) ? 1 : 0; break;
            case FilterOp::NE: data[i] = (extracted_str != compare_value) ? 1 : 0; break;
            case FilterOp::GT: data[i] = (jsonStringCompare(extracted_str, compare_value) > 0) ? 1 : 0; break;
            case FilterOp::GE: data[i] = (jsonStringCompare(extracted_str, compare_value) >= 0) ? 1 : 0; break;
            case FilterOp::LT: data[i] = (jsonStringCompare(extracted_str, compare_value) < 0) ? 1 : 0; break;
            case FilterOp::LE: data[i] = (jsonStringCompare(extracted_str, compare_value) <= 0) ? 1 : 0; break;
            default: nulls[i] = 1; break;
            }
        }

        return ColumnNullable::create(std::move(result_data), std::move(result_null));
    }

    static void setAllNull(Block & block, size_t result, size_t rows)
    {
        auto data_col = ColumnUInt8::create(rows, 0);
        auto null_map = ColumnUInt8::create(rows, 1);
        block.getByPosition(result).column = ColumnNullable::create(std::move(data_col), std::move(null_map));
    }

    /// Compare two strings using JSON string comparison semantics.
    /// In binary JSON, strings are stored as [varint_length, bytes...].
    /// Comparing binary representations means length is compared first.
    static int jsonStringCompare(const String & a, const String & b)
    {
        if (a.size() != b.size())
            return a.size() < b.size() ? -1 : 1;
        return a.compare(b);
    }

    static int jsonStringCompare(StringRef a, const String & b)
    {
        if (a.size != b.size())
            return a.size < b.size() ? -1 : 1;
        return memcmp(a.data, b.data(), a.size);
    }

    static String decodeBinaryJsonToString(const String & binary_json)
    {
        if (binary_json.empty())
            return "";

        auto type_code = static_cast<UInt8>(binary_json[0]);
        if (type_code == JsonBinary::TYPE_CODE_STRING && binary_json.size() > 1)
        {
            // Decode varint length then extract string
            size_t pos = 1;
            UInt64 len = 0;
            UInt32 shift = 0;
            while (pos < binary_json.size())
            {
                auto byte = static_cast<UInt8>(binary_json[pos]);
                len |= static_cast<UInt64>(byte & 0x7F) << shift;
                ++pos;
                if ((byte & 0x80) == 0)
                    break;
                shift += 7;
            }
            if (pos + len <= binary_json.size())
                return binary_json.substr(pos, len);
        }
        else if (type_code == JsonBinary::TYPE_CODE_LITERAL)
        {
            if (binary_json.size() > 1)
            {
                auto lit = static_cast<UInt8>(binary_json[1]);
                if (lit == JsonBinary::LITERAL_TRUE)
                    return "true";
                if (lit == JsonBinary::LITERAL_FALSE)
                    return "false";
            }
            return "null";
        }
        else if (type_code == JsonBinary::TYPE_CODE_INT64 && binary_json.size() >= 9)
        {
            Int64 val;
            memcpy(&val, binary_json.data() + 1, sizeof(Int64));
            return std::to_string(val);
        }
        else if (type_code == JsonBinary::TYPE_CODE_UINT64 && binary_json.size() >= 9)
        {
            UInt64 val;
            memcpy(&val, binary_json.data() + 1, sizeof(UInt64));
            return std::to_string(val);
        }
        else if (type_code == JsonBinary::TYPE_CODE_FLOAT64 && binary_json.size() >= 9)
        {
            Float64 val;
            memcpy(&val, binary_json.data() + 1, sizeof(Float64));
            return fmt::format("{:.17g}", val);
        }

        // Return raw bytes as fallback
        return binary_json.substr(1);
    }

    static ColumnPtr applyDictionaryFilter(
        const ColumnDictionary & dict_col,
        const ColumnUInt8::Container & null_map,
        const String & op,
        const String & compare_value,
        size_t rows)
    {
        auto result_data = ColumnUInt8::create(rows, 0);
        auto result_null = ColumnUInt8::create(rows, 0);
        auto & data = result_data->getData();
        auto & nulls = result_null->getData();

        Field compare_field(compare_value);

        if (op == "eq")
        {
            auto fr = DM::EncodedFilter::evaluateEquals(dict_col, compare_field);
            for (size_t i = 0; i < rows; ++i)
            {
                if (null_map[i])
                {
                    nulls[i] = 1;
                    continue;
                }
                data[i] = fr.filter[i];
            }
        }
        else if (op == "ne")
        {
            auto fr = DM::EncodedFilter::evaluateNotEquals(dict_col, compare_field);
            for (size_t i = 0; i < rows; ++i)
            {
                if (null_map[i])
                {
                    nulls[i] = 1;
                    continue;
                }
                data[i] = fr.filter[i];
            }
        }
        else if (op == "like")
        {
            auto fr = DM::EncodedFilter::evaluateLike(dict_col, compare_value);
            for (size_t i = 0; i < rows; ++i)
            {
                if (null_map[i])
                {
                    nulls[i] = 1;
                    continue;
                }
                data[i] = fr.filter[i];
            }
        }
        else if (op == "gt" || op == "ge" || op == "lt" || op == "le")
        {
            auto predicate = [&](const Field & entry) -> bool {
                const auto & entry_str = entry.get<String>();
                int cmp = jsonStringCompare(entry_str, compare_value);
                if (op == "gt")
                    return cmp > 0;
                if (op == "ge")
                    return cmp >= 0;
                if (op == "lt")
                    return cmp < 0;
                return cmp <= 0; // le
            };
            auto fr = DM::EncodedFilter::evaluatePredicate(dict_col, predicate);
            for (size_t i = 0; i < rows; ++i)
            {
                if (null_map[i])
                {
                    nulls[i] = 1;
                    continue;
                }
                data[i] = fr.filter[i];
            }
        }
        else
        {
            // Unknown op — all null
            for (size_t i = 0; i < rows; ++i)
                nulls[i] = 1;
        }

        return ColumnNullable::create(std::move(result_data), std::move(result_null));
    }

    static ColumnPtr applyStringFilter(
        const ColumnString & str_col,
        const ColumnUInt8::Container & null_map,
        const String & op,
        const String & compare_value,
        size_t rows)
    {
        auto result_data = ColumnUInt8::create(rows, 0);
        auto result_null = ColumnUInt8::create(rows, 0);
        auto & data = result_data->getData();
        auto & nulls = result_null->getData();

        StringRef compare_ref(compare_value.data(), compare_value.size());
        const auto filter_op = parseOp(op);

        for (size_t i = 0; i < rows; ++i)
        {
            if (null_map[i])
            {
                nulls[i] = 1;
                continue;
            }

            StringRef row_val = str_col.getDataAt(i);
            switch (filter_op)
            {
            case FilterOp::EQ: data[i] = (row_val == compare_ref) ? 1 : 0; break;
            case FilterOp::NE: data[i] = (row_val != compare_ref) ? 1 : 0; break;
            case FilterOp::GT: data[i] = (jsonStringCompare(row_val, compare_value) > 0) ? 1 : 0; break;
            case FilterOp::GE: data[i] = (jsonStringCompare(row_val, compare_value) >= 0) ? 1 : 0; break;
            case FilterOp::LT: data[i] = (jsonStringCompare(row_val, compare_value) < 0) ? 1 : 0; break;
            case FilterOp::LE: data[i] = (jsonStringCompare(row_val, compare_value) <= 0) ? 1 : 0; break;
            case FilterOp::LIKE:
                data[i] = DM::EncodedFilter::matchLike(
                              String(row_val.data, row_val.size),
                              compare_value)
                              ? 1 : 0;
                break;
            default: nulls[i] = 1; break;
            }
        }

        return ColumnNullable::create(std::move(result_data), std::move(result_null));
    }

    static ColumnPtr applyInt64Filter(
        const ColumnInt64 & int_col,
        const ColumnUInt8::Container & null_map,
        const String & op,
        const String & compare_value,
        size_t rows)
    {
        auto result_data = ColumnUInt8::create(rows, 0);
        auto result_null = ColumnUInt8::create(rows, 0);
        auto & data = result_data->getData();
        auto & nulls = result_null->getData();

        Int64 cmp_val = 0;
        try
        {
            cmp_val = std::stoll(compare_value);
        }
        catch (...)
        {
            for (size_t i = 0; i < rows; ++i)
                nulls[i] = 1;
            return ColumnNullable::create(std::move(result_data), std::move(result_null));
        }

        const auto & int_data = int_col.getData();
        const auto filter_op = parseOp(op);
        for (size_t i = 0; i < rows; ++i)
        {
            if (null_map[i])
            {
                nulls[i] = 1;
                continue;
            }

            switch (filter_op)
            {
            case FilterOp::EQ: data[i] = (int_data[i] == cmp_val) ? 1 : 0; break;
            case FilterOp::NE: data[i] = (int_data[i] != cmp_val) ? 1 : 0; break;
            case FilterOp::LT: data[i] = (int_data[i] < cmp_val) ? 1 : 0; break;
            case FilterOp::LE: data[i] = (int_data[i] <= cmp_val) ? 1 : 0; break;
            case FilterOp::GT: data[i] = (int_data[i] > cmp_val) ? 1 : 0; break;
            case FilterOp::GE: data[i] = (int_data[i] >= cmp_val) ? 1 : 0; break;
            default: nulls[i] = 1; break;
            }
        }

        return ColumnNullable::create(std::move(result_data), std::move(result_null));
    }

    static ColumnPtr applyFloat64Filter(
        const ColumnFloat64 & float_col,
        const ColumnUInt8::Container & null_map,
        const String & op,
        const String & compare_value,
        size_t rows)
    {
        auto result_data = ColumnUInt8::create(rows, 0);
        auto result_null = ColumnUInt8::create(rows, 0);
        auto & data = result_data->getData();
        auto & nulls = result_null->getData();

        Float64 cmp_val = 0;
        try
        {
            cmp_val = std::stod(compare_value);
        }
        catch (...)
        {
            for (size_t i = 0; i < rows; ++i)
                nulls[i] = 1;
            return ColumnNullable::create(std::move(result_data), std::move(result_null));
        }

        const auto & float_data = float_col.getData();
        const auto filter_op = parseOp(op);
        for (size_t i = 0; i < rows; ++i)
        {
            if (null_map[i])
            {
                nulls[i] = 1;
                continue;
            }

            switch (filter_op)
            {
            case FilterOp::EQ: data[i] = (float_data[i] == cmp_val) ? 1 : 0; break;
            case FilterOp::NE: data[i] = (float_data[i] != cmp_val) ? 1 : 0; break;
            case FilterOp::LT: data[i] = (float_data[i] < cmp_val) ? 1 : 0; break;
            case FilterOp::LE: data[i] = (float_data[i] <= cmp_val) ? 1 : 0; break;
            case FilterOp::GT: data[i] = (float_data[i] > cmp_val) ? 1 : 0; break;
            case FilterOp::GE: data[i] = (float_data[i] >= cmp_val) ? 1 : 0; break;
            default: nulls[i] = 1; break;
            }
        }

        return ColumnNullable::create(std::move(result_data), std::move(result_null));
    }

    static ColumnPtr applyUInt64Filter(
        const ColumnUInt64 & uint_col,
        const ColumnUInt8::Container & null_map,
        const String & op,
        const String & compare_value,
        size_t rows)
    {
        auto result_data = ColumnUInt8::create(rows, 0);
        auto result_null = ColumnUInt8::create(rows, 0);
        auto & data = result_data->getData();
        auto & nulls = result_null->getData();

        UInt64 cmp_val = 0;
        try
        {
            cmp_val = std::stoull(compare_value);
        }
        catch (...)
        {
            for (size_t i = 0; i < rows; ++i)
                nulls[i] = 1;
            return ColumnNullable::create(std::move(result_data), std::move(result_null));
        }

        const auto & uint_data = uint_col.getData();
        const auto filter_op = parseOp(op);
        for (size_t i = 0; i < rows; ++i)
        {
            if (null_map[i])
            {
                nulls[i] = 1;
                continue;
            }

            switch (filter_op)
            {
            case FilterOp::EQ: data[i] = (uint_data[i] == cmp_val) ? 1 : 0; break;
            case FilterOp::NE: data[i] = (uint_data[i] != cmp_val) ? 1 : 0; break;
            case FilterOp::LT: data[i] = (uint_data[i] < cmp_val) ? 1 : 0; break;
            case FilterOp::LE: data[i] = (uint_data[i] <= cmp_val) ? 1 : 0; break;
            case FilterOp::GT: data[i] = (uint_data[i] > cmp_val) ? 1 : 0; break;
            case FilterOp::GE: data[i] = (uint_data[i] >= cmp_val) ? 1 : 0; break;
            default: nulls[i] = 1; break;
            }
        }

        return ColumnNullable::create(std::move(result_data), std::move(result_null));
    }
};

} // namespace DB
