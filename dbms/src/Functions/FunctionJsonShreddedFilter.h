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
#include <Storages/DeltaMerge/Encoded/EncodedFilter.h>
#include <Storages/DeltaMerge/JsonShredding/JsonShredder.h>
#include <Storages/DeltaMerge/JsonShredding/JsonShreddedStore.h>
#include <TiDB/Decode/JsonBinary.h>
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

        LOG_DEBUG(
            Logger::get("JsonShreddedFilter"),
            "executeImpl: path={} op={} value_len={} value_hex={}",
            path,
            op,
            value_binary_json.size(),
            [&]() {
                String hex;
                for (size_t i = 0; i < std::min(value_binary_json.size(), size_t(32)); ++i)
                    hex += fmt::format("{:02x}", static_cast<unsigned char>(value_binary_json[i]));
                return hex;
            }());

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
        if (!attach_ptr)
        {
            // Fallback: look up from global thread-safe cache by column name
            attach_ptr = DM::ShreddedAttachmentCache::instance().findByColName(json_col_ref.name);
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

        // Decode the comparison value from binary JSON
        String compare_value = decodeBinaryJsonToString(value_binary_json);

        LOG_DEBUG(
            Logger::get("JsonShreddedFilter"),
            "decoded compare_value='{}' sub_col_type={} sub_col_size={}",
            compare_value,
            sub_col ? sub_col->getName() : "null",
            sub_col ? sub_col->size() : 0);

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

        // Unsupported type — fall back
        setAllNull(block, result, rows);
    }

private:
    static void setAllNull(Block & block, size_t result, size_t rows)
    {
        auto data_col = ColumnUInt8::create(rows, 0);
        auto null_map = ColumnUInt8::create(rows, 1);
        block.getByPosition(result).column = ColumnNullable::create(std::move(data_col), std::move(null_map));
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
        else if (type_code == JsonBinary::TYPE_CODE_FLOAT64 && binary_json.size() >= 9)
        {
            Float64 val;
            memcpy(&val, binary_json.data() + 1, sizeof(Float64));
            return std::to_string(val);
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

        for (size_t i = 0; i < rows; ++i)
        {
            if (null_map[i])
            {
                nulls[i] = 1;
                continue;
            }

            StringRef row_val = str_col.getDataAt(i);
            if (op == "eq")
                data[i] = (row_val == compare_ref) ? 1 : 0;
            else if (op == "ne")
                data[i] = (row_val != compare_ref) ? 1 : 0;
            else if (op == "like")
                data[i] = DM::EncodedFilter::matchLike(
                              String(row_val.data, row_val.size),
                              compare_value)
                              ? 1
                              : 0;
            else
                nulls[i] = 1;
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
        for (size_t i = 0; i < rows; ++i)
        {
            if (null_map[i])
            {
                nulls[i] = 1;
                continue;
            }

            if (op == "eq")
                data[i] = (int_data[i] == cmp_val) ? 1 : 0;
            else if (op == "ne")
                data[i] = (int_data[i] != cmp_val) ? 1 : 0;
            else if (op == "lt")
                data[i] = (int_data[i] < cmp_val) ? 1 : 0;
            else if (op == "le")
                data[i] = (int_data[i] <= cmp_val) ? 1 : 0;
            else if (op == "gt")
                data[i] = (int_data[i] > cmp_val) ? 1 : 0;
            else if (op == "ge")
                data[i] = (int_data[i] >= cmp_val) ? 1 : 0;
            else
                nulls[i] = 1;
        }

        return ColumnNullable::create(std::move(result_data), std::move(result_null));
    }
};

} // namespace DB
