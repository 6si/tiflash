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
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Storages/DeltaMerge/JsonShredding/JsonBinaryNavigator.h>
#include <Storages/DeltaMerge/JsonShredding/JsonShredder.h>

namespace DB::DM
{

JsonShredder::JsonShredder(const JsonShreddingConfig & config)
    : config_(config)
{}

ShreddedJsonData JsonShredder::shred(const ColumnString & json_column)
{
    // Step 1: Infer schema from all rows
    JsonSchemaTree tree(config_);
    size_t num_rows = json_column.size();

    for (size_t i = 0; i < num_rows; ++i)
    {
        StringRef data = json_column.getDataAt(i);
        if (data.size == 0)
            tree.addNull();
        else
            tree.addRow(data);
    }

    JsonInferredSchema schema = tree.finalize();

    // Step 2: Shred using the inferred schema
    return shredWithSchema(json_column, schema);
}

ShreddedJsonData JsonShredder::shredWithSchema(
    const ColumnString & json_column,
    const JsonInferredSchema & schema)
{
    ShreddedJsonData result;
    size_t num_rows = json_column.size();

    // Always preserve the original blob
    result.original_blob = json_column.getPtr();
    result.schema = schema;

    if (schema.empty())
        return result;

    // Initialize sub-columns
    result.sub_columns = initSubColumns(schema, num_rows);

    // Extract values row by row
    for (size_t i = 0; i < num_rows; ++i)
    {
        StringRef data = json_column.getDataAt(i);
        extractRow(data, schema, result.sub_columns);
    }

    return result;
}

std::vector<JsonSubColumn> JsonShredder::initSubColumns(
    const JsonInferredSchema & schema,
    size_t /*num_rows*/)
{
    std::vector<JsonSubColumn> sub_columns;
    sub_columns.reserve(schema.columns.size());

    for (const auto & col_def : schema.columns)
    {
        JsonSubColumn sub_col;
        sub_col.path = col_def.path;
        sub_col.type = col_def.type;
        sub_col.is_array = col_def.is_array;

        // Create the appropriate nullable column type
        MutableColumnPtr inner;
        switch (col_def.type)
        {
        case JsonLeafType::Int64:
            inner = ColumnInt64::create();
            break;
        case JsonLeafType::UInt64:
            inner = ColumnUInt64::create();
            break;
        case JsonLeafType::Float64:
            inner = ColumnFloat64::create();
            break;
        case JsonLeafType::Bool:
            inner = ColumnUInt8::create();
            break;
        case JsonLeafType::String:
        case JsonLeafType::Mixed:
        case JsonLeafType::Null:
            inner = ColumnString::create();
            break;
        }

        sub_col.data = ColumnNullable::create(std::move(inner), ColumnUInt8::create());
        sub_columns.push_back(std::move(sub_col));
    }

    return sub_columns;
}

void JsonShredder::extractRow(
    const StringRef & json_data,
    const JsonInferredSchema & schema,
    std::vector<JsonSubColumn> & sub_columns)
{
    if (json_data.size == 0)
    {
        // NULL row — insert NULL into all sub-columns
        for (auto & sub_col : sub_columns)
        {
            auto & nullable = assert_cast<ColumnNullable &>(*sub_col.data);
            nullable.insertDefault(); // inserts NULL
        }
        return;
    }

    auto type_code = static_cast<JsonBinary::JsonType>(json_data.data[0]);
    StringRef data(json_data.data + 1, json_data.size - 1);

    for (size_t col_idx = 0; col_idx < schema.columns.size(); ++col_idx)
    {
        const auto & col_def = schema.columns[col_idx];
        auto & sub_col = sub_columns[col_idx];
        auto & nullable = assert_cast<ColumnNullable &>(*sub_col.data);

        auto nav_result = JsonBinaryNavigator::navigatePath(type_code, data, col_def.path);

        if (!nav_result.has_value())
        {
            nullable.insertDefault(); // NULL — path not present
            continue;
        }

        auto & val = nav_result.value();

        // Check for JSON null literal
        if (val.type == JsonBinary::TYPE_CODE_LITERAL)
        {
            UInt8 lit = JsonBinaryNavigator::getLiteral(val);
            if (lit == JsonBinary::LITERAL_NIL)
            {
                nullable.insertDefault();
                continue;
            }
            // true/false for Bool type
            if (col_def.type == JsonLeafType::Bool)
            {
                UInt8 bool_val = (lit == JsonBinary::LITERAL_TRUE) ? 1 : 0;
                nullable.insert(Field(static_cast<UInt64>(bool_val)));
                continue;
            }
        }

        // Extract typed value
        Field field_val = jsonValueToField(val, col_def.type);
        if (field_val.isNull())
            nullable.insertDefault();
        else
            nullable.insert(field_val);
    }
}

bool JsonShredder::navigateToPath(
    const JsonBinary & /*root*/,
    const String & path,
    JsonBinary & /*out_value*/,
    JsonBinary::JsonType & /*out_type*/)
{
    // Deprecated — use JsonBinaryNavigator::navigatePath instead
    (void)path;
    return false;
}

Field JsonShredder::jsonValueToField(
    const JsonBinaryNavigator::ValueRef & value,
    JsonLeafType target_type)
{
    switch (target_type)
    {
    case JsonLeafType::Int64:
    {
        if (value.type == JsonBinary::TYPE_CODE_INT64)
            return Field(JsonBinaryNavigator::getInt64(value));
        return Field();
    }
    case JsonLeafType::UInt64:
    {
        if (value.type == JsonBinary::TYPE_CODE_UINT64)
            return Field(JsonBinaryNavigator::getUInt64(value));
        return Field();
    }
    case JsonLeafType::Float64:
    {
        if (value.type == JsonBinary::TYPE_CODE_FLOAT64)
            return Field(JsonBinaryNavigator::getFloat64(value));
        return Field();
    }
    case JsonLeafType::Bool:
    {
        if (value.type == JsonBinary::TYPE_CODE_LITERAL)
        {
            UInt8 lit = JsonBinaryNavigator::getLiteral(value);
            if (lit == JsonBinary::LITERAL_TRUE)
                return Field(static_cast<UInt64>(1));
            if (lit == JsonBinary::LITERAL_FALSE)
                return Field(static_cast<UInt64>(0));
        }
        return Field();
    }
    case JsonLeafType::String:
    {
        if (value.type == JsonBinary::TYPE_CODE_STRING)
            return Field(JsonBinaryNavigator::getString(value));
        return Field();
    }
    case JsonLeafType::Mixed:
    {
        // For Mixed type, store the raw value as a string representation
        if (value.type == JsonBinary::TYPE_CODE_STRING)
            return Field(JsonBinaryNavigator::getString(value));
        if (value.type == JsonBinary::TYPE_CODE_INT64)
            return Field(std::to_string(JsonBinaryNavigator::getInt64(value)));
        if (value.type == JsonBinary::TYPE_CODE_UINT64)
            return Field(std::to_string(JsonBinaryNavigator::getUInt64(value)));
        if (value.type == JsonBinary::TYPE_CODE_FLOAT64)
            return Field(std::to_string(JsonBinaryNavigator::getFloat64(value)));
        if (value.type == JsonBinary::TYPE_CODE_LITERAL)
        {
            UInt8 lit = JsonBinaryNavigator::getLiteral(value);
            if (lit == JsonBinary::LITERAL_TRUE)
                return Field(String("true"));
            if (lit == JsonBinary::LITERAL_FALSE)
                return Field(String("false"));
            return Field(String("null"));
        }
        // Objects/arrays stored as binary — not decoded further for Mixed
        return Field(String("[complex]"));
    }
    case JsonLeafType::Null:
        return Field();
    }
    return Field();
}

MutableColumnPtr JsonShredder::extractPath(
    const ColumnString & json_column,
    const String & path,
    JsonLeafType expected_type)
{
    MutableColumnPtr inner;
    switch (expected_type)
    {
    case JsonLeafType::Int64:
        inner = ColumnInt64::create();
        break;
    case JsonLeafType::UInt64:
        inner = ColumnUInt64::create();
        break;
    case JsonLeafType::Float64:
        inner = ColumnFloat64::create();
        break;
    case JsonLeafType::Bool:
        inner = ColumnUInt8::create();
        break;
    default:
        inner = ColumnString::create();
        break;
    }
    auto result = ColumnNullable::create(std::move(inner), ColumnUInt8::create());

    for (size_t i = 0; i < json_column.size(); ++i)
    {
        StringRef data = json_column.getDataAt(i);
        if (data.size == 0)
        {
            result->insertDefault();
            continue;
        }

        auto type_code = static_cast<JsonBinary::JsonType>(data.data[0]);
        StringRef val_data(data.data + 1, data.size - 1);

        auto nav_result = JsonBinaryNavigator::navigatePath(type_code, val_data, path);
        if (!nav_result.has_value())
        {
            result->insertDefault();
            continue;
        }

        Field value = jsonValueToField(nav_result.value(), expected_type);
        if (value.isNull())
            result->insertDefault();
        else
            result->insert(value);
    }

    return result;
}

// --- JsonSubColumnReader ---

ColumnPtr JsonSubColumnReader::readPath(const ShreddedJsonData & data, const String & path)
{
    for (const auto & sub_col : data.sub_columns)
    {
        if (sub_col.path == path)
        {
            auto col_ptr = sub_col.data->getPtr();
            // If the nested column is dictionary-encoded, decode it for callers
            // that expect a regular typed column (e.g., ColumnString).
            const auto * nullable = typeid_cast<const ColumnNullable *>(col_ptr.get());
            if (nullable)
            {
                const auto * dict_col = typeid_cast<const ColumnDictionary *>(&nullable->getNestedColumn());
                if (dict_col)
                {
                    auto decoded = dict_col->decode();
                    auto null_map = nullable->getNullMapColumnPtr();
                    return ColumnNullable::create(decoded->assumeMutable(), null_map->assumeMutable());
                }
            }
            return col_ptr;
        }
    }
    return nullptr;
}

ColumnPtr JsonSubColumnReader::readFullBlob(const ShreddedJsonData & data)
{
    return data.original_blob;
}

bool JsonSubColumnReader::hasPath(const ShreddedJsonData & data, const String & path)
{
    for (const auto & sub_col : data.sub_columns)
    {
        if (sub_col.path == path)
            return true;
    }
    return false;
}

} // namespace DB::DM
