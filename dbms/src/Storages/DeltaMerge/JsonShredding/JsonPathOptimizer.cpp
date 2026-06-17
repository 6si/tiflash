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

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/assert_cast.h>
#include <Storages/DeltaMerge/JsonShredding/JsonPathOptimizer.h>

namespace DB::DM
{

namespace
{
/// Simple LIKE pattern matcher supporting % (any sequence) and _ (single char)
bool matchLikePattern(const String & str, const String & pattern)
{
    size_t s = 0, p = 0;
    size_t star_p = String::npos, star_s = 0;

    while (s < str.size())
    {
        if (p < pattern.size() && (pattern[p] == str[s] || pattern[p] == '_'))
        {
            ++s;
            ++p;
        }
        else if (p < pattern.size() && pattern[p] == '%')
        {
            star_p = p;
            star_s = s;
            ++p;
        }
        else if (star_p != String::npos)
        {
            p = star_p + 1;
            s = ++star_s;
        }
        else
        {
            return false;
        }
    }

    while (p < pattern.size() && pattern[p] == '%')
        ++p;

    return p == pattern.size();
}
} // anonymous namespace

ColumnPtr JsonPathOptimizer::tryReadShredded(
    const ShreddedJsonData & shredded_data,
    const String & json_path)
{
    if (!JsonShreddingFlag::instance().useShredded())
        return nullptr;

    String normalized = normalizePath(json_path);
    return JsonSubColumnReader::readPath(shredded_data, normalized);
}

ColumnPtr JsonPathOptimizer::extractJsonPath(
    const ShreddedJsonData & shredded_data,
    const String & json_path,
    JsonLeafType type_hint)
{
    // Try shredded fast path first
    ColumnPtr shredded = tryReadShredded(shredded_data, json_path);
    if (shredded)
        return shredded;

    // Fall back to blob extraction
    String normalized = normalizePath(json_path);
    auto blob = shredded_data.original_blob;
    if (!blob)
        return nullptr;

    const auto & col_string = assert_cast<const ColumnString &>(*blob);
    return JsonShredder::extractPath(col_string, normalized, type_hint);
}

bool JsonPathOptimizer::canUseShredded(const ShreddedJsonData & shredded_data, const String & json_path)
{
    if (!JsonShreddingFlag::instance().useShredded())
        return false;

    String normalized = normalizePath(json_path);
    return JsonSubColumnReader::hasPath(shredded_data, normalized);
}

MutableColumnPtr JsonPathOptimizer::applyFilter(
    const ShreddedJsonData & shredded_data,
    const String & json_path,
    FilterOp op,
    const Field & value)
{
    String normalized = normalizePath(json_path);
    size_t num_rows = shredded_data.numRows();
    auto result = ColumnUInt8::create(num_rows, 0);
    auto & result_data = result->getData();

    // Try to use shredded sub-column
    ColumnPtr sub_col = nullptr;
    if (JsonShreddingFlag::instance().useShredded())
        sub_col = JsonSubColumnReader::readPath(shredded_data, normalized);

    if (!sub_col)
    {
        // Fall back: extract from blob and apply filter
        const auto & blob = assert_cast<const ColumnString &>(*shredded_data.original_blob);

        // Determine type from schema
        JsonLeafType type = JsonLeafType::String;
        for (const auto & col : shredded_data.schema.columns)
        {
            if (col.path == normalized)
            {
                type = col.type;
                break;
            }
        }

        sub_col = JsonShredder::extractPath(blob, normalized, type);
    }

    if (!sub_col)
    {
        // Path doesn't exist at all — all rows fail filter (except IsNull)
        if (op == FilterOp::IsNull)
            std::fill(result_data.begin(), result_data.end(), 1);
        return result;
    }

    const auto & nullable = assert_cast<const ColumnNullable &>(*sub_col);

    for (size_t i = 0; i < num_rows; ++i)
    {
        bool is_null = nullable.isNullAt(i);

        switch (op)
        {
        case FilterOp::IsNull:
            result_data[i] = is_null ? 1 : 0;
            break;
        case FilterOp::IsNotNull:
            result_data[i] = is_null ? 0 : 1;
            break;
        case FilterOp::Equal:
            if (is_null)
            {
                result_data[i] = 0;
            }
            else
            {
                Field cell = nullable.getNestedColumn().operator[](i);
                result_data[i] = (cell == value) ? 1 : 0;
            }
            break;
        case FilterOp::NotEqual:
            if (is_null)
            {
                result_data[i] = 0;
            }
            else
            {
                Field cell = nullable.getNestedColumn().operator[](i);
                result_data[i] = (cell != value) ? 1 : 0;
            }
            break;
        case FilterOp::LessThan:
            if (is_null)
            {
                result_data[i] = 0;
            }
            else
            {
                Field cell = nullable.getNestedColumn().operator[](i);
                result_data[i] = (cell < value) ? 1 : 0;
            }
            break;
        case FilterOp::LessOrEqual:
            if (is_null)
            {
                result_data[i] = 0;
            }
            else
            {
                Field cell = nullable.getNestedColumn().operator[](i);
                result_data[i] = (cell <= value) ? 1 : 0;
            }
            break;
        case FilterOp::GreaterThan:
            if (is_null)
            {
                result_data[i] = 0;
            }
            else
            {
                Field cell = nullable.getNestedColumn().operator[](i);
                result_data[i] = (cell > value) ? 1 : 0;
            }
            break;
        case FilterOp::GreaterOrEqual:
            if (is_null)
            {
                result_data[i] = 0;
            }
            else
            {
                Field cell = nullable.getNestedColumn().operator[](i);
                result_data[i] = (cell >= value) ? 1 : 0;
            }
            break;
        case FilterOp::Like:
        {
            // LIKE only applies to string columns
            if (is_null)
            {
                result_data[i] = 0;
            }
            else
            {
                Field cell = nullable.getNestedColumn().operator[](i);
                if (cell.getType() == Field::Types::String)
                {
                    const String & str_val = cell.get<String>();
                    const String & pattern = value.get<String>();
                    // Simple LIKE pattern matching with % and _
                    // Reuse the matchLike approach from EncodedFilter
                    result_data[i] = matchLikePattern(str_val, pattern) ? 1 : 0;
                }
                else
                {
                    result_data[i] = 0;
                }
            }
            break;
        }
        }
    }

    return result;
}

String JsonPathOptimizer::normalizePath(const String & tidb_path)
{
    // TiDB paths look like: $.field.subfield or $."field name"
    // Our internal format: field.subfield (dot-separated, no $. prefix)
    String result = tidb_path;

    // Strip leading $. or $
    if (result.size() >= 2 && result[0] == '$' && result[1] == '.')
        result = result.substr(2);
    else if (result.size() >= 1 && result[0] == '$')
        result = result.substr(1);

    // Strip surrounding quotes from path components if present
    // e.g., "field"."subfield" → field.subfield
    String cleaned;
    bool in_quotes = false;
    for (size_t i = 0; i < result.size(); ++i)
    {
        if (result[i] == '"')
        {
            in_quotes = !in_quotes;
            continue;
        }
        cleaned += result[i];
    }

    return cleaned;
}

} // namespace DB::DM
