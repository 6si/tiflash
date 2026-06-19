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

#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <IO/Buffer/ReadBufferFromFile.h>
#include <IO/Buffer/WriteBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Poco/Path.h>
#include <Storages/DeltaMerge/JsonShredding/JsonShreddedStore.h>

namespace DB::DM
{

namespace
{
constexpr UInt32 SIDECAR_MAGIC = 0x4A534852; // "JSHR"
constexpr UInt32 SIDECAR_VERSION = 1;

String sanitizePath(const String & path)
{
    // Replace dots with underscores for filesystem safety
    String result = path;
    for (auto & c : result)
    {
        if (c == '.' || c == '/' || c == '\\')
            c = '_';
    }
    return result;
}
} // namespace

String JsonShreddedStore::sidecarPath(const String & dmfile_path, const String & col_name)
{
    return dmfile_path + "/.json_shredded/" + col_name;
}

bool JsonShreddedStore::hasSidecar(const String & dmfile_path, const String & col_name)
{
    String path = sidecarPath(dmfile_path, col_name) + "/manifest.bin";
    return Poco::File(path).exists();
}

void JsonShreddedStore::writeSidecar(
    const String & dmfile_path,
    const String & col_name,
    const ShreddedJsonData & data)
{
    if (data.sub_columns.empty())
        return;

    String dir = sidecarPath(dmfile_path, col_name);
    Poco::File(dir).createDirectories();

    // Write manifest: magic, version, num_rows, num_sub_columns, schema
    {
        String manifest_path = dir + "/manifest.bin";
        WriteBufferFromFile buf(manifest_path);
        writeBinary(SIDECAR_MAGIC, buf);
        writeBinary(SIDECAR_VERSION, buf);
        writeBinary(static_cast<UInt64>(data.numRows()), buf);
        writeBinary(static_cast<UInt32>(data.sub_columns.size()), buf);

        // Write schema entries
        for (const auto & sub_col : data.sub_columns)
        {
            writeBinary(sub_col.path, buf);
            writeBinary(static_cast<UInt8>(sub_col.type), buf);
            writeBinary(static_cast<UInt8>(sub_col.is_array ? 1 : 0), buf);
        }
        buf.sync();
    }

    // Write each sub-column's data
    for (const auto & sub_col : data.sub_columns)
    {
        String safe_path = sanitizePath(sub_col.path);
        String col_file = dir + "/" + safe_path + ".bin";
        WriteBufferFromFile buf(col_file);

        const auto & nullable = assert_cast<const ColumnNullable &>(*sub_col.data);
        const auto & null_map = nullable.getNullMapData();
        size_t num_rows = nullable.size();

        // Write null bitmap
        writeBinary(static_cast<UInt64>(num_rows), buf);
        buf.write(reinterpret_cast<const char *>(null_map.data()), num_rows);

        // Write data based on type
        const auto & nested = nullable.getNestedColumn();
        switch (sub_col.type)
        {
        case JsonLeafType::Int64:
        {
            const auto & typed = assert_cast<const ColumnInt64 &>(nested);
            buf.write(reinterpret_cast<const char *>(typed.getData().data()), num_rows * sizeof(Int64));
            break;
        }
        case JsonLeafType::UInt64:
        {
            const auto & typed = assert_cast<const ColumnUInt64 &>(nested);
            buf.write(reinterpret_cast<const char *>(typed.getData().data()), num_rows * sizeof(UInt64));
            break;
        }
        case JsonLeafType::Float64:
        {
            const auto & typed = assert_cast<const ColumnFloat64 &>(nested);
            buf.write(reinterpret_cast<const char *>(typed.getData().data()), num_rows * sizeof(Float64));
            break;
        }
        case JsonLeafType::Bool:
        {
            const auto & typed = assert_cast<const ColumnUInt8 &>(nested);
            buf.write(reinterpret_cast<const char *>(typed.getData().data()), num_rows * sizeof(UInt8));
            break;
        }
        case JsonLeafType::String:
        case JsonLeafType::Mixed:
        case JsonLeafType::Null:
        {
            const auto & typed = assert_cast<const ColumnString &>(nested);
            // Write offsets then chars
            const auto & offsets = typed.getOffsets();
            const auto & chars = typed.getChars();
            writeBinary(static_cast<UInt64>(offsets.size()), buf);
            buf.write(reinterpret_cast<const char *>(offsets.data()), offsets.size() * sizeof(ColumnString::Offset));
            writeBinary(static_cast<UInt64>(chars.size()), buf);
            buf.write(reinterpret_cast<const char *>(chars.data()), chars.size());
            break;
        }
        }
        buf.sync();
    }

    LOG_INFO(log(), "Wrote JSON shredding sidecar: dmfile={} col={} rows={} sub_columns={}",
        dmfile_path, col_name, data.numRows(), data.sub_columns.size());
}

std::optional<ShreddedJsonData> JsonShreddedStore::readSidecar(
    const String & dmfile_path,
    const String & col_name)
{
    String dir = sidecarPath(dmfile_path, col_name);
    String manifest_path = dir + "/manifest.bin";

    if (!Poco::File(manifest_path).exists())
        return std::nullopt;

    // Read manifest
    ReadBufferFromFile manifest_buf(manifest_path);
    UInt32 magic, version;
    UInt64 num_rows;
    UInt32 num_sub_columns;

    readBinary(magic, manifest_buf);
    readBinary(version, manifest_buf);
    if (magic != SIDECAR_MAGIC || version != SIDECAR_VERSION)
        return std::nullopt;

    readBinary(num_rows, manifest_buf);
    readBinary(num_sub_columns, manifest_buf);

    // Read schema
    std::vector<std::pair<String, JsonLeafType>> schema_entries;
    std::vector<bool> is_array_entries;
    for (UInt32 i = 0; i < num_sub_columns; ++i)
    {
        String path;
        UInt8 type_val, is_arr;
        readBinary(path, manifest_buf);
        readBinary(type_val, manifest_buf);
        readBinary(is_arr, manifest_buf);
        schema_entries.emplace_back(path, static_cast<JsonLeafType>(type_val));
        is_array_entries.push_back(is_arr != 0);
    }

    // Read sub-column data
    ShreddedJsonData result;
    result.schema.total_rows = num_rows;
    result.schema.rows_with_json = num_rows;

    for (UInt32 col_idx = 0; col_idx < num_sub_columns; ++col_idx)
    {
        const auto & [path, type] = schema_entries[col_idx];
        String safe_path = sanitizePath(path);
        String col_file = dir + "/" + safe_path + ".bin";

        if (!Poco::File(col_file).exists())
            return std::nullopt;

        ReadBufferFromFile col_buf(col_file);

        UInt64 file_num_rows;
        readBinary(file_num_rows, col_buf);
        if (file_num_rows != num_rows)
            return std::nullopt;

        // Read null bitmap
        ColumnUInt8::Container null_map(num_rows);
        col_buf.readStrict(reinterpret_cast<char *>(null_map.data()), num_rows);

        // Read data
        MutableColumnPtr inner;
        switch (type)
        {
        case JsonLeafType::Int64:
        {
            auto col = ColumnInt64::create(num_rows);
            col_buf.readStrict(reinterpret_cast<char *>(col->getData().data()), num_rows * sizeof(Int64));
            inner = std::move(col);
            break;
        }
        case JsonLeafType::UInt64:
        {
            auto col = ColumnUInt64::create(num_rows);
            col_buf.readStrict(reinterpret_cast<char *>(col->getData().data()), num_rows * sizeof(UInt64));
            inner = std::move(col);
            break;
        }
        case JsonLeafType::Float64:
        {
            auto col = ColumnFloat64::create(num_rows);
            col_buf.readStrict(reinterpret_cast<char *>(col->getData().data()), num_rows * sizeof(Float64));
            inner = std::move(col);
            break;
        }
        case JsonLeafType::Bool:
        {
            auto col = ColumnUInt8::create(num_rows);
            col_buf.readStrict(reinterpret_cast<char *>(col->getData().data()), num_rows * sizeof(UInt8));
            inner = std::move(col);
            break;
        }
        case JsonLeafType::String:
        case JsonLeafType::Mixed:
        case JsonLeafType::Null:
        {
            UInt64 num_offsets, num_chars;
            readBinary(num_offsets, col_buf);
            auto col = ColumnString::create();
            col->getOffsets().resize(num_offsets);
            col_buf.readStrict(
                reinterpret_cast<char *>(col->getOffsets().data()),
                num_offsets * sizeof(ColumnString::Offset));
            readBinary(num_chars, col_buf);
            col->getChars().resize(num_chars);
            col_buf.readStrict(reinterpret_cast<char *>(col->getChars().data()), num_chars);
            inner = std::move(col);
            break;
        }
        }

        auto null_map_col = ColumnUInt8::create();
        null_map_col->getData() = std::move(null_map);
        auto nullable_col = ColumnNullable::create(std::move(inner), std::move(null_map_col));

        JsonSubColumn sub_col;
        sub_col.path = path;
        sub_col.type = type;
        sub_col.is_array = is_array_entries[col_idx];
        sub_col.data = std::move(nullable_col);
        result.sub_columns.push_back(std::move(sub_col));

        result.schema.columns.push_back(JsonShreddedColumn{path, type, num_rows, is_array_entries[col_idx]});
    }

    LOG_INFO(log(), "Read JSON shredding sidecar: dmfile={} col={} rows={} sub_columns={}",
        dmfile_path, col_name, num_rows, num_sub_columns);

    return result;
}

const ShreddedJsonData * JsonShreddedStore::getCached(const String & dmfile_path, const String & col_name)
{
    std::lock_guard lock(cacheMutex());
    auto it = cache().find(cacheKey(dmfile_path, col_name));
    if (it != cache().end())
        return &it->second;
    return nullptr;
}

void JsonShreddedStore::putCache(const String & dmfile_path, const String & col_name, ShreddedJsonData && data)
{
    std::lock_guard lock(cacheMutex());
    cache()[cacheKey(dmfile_path, col_name)] = std::move(data);
}

} // namespace DB::DM
