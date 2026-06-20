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

#include <unordered_map>

namespace DB::DM
{

namespace
{
constexpr UInt32 SIDECAR_MAGIC = 0x4A534852; // "JSHR"
constexpr UInt32 SIDECAR_VERSION = 2; // v2: supports dictionary encoding for string sub-columns
constexpr UInt32 SIDECAR_VERSION_V1 = 1; // v1: raw columns only (still readable)

/// Encoding type stored per sub-column in manifest
enum class SubColumnEncoding : UInt8
{
    Raw = 0,
    Dictionary = 1,
};

/// Configuration for dictionary encoding of shredded sub-columns
constexpr size_t DICT_ENCODING_MAX_CARDINALITY = 4096;
constexpr size_t DICT_ENCODING_MIN_ROWS = 64;

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

    // Determine encoding for each sub-column
    std::vector<SubColumnEncoding> encodings(data.sub_columns.size(), SubColumnEncoding::Raw);
    size_t dict_encoded_count = 0;

    for (size_t i = 0; i < data.sub_columns.size(); ++i)
    {
        const auto & sub_col = data.sub_columns[i];
        // Dictionary encode string-typed sub-columns with low cardinality
        if ((sub_col.type == JsonLeafType::String || sub_col.type == JsonLeafType::Mixed)
            && sub_col.rows() >= DICT_ENCODING_MIN_ROWS)
        {
            const auto & nullable = assert_cast<const ColumnNullable &>(*sub_col.data);
            const auto & nested = assert_cast<const ColumnString &>(nullable.getNestedColumn());
            // Quick cardinality check using hash set
            std::unordered_map<StringRef, UInt32> value_to_id;
            bool suitable = true;
            for (size_t row = 0; row < nested.size(); ++row)
            {
                auto ref = nested.getDataAt(row);
                if (value_to_id.find(ref) == value_to_id.end())
                {
                    if (value_to_id.size() >= DICT_ENCODING_MAX_CARDINALITY)
                    {
                        suitable = false;
                        break;
                    }
                    value_to_id[ref] = static_cast<UInt32>(value_to_id.size());
                }
            }
            if (suitable && !value_to_id.empty())
            {
                encodings[i] = SubColumnEncoding::Dictionary;
                ++dict_encoded_count;
            }
        }
    }

    // Write manifest: magic, version, num_rows, num_sub_columns, schema + encoding
    {
        String manifest_path = dir + "/manifest.bin";
        WriteBufferFromFile buf(manifest_path);
        writeBinary(SIDECAR_MAGIC, buf);
        writeBinary(SIDECAR_VERSION, buf);
        writeBinary(static_cast<UInt64>(data.numRows()), buf);
        writeBinary(static_cast<UInt32>(data.sub_columns.size()), buf);

        // Write schema entries (v2: includes encoding type)
        for (size_t i = 0; i < data.sub_columns.size(); ++i)
        {
            const auto & sub_col = data.sub_columns[i];
            writeBinary(sub_col.path, buf);
            writeBinary(static_cast<UInt8>(sub_col.type), buf);
            writeBinary(static_cast<UInt8>(sub_col.is_array ? 1 : 0), buf);
            writeBinary(static_cast<UInt8>(encodings[i]), buf);
        }
        buf.sync();
    }

    // Write each sub-column's data
    for (size_t i = 0; i < data.sub_columns.size(); ++i)
    {
        const auto & sub_col = data.sub_columns[i];
        String safe_path = sanitizePath(sub_col.path);
        String col_file = dir + "/" + safe_path + ".bin";
        WriteBufferFromFile buf(col_file);

        const auto & nullable = assert_cast<const ColumnNullable &>(*sub_col.data);
        const auto & null_map = nullable.getNullMapData();
        size_t num_rows = nullable.size();

        // Write null bitmap
        writeBinary(static_cast<UInt64>(num_rows), buf);
        buf.write(reinterpret_cast<const char *>(null_map.data()), num_rows);

        // Write data based on type + encoding
        const auto & nested = nullable.getNestedColumn();

        if (encodings[i] == SubColumnEncoding::Dictionary)
        {
            // Dictionary-encoded string column: write dictionary + IDs
            const auto & typed = assert_cast<const ColumnString &>(nested);
            std::unordered_map<StringRef, UInt32> value_to_id;
            std::vector<String> dictionary;
            PaddedPODArray<UInt32> ids;
            ids.reserve(num_rows);

            for (size_t row = 0; row < num_rows; ++row)
            {
                auto ref = typed.getDataAt(row);
                auto it = value_to_id.find(ref);
                if (it == value_to_id.end())
                {
                    UInt32 id = static_cast<UInt32>(dictionary.size());
                    value_to_id[ref] = id;
                    dictionary.push_back(ref.toString());
                    ids.push_back(id);
                }
                else
                {
                    ids.push_back(it->second);
                }
            }

            // Write: cardinality, dictionary entries, then IDs
            writeBinary(static_cast<UInt32>(dictionary.size()), buf);
            for (const auto & entry : dictionary)
                writeBinary(entry, buf);
            // IDs as UInt32 array
            buf.write(reinterpret_cast<const char *>(ids.data()), num_rows * sizeof(UInt32));
        }
        else
        {
            // Raw encoding (unchanged from v1)
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
                const auto & offsets = typed.getOffsets();
                const auto & chars = typed.getChars();
                writeBinary(static_cast<UInt64>(offsets.size()), buf);
                buf.write(
                    reinterpret_cast<const char *>(offsets.data()),
                    offsets.size() * sizeof(ColumnString::Offset));
                writeBinary(static_cast<UInt64>(chars.size()), buf);
                buf.write(reinterpret_cast<const char *>(chars.data()), chars.size());
                break;
            }
            }
        }
        buf.sync();
    }

    LOG_INFO(
        log(),
        "Wrote JSON shredding sidecar: dmfile={} col={} rows={} sub_columns={} dict_encoded={}",
        dmfile_path,
        col_name,
        data.numRows(),
        data.sub_columns.size(),
        dict_encoded_count);
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
    if (magic != SIDECAR_MAGIC || (version != SIDECAR_VERSION && version != SIDECAR_VERSION_V1))
        return std::nullopt;

    readBinary(num_rows, manifest_buf);
    readBinary(num_sub_columns, manifest_buf);

    // Read schema
    struct SchemaEntry
    {
        String path;
        JsonLeafType type;
        bool is_array;
        SubColumnEncoding encoding;
    };
    std::vector<SchemaEntry> schema_entries;
    schema_entries.reserve(num_sub_columns);

    for (UInt32 i = 0; i < num_sub_columns; ++i)
    {
        SchemaEntry entry;
        UInt8 type_val, is_arr;
        readBinary(entry.path, manifest_buf);
        readBinary(type_val, manifest_buf);
        readBinary(is_arr, manifest_buf);
        entry.type = static_cast<JsonLeafType>(type_val);
        entry.is_array = (is_arr != 0);

        // v2 has encoding byte; v1 defaults to Raw
        if (version >= SIDECAR_VERSION)
        {
            UInt8 enc;
            readBinary(enc, manifest_buf);
            entry.encoding = static_cast<SubColumnEncoding>(enc);
        }
        else
        {
            entry.encoding = SubColumnEncoding::Raw;
        }
        schema_entries.push_back(std::move(entry));
    }

    // Read sub-column data
    ShreddedJsonData result;
    result.schema.total_rows = num_rows;
    result.schema.rows_with_json = num_rows;

    for (UInt32 col_idx = 0; col_idx < num_sub_columns; ++col_idx)
    {
        const auto & entry = schema_entries[col_idx];
        String safe_path = sanitizePath(entry.path);
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

        if (entry.encoding == SubColumnEncoding::Dictionary)
        {
            // Dictionary-encoded: read dictionary + IDs, reconstruct ColumnString
            UInt32 cardinality;
            readBinary(cardinality, col_buf);
            std::vector<String> dictionary(cardinality);
            for (UInt32 d = 0; d < cardinality; ++d)
                readBinary(dictionary[d], col_buf);

            // Read IDs
            PaddedPODArray<UInt32> ids(num_rows);
            col_buf.readStrict(reinterpret_cast<char *>(ids.data()), num_rows * sizeof(UInt32));

            // Reconstruct ColumnString from dictionary + IDs
            auto col = ColumnString::create();
            col->reserve(num_rows);
            for (size_t row = 0; row < num_rows; ++row)
            {
                const auto & val = dictionary[ids[row]];
                col->insertData(val.data(), val.size());
            }
            inner = std::move(col);
        }
        else
        {
            // Raw encoding
            switch (entry.type)
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
        }

        auto null_map_col = ColumnUInt8::create();
        null_map_col->getData() = std::move(null_map);
        auto nullable_col = ColumnNullable::create(std::move(inner), std::move(null_map_col));

        JsonSubColumn sub_col;
        sub_col.path = entry.path;
        sub_col.type = entry.type;
        sub_col.is_array = entry.is_array;
        sub_col.data = std::move(nullable_col);
        result.sub_columns.push_back(std::move(sub_col));

        result.schema.columns.push_back(JsonShreddedColumn{entry.path, entry.type, num_rows, entry.is_array});
    }

    LOG_INFO(
        log(),
        "Read JSON shredding sidecar: dmfile={} col={} rows={} sub_columns={} version={}",
        dmfile_path,
        col_name,
        num_rows,
        num_sub_columns,
        version);

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

ColumnPtr JsonShreddedStore::getCachedColumn(const String & dmfile_path, const String & col_name, const String & path)
{
    std::lock_guard lock(cacheMutex());
    auto it = columnCache().find(columnCacheKey(dmfile_path, col_name, path));
    if (it != columnCache().end())
        return it->second;
    return nullptr;
}

void JsonShreddedStore::putCachedColumn(
    const String & dmfile_path,
    const String & col_name,
    const String & path,
    ColumnPtr col)
{
    std::lock_guard lock(cacheMutex());
    columnCache()[columnCacheKey(dmfile_path, col_name, path)] = std::move(col);
}

std::vector<SidecarSchemaEntry> JsonShreddedStore::readSidecarManifest(
    const String & dmfile_path,
    const String & col_name,
    UInt64 & out_num_rows)
{
    String dir = sidecarPath(dmfile_path, col_name);
    String manifest_path = dir + "/manifest.bin";

    if (!Poco::File(manifest_path).exists())
        return {};

    ReadBufferFromFile manifest_buf(manifest_path);
    UInt32 magic, version;
    UInt64 num_rows;
    UInt32 num_sub_columns;

    readBinary(magic, manifest_buf);
    readBinary(version, manifest_buf);
    if (magic != SIDECAR_MAGIC || (version != SIDECAR_VERSION && version != SIDECAR_VERSION_V1))
        return {};

    readBinary(num_rows, manifest_buf);
    readBinary(num_sub_columns, manifest_buf);
    out_num_rows = num_rows;

    std::vector<SidecarSchemaEntry> entries;
    entries.reserve(num_sub_columns);

    for (UInt32 i = 0; i < num_sub_columns; ++i)
    {
        SidecarSchemaEntry entry;
        UInt8 type_val, is_arr;
        readBinary(entry.path, manifest_buf);
        readBinary(type_val, manifest_buf);
        readBinary(is_arr, manifest_buf);
        entry.type = type_val;
        entry.is_array = (is_arr != 0);

        if (version >= SIDECAR_VERSION)
        {
            UInt8 enc;
            readBinary(enc, manifest_buf);
            entry.encoding = enc;
        }
        else
        {
            entry.encoding = static_cast<UInt8>(SubColumnEncoding::Raw);
        }
        entries.push_back(std::move(entry));
    }

    return entries;
}

ColumnPtr JsonShreddedStore::readColumnFile(
    const String & dir,
    const String & path,
    UInt8 type,
    UInt8 encoding,
    UInt64 num_rows)
{
    String safe_path = sanitizePath(path);
    String col_file = dir + "/" + safe_path + ".bin";

    if (!Poco::File(col_file).exists())
        return nullptr;

    ReadBufferFromFile col_buf(col_file);

    UInt64 file_num_rows;
    readBinary(file_num_rows, col_buf);
    if (file_num_rows != num_rows)
        return nullptr;

    // Read null bitmap
    ColumnUInt8::Container null_map(num_rows);
    col_buf.readStrict(reinterpret_cast<char *>(null_map.data()), num_rows);

    // Read data based on type + encoding
    MutableColumnPtr inner;
    auto leaf_type = static_cast<JsonLeafType>(type);
    auto col_encoding = static_cast<SubColumnEncoding>(encoding);

    if (col_encoding == SubColumnEncoding::Dictionary)
    {
        UInt32 cardinality;
        readBinary(cardinality, col_buf);
        std::vector<String> dictionary(cardinality);
        for (UInt32 d = 0; d < cardinality; ++d)
            readBinary(dictionary[d], col_buf);

        PaddedPODArray<UInt32> ids(num_rows);
        col_buf.readStrict(reinterpret_cast<char *>(ids.data()), num_rows * sizeof(UInt32));

        auto col = ColumnString::create();
        col->reserve(num_rows);
        for (size_t row = 0; row < num_rows; ++row)
        {
            const auto & val = dictionary[ids[row]];
            col->insertData(val.data(), val.size());
        }
        inner = std::move(col);
    }
    else
    {
        switch (leaf_type)
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
    }

    auto null_map_col = ColumnUInt8::create();
    null_map_col->getData() = std::move(null_map);
    return ColumnNullable::create(std::move(inner), std::move(null_map_col));
}

ColumnPtr JsonShreddedStore::readSidecarColumn(
    const String & dmfile_path,
    const String & col_name,
    const String & path)
{
    // Check per-column cache first
    ColumnPtr cached = getCachedColumn(dmfile_path, col_name, path);
    if (cached)
        return cached;

    // Read manifest to find the path's type and encoding
    UInt64 num_rows = 0;
    auto entries = readSidecarManifest(dmfile_path, col_name, num_rows);
    if (entries.empty())
        return nullptr;

    // Find the entry for the requested path
    const SidecarSchemaEntry * target = nullptr;
    for (const auto & entry : entries)
    {
        if (entry.path == path)
        {
            target = &entry;
            break;
        }
    }
    if (!target)
        return nullptr;

    String dir = sidecarPath(dmfile_path, col_name);
    ColumnPtr result = readColumnFile(dir, path, target->type, target->encoding, num_rows);

    if (result)
    {
        putCachedColumn(dmfile_path, col_name, path, result);

        static std::atomic<int> lazy_load_log_count{0};
        if (lazy_load_log_count.fetch_add(1) < 5)
        {
            LOG_INFO(
                log(),
                "Lazy-loaded single sidecar column: dmfile={} col={} path={} rows={}",
                dmfile_path,
                col_name,
                path,
                num_rows);
        }
    }

    return result;
}

} // namespace DB::DM
