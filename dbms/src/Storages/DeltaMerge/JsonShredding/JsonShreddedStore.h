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

#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnShreddedAttachment.h>
#include <Core/Types.h>
#include <Storages/DeltaMerge/JsonShredding/JsonSchemaTree.h>
#include <Storages/DeltaMerge/JsonShredding/JsonShredder.h>
#include <common/logger_useful.h>

#include <mutex>
#include <optional>
#include <unordered_map>

namespace DB::DM
{

/// Thread-local context that holds shredded sub-columns for the current block being read.
/// Set by the read pipeline (DMFileBlockInputStream or Segment read), consumed by FunctionJsonExtract.
///
/// Lifecycle:
///   1. Reader loads a block from DMFile that has a shredded sidecar
///   2. Reader calls setForCurrentBlock() with the shredded data + row range
///   3. Expression evaluation runs (including FunctionJsonExtract)
///   4. FunctionJsonExtract calls getSubColumn() to get pre-extracted values (sliced to pack range)
///   5. After block processing, reader calls clear()
class JsonShreddedBlockContext
{
public:
    static JsonShreddedBlockContext & instance()
    {
        thread_local JsonShreddedBlockContext ctx;
        return ctx;
    }

    /// Set the shredded data for the current block being processed.
    /// col_name: the logical column name (e.g., "payload")
    /// col_id: the column ID (stable across rename/projection)
    /// data: the shredded sub-columns for the ENTIRE DMFile
    /// row_offset: starting row within the DMFile for this pack batch
    /// row_count: number of rows in this pack batch
    void setForCurrentBlock(
        const String & col_name,
        Int64 col_id,
        const ShreddedJsonData & data,
        size_t row_offset,
        size_t row_count)
    {
        BlockShreddedRef ref{&data, row_offset, row_count};
        current_data_[col_name] = ref;
        if (col_id != 0)
            current_data_by_id_[col_id] = ref;
    }

    /// Get a specific sub-column for a path from the current block's shredded data.
    /// Looks up by column_id first (survives column rename in projection), then by name.
    /// Returns a column sliced to the current pack's row range.
    /// Returns nullptr if not available or if the path doesn't exist in the sidecar.
    ColumnPtr getSubColumn(const String & col_name, Int64 col_id, const String & path) const
    {
        const BlockShreddedRef * ref_ptr = nullptr;
        // Try column_id first (stable across rename/projection)
        if (col_id != 0)
        {
            auto it = current_data_by_id_.find(col_id);
            if (it != current_data_by_id_.end() && it->second.data != nullptr)
                ref_ptr = &it->second;
        }
        // Fall back to name lookup
        if (!ref_ptr)
        {
            auto it = current_data_.find(col_name);
            if (it != current_data_.end() && it->second.data != nullptr)
                ref_ptr = &it->second;
        }
        if (!ref_ptr)
            return nullptr;

        auto full_col = JsonSubColumnReader::readPath(*ref_ptr->data, path);
        if (!full_col)
            return nullptr;
        if (full_col->size() == ref_ptr->row_count)
            return full_col;
        if (full_col->size() < ref_ptr->row_offset + ref_ptr->row_count)
            return nullptr;
        return full_col->cut(ref_ptr->row_offset, ref_ptr->row_count);
    }

    /// Backward-compatible overload (name-only, for unit tests).
    ColumnPtr getSubColumn(const String & col_name, const String & path) const
    {
        return getSubColumn(col_name, 0, path);
    }

    /// Check if shredded data is available for a column (by id or name).
    bool hasShredded(const String & col_name, Int64 col_id = 0) const
    {
        if (col_id != 0)
        {
            auto it = current_data_by_id_.find(col_id);
            if (it != current_data_by_id_.end() && it->second.data != nullptr)
                return true;
        }
        auto it = current_data_.find(col_name);
        return it != current_data_.end() && it->second.data != nullptr;
    }

    /// Clear the context after block processing.
    void clear()
    {
        current_data_.clear();
        current_data_by_id_.clear();
    }

private:
    JsonShreddedBlockContext() = default;

    struct BlockShreddedRef
    {
        const ShreddedJsonData * data = nullptr;
        size_t row_offset = 0;
        size_t row_count = 0;
    };
    std::unordered_map<String, BlockShreddedRef> current_data_;
    std::unordered_map<Int64, BlockShreddedRef> current_data_by_id_;
};

/// Manages persistent storage of shredded JSON sub-columns alongside DMFiles.
///
/// Sidecar layout on disk:
///   {dmfile_path}/.json_shredded/{col_name}/manifest.bin
///   {dmfile_path}/.json_shredded/{col_name}/{path_name}.bin  (null bitmap + column data)
///
/// The manifest contains: schema (paths + types), total rows, and pack offsets.
class JsonShreddedStore
{
public:
    /// Write shredded sub-columns to sidecar files alongside a DMFile.
    /// Called during writeIntoNewDMFile after shredding a JSON column's block.
    static void writeSidecar(
        const String & dmfile_path,
        const String & col_name,
        const ShreddedJsonData & data);

    /// Read ALL shredded sub-columns from sidecar files (legacy, used by segment merge).
    /// Returns nullopt if no sidecar exists for this DMFile/column.
    static std::optional<ShreddedJsonData> readSidecar(
        const String & dmfile_path,
        const String & col_name);

    /// Read the manifest only (paths, types, encodings, num_rows) without loading column data.
    /// Used by DMFileReader to create a lazy attachment that loads columns on demand.
    /// Returns manifest entries and num_rows. Empty vector if no sidecar exists.
    static std::vector<SidecarSchemaEntry> readSidecarManifest(
        const String & dmfile_path,
        const String & col_name,
        UInt64 & out_num_rows);

    /// Read a SINGLE sub-column from the sidecar by path name.
    /// Returns the typed Nullable column for that path, or nullptr if not found.
    /// This is the key optimization: reads only one .bin file (~1-2MB) instead of all 49 (~89MB).
    static ColumnPtr readSidecarColumn(
        const String & dmfile_path,
        const String & col_name,
        const String & path);

    /// Check if a sidecar exists for a given DMFile and column.
    static bool hasSidecar(const String & dmfile_path, const String & col_name);

    /// Get the sidecar directory path for a DMFile and column.
    static String sidecarPath(const String & dmfile_path, const String & col_name);

    /// In-memory cache of loaded sidecars to avoid repeated disk reads.
    /// Keyed by "{dmfile_path}/{col_name}".
    static const ShreddedJsonData * getCached(const String & dmfile_path, const String & col_name);

    /// Store data in cache (called after write or first read).
    static void putCache(const String & dmfile_path, const String & col_name, ShreddedJsonData && data);

    /// Per-column cache: caches individual sub-columns loaded on demand.
    /// Keyed by "{dmfile_path}/{col_name}/{path}".
    static ColumnPtr getCachedColumn(const String & dmfile_path, const String & col_name, const String & path);
    static void putCachedColumn(const String & dmfile_path, const String & col_name, const String & path, ColumnPtr col);

private:
    static std::mutex & cacheMutex()
    {
        static std::mutex mtx;
        return mtx;
    }

    static std::unordered_map<String, ShreddedJsonData> & cache()
    {
        static std::unordered_map<String, ShreddedJsonData> c;
        return c;
    }

    static std::unordered_map<String, ColumnPtr> & columnCache()
    {
        static std::unordered_map<String, ColumnPtr> c;
        return c;
    }

    struct ManifestCacheEntry
    {
        UInt64 num_rows;
        std::vector<SidecarSchemaEntry> entries;
    };

    static std::unordered_map<String, ManifestCacheEntry> & manifestCache()
    {
        static std::unordered_map<String, ManifestCacheEntry> c;
        return c;
    }

    static String cacheKey(const String & dmfile_path, const String & col_name)
    {
        return dmfile_path + "/" + col_name;
    }

    static String columnCacheKey(const String & dmfile_path, const String & col_name, const String & path)
    {
        return dmfile_path + "/" + col_name + "/" + path;
    }

    /// Internal: read a single .bin file given the manifest entry.
    static ColumnPtr readColumnFile(
        const String & dir,
        const String & path,
        UInt8 type,
        UInt8 encoding,
        UInt64 num_rows);

    static LoggerPtr log()
    {
        static auto logger = Logger::get("JsonShreddedStore");
        return logger;
    }
};

/// Helper: detect if a ColumnString likely contains binary JSON data.
/// TiDB binary JSON always starts with a type byte in range [0x01, 0x0d].
/// Returns true if the first non-empty row starts with a valid JSON type byte.
inline bool isBinaryJsonColumn(const ColumnString & col)
{
    for (size_t i = 0; i < std::min(col.size(), static_cast<size_t>(10)); ++i)
    {
        StringRef data = col.getDataAt(i);
        if (data.size == 0)
            continue;
        auto first_byte = static_cast<UInt8>(data.data[0]);
        // TiDB binary JSON type codes: 0x01=Object, 0x03=Array, 0x04=Literal,
        // 0x05=Int64, 0x06=UInt64, 0x07=Float64, 0x08=String, 0x09=Opaque,
        // 0x0a=Date, 0x0b=Datetime, 0x0c=Duration, 0x0d=BinaryJSON
        return first_byte >= 0x01 && first_byte <= 0x0d;
    }
    return false;
}

} // namespace DB::DM
