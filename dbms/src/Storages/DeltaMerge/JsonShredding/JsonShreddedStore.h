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
    /// data: the shredded sub-columns for the ENTIRE DMFile
    /// row_offset: starting row within the DMFile for this pack batch
    /// row_count: number of rows in this pack batch
    void setForCurrentBlock(const String & col_name, const ShreddedJsonData & data, size_t row_offset, size_t row_count)
    {
        current_data_[col_name] = BlockShreddedRef{&data, row_offset, row_count};
    }

    /// Get a specific sub-column for a path from the current block's shredded data.
    /// Returns a column sliced to the current pack's row range.
    /// Returns nullptr if not available or if the path doesn't exist in the sidecar.
    ColumnPtr getSubColumn(const String & col_name, const String & path) const
    {
        auto it = current_data_.find(col_name);
        if (it == current_data_.end() || it->second.data == nullptr)
            return nullptr;
        auto full_col = JsonSubColumnReader::readPath(*it->second.data, path);
        if (!full_col)
            return nullptr;
        const auto & ref = it->second;
        if (full_col->size() == ref.row_count)
            return full_col;
        if (full_col->size() < ref.row_offset + ref.row_count)
            return nullptr;
        return full_col->cut(ref.row_offset, ref.row_count);
    }

    /// Check if shredded data is available for a column.
    bool hasShredded(const String & col_name) const
    {
        auto it = current_data_.find(col_name);
        return it != current_data_.end() && it->second.data != nullptr;
    }

    /// Clear the context after block processing.
    void clear() { current_data_.clear(); }

private:
    JsonShreddedBlockContext() = default;

    struct BlockShreddedRef
    {
        const ShreddedJsonData * data = nullptr;
        size_t row_offset = 0;
        size_t row_count = 0;
    };
    std::unordered_map<String, BlockShreddedRef> current_data_;
};

/// Manages persistent storage of shredded JSON sub-columns alongside DMFiles.
///
/// Sidecar layout on disk:
///   {dmfile_path}/.json_shredded/{col_name}/manifest.bin
///   {dmfile_path}/.json_shredded/{col_name}/{path_name}.null.bin  (null bitmap)
///   {dmfile_path}/.json_shredded/{col_name}/{path_name}.data.bin  (column data)
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

    /// Read shredded sub-columns from sidecar files.
    /// Returns nullopt if no sidecar exists for this DMFile/column.
    static std::optional<ShreddedJsonData> readSidecar(
        const String & dmfile_path,
        const String & col_name);

    /// Check if a sidecar exists for a given DMFile and column.
    static bool hasSidecar(const String & dmfile_path, const String & col_name);

    /// Get the sidecar directory path for a DMFile and column.
    static String sidecarPath(const String & dmfile_path, const String & col_name);

    /// In-memory cache of loaded sidecars to avoid repeated disk reads.
    /// Keyed by "{dmfile_path}/{col_name}".
    static const ShreddedJsonData * getCached(const String & dmfile_path, const String & col_name);

    /// Store data in cache (called after write or first read).
    static void putCache(const String & dmfile_path, const String & col_name, ShreddedJsonData && data);

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

    static String cacheKey(const String & dmfile_path, const String & col_name)
    {
        return dmfile_path + "/" + col_name;
    }

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
