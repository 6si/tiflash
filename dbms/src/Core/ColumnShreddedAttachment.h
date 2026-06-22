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

#include <Core/Types.h>

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB::DM
{

struct ShreddedJsonData;

/// Schema entry from the sidecar manifest (path + type + encoding info).
/// Used by lazy loading to know what paths are available without reading column data.
struct SidecarSchemaEntry
{
    String path;
    UInt8 type;
    bool is_array;
    UInt8 encoding;
};

/// Lightweight attachment that carries shredded JSON sub-column data with a column
/// through the pipeline. This survives thread handoffs (reader pool → MPP worker pool)
/// and column renames (PROJECT actions).
///
/// Supports two modes:
///   - Eager (legacy): `data` is populated with all sub-columns pre-loaded.
///   - Lazy (preferred): `data` is null, but `dmfile_path`/`col_name`/`manifest_entries`
///     are set. Individual paths are loaded on-demand via `loadColumn()`.
///
/// Lifecycle:
///   1. DMFileReader reads sidecar manifest and attaches to the JSON column
///   2. Column flows through pipeline (PROJECT renames, filter, etc.)
///   3. FunctionJsonExtract calls `loadColumn(path)` to read just the needed sub-column
///   4. Attachment is reference-counted (shared_ptr) — zero cost when null
struct ColumnShreddedAttachment
{
    /// Shared reference to the full sidecar data (cached, covers entire DMFile).
    /// Multiple packs from the same DMFile share this via shared_ptr.
    /// In lazy mode, this is null — use loadColumn() instead.
    std::shared_ptr<const ShreddedJsonData> data;

    /// Row range within the sidecar that corresponds to this block/pack.
    size_t row_offset = 0;
    size_t row_count = 0;

    /// Lazy loading fields: set when data is null (manifest-only mode).
    String dmfile_path;
    String col_name;
    UInt64 num_rows = 0;
    std::vector<SidecarSchemaEntry> manifest_entries;
    /// O(1) path lookup index (built from manifest_entries).
    std::unordered_map<String, size_t> path_index;

    /// Build the O(1) path index from manifest_entries. Call after populating manifest_entries.
    void buildPathIndex()
    {
        path_index.reserve(manifest_entries.size());
        for (size_t i = 0; i < manifest_entries.size(); ++i)
            path_index[manifest_entries[i].path] = i;
    }

    /// O(1) check if a specific path is available in this sidecar.
    bool hasPath(const String & path) const { return path_index.count(path) > 0; }

    /// Whether this attachment is in lazy mode (manifest loaded, data not loaded).
    bool isLazy() const { return !data && !dmfile_path.empty(); }

    /// True when this column belongs to an NGC DMFile (no sidecar). Used as a sentinel
    /// to block the findByColName() fallback in FunctionsJson/FunctionJsonShreddedFilter,
    /// preventing stale sidecar attachments from poisoning real-blob column reads.
    bool is_ngc = false;

    /// MVCC row-selection filter: non-empty when MVCC filtering reduced the block's row
    /// count below row_count. Each element is 1 (row survived) or 0 (row filtered out).
    /// Length == row_count. FunctionsJson applies this to sub_col to get the right rows.
    std::vector<UInt8> mvcc_filter;
};

using ColumnShreddedAttachmentPtr = std::shared_ptr<const ColumnShreddedAttachment>;

} // namespace DB::DM
