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

#include <Columns/ColumnString.h>
#include <Core/Field.h>
#include <Storages/DeltaMerge/JsonShredding/JsonSchemaTree.h>
#include <Storages/DeltaMerge/JsonShredding/JsonShredder.h>
#include <Storages/DeltaMerge/JsonShredding/JsonShreddingConfig.h>

namespace DB::DM
{

/// JsonPathOptimizer: Query-time component that routes json_extract operations
/// to either the shredded sub-column (fast path) or the original blob (fallback).
///
/// Usage in TiFlash execution:
///   1. When TiFlash processes a json_extract(col, '$.path') expression:
///      - Check if flag is ON and the path exists in shredded form
///      - If yes: read directly from the typed sub-column (no parsing)
///      - If no: fall back to reading the full blob and extracting at runtime
///
///   2. When TiFlash processes a filter like json_extract(col, '$.status') = 'active':
///      - Check if flag is ON and '$.status' is shredded
///      - If yes: apply filter directly on the String sub-column (or dictionary-encoded)
///      - If no: fall back to per-row JSON parsing + string comparison
class JsonPathOptimizer
{
public:
    /// Try to read a JSON path from shredded sub-columns.
    /// Returns the column of extracted values if available (flag ON + path exists),
    /// or nullptr if the fast path is not available.
    static ColumnPtr tryReadShredded(
        const ShreddedJsonData & shredded_data,
        const String & json_path);

    /// Extract a JSON path with automatic routing:
    /// - If shredded data available + flag ON → read from sub-column
    /// - Otherwise → extract from blob column
    static ColumnPtr extractJsonPath(
        const ShreddedJsonData & shredded_data,
        const String & json_path,
        JsonLeafType type_hint = JsonLeafType::String);

    /// Check if a path can be served from shredded sub-columns.
    static bool canUseShredded(const ShreddedJsonData & shredded_data, const String & json_path);

    /// Apply a filter predicate on a shredded JSON path.
    /// Returns a bitmap (column of UInt8) where 1 = row passes filter.
    /// Supports equality, comparison, IS NULL, and LIKE predicates.
    enum class FilterOp
    {
        Equal,
        NotEqual,
        LessThan,
        LessOrEqual,
        GreaterThan,
        GreaterOrEqual,
        IsNull,
        IsNotNull,
        Like,
    };

    static MutableColumnPtr applyFilter(
        const ShreddedJsonData & shredded_data,
        const String & json_path,
        FilterOp op,
        const Field & value);

    /// Convert a TiDB JSON path expression (e.g., "$.details.publisher")
    /// to our internal dot-separated format (e.g., "details.publisher").
    static String normalizePath(const String & tidb_path);
};

} // namespace DB::DM
