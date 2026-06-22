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
#include <Core/Field.h>
#include <Storages/DeltaMerge/JsonShredding/JsonBinaryNavigator.h>
#include <Storages/DeltaMerge/JsonShredding/JsonSchemaTree.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB::DM
{

/// Represents a single shredded sub-column for a JSON path.
/// Each sub-column stores: values + definition level (present/null).
struct JsonSubColumn
{
    String path; // e.g., "details.publisher"
    JsonLeafType type; // Inferred type
    bool is_array; // Whether this path is an array

    /// The actual column data. Nullable — NULL means the path was not present in that row.
    /// For Int64 paths: ColumnNullable(ColumnInt64)
    /// For UInt64 paths: ColumnNullable(ColumnUInt64)
    /// For Float64 paths: ColumnNullable(ColumnFloat64)
    /// For String/Mixed paths: ColumnNullable(ColumnString)
    /// For Bool paths: ColumnNullable(ColumnUInt8)
    ///
    /// null_map encoding:
    ///   0 = value present (non-null)
    ///   1 = key absent → json_extract returns SQL NULL
    ///   2 = key present but value is JSON null literal → json_extract returns JSON null, not SQL NULL
    MutableColumnPtr data;

    size_t rows() const { return data ? data->size() : 0; }
};

/// Result of shredding a batch of JSON rows.
struct ShreddedJsonData
{
    /// The original blob column (preserved for backward compat / flag-OFF reads)
    ColumnPtr original_blob;

    /// The shredded sub-columns, keyed by path
    std::vector<JsonSubColumn> sub_columns;

    /// The schema that was used for shredding
    JsonInferredSchema schema;

    size_t numRows() const { return original_blob ? original_blob->size() : 0; }
    size_t numSubColumns() const { return sub_columns.size(); }
};

/// JsonShredder: Takes a ColumnString of binary JSON blobs and produces
/// both the original column AND shredded sub-columns.
///
/// Write path (always executes — no flag check):
/// 1. Infer schema from the batch of JSON rows
/// 2. For each row, extract values at each inferred leaf path
/// 3. Store extracted values in typed sub-columns (with NULL for missing paths)
/// 4. Return both original blob and sub-columns
///
/// The flag only controls the READ path (which representation to use for queries).
class JsonShredder
{
public:
    explicit JsonShredder(const JsonShreddingConfig & config = {});

    /// Shred a column of binary JSON blobs into sub-columns.
    /// The input column is a ColumnString where each row is a binary JSON value.
    /// Returns the shredded result containing both original blob and sub-columns.
    ShreddedJsonData shred(const ColumnString & json_column);

    /// Shred using a pre-computed schema (e.g., from a previous segment for schema reuse).
    ShreddedJsonData shredWithSchema(
        const ColumnString & json_column,
        const JsonInferredSchema & schema);

    /// Extract a single path's values from a JSON column.
    /// Used by the query read path to get just one sub-column on demand.
    static MutableColumnPtr extractPath(
        const ColumnString & json_column,
        const String & path,
        JsonLeafType expected_type);

private:
    /// Initialize sub-columns based on schema
    std::vector<JsonSubColumn> initSubColumns(const JsonInferredSchema & schema, size_t num_rows);

    /// Extract values from a single JSON row into the sub-columns
    void extractRow(
        const StringRef & json_data,
        const JsonInferredSchema & schema,
        std::vector<JsonSubColumn> & sub_columns);

    /// Navigate binary JSON to find value at a dot-separated path (legacy, unused)
    static bool navigateToPath(
        const JsonBinary & root,
        const String & path,
        JsonBinary & out_value,
        JsonBinary::JsonType & out_type);

    /// Convert a navigator ValueRef to a Field of the target type
    static Field jsonValueToField(const JsonBinaryNavigator::ValueRef & value, JsonLeafType target_type);

    JsonShreddingConfig config_;
};

/// JsonSubColumnReader: Reads shredded sub-columns and reconstructs
/// values for query execution.
///
/// Used when the flag is ON — reads from sub-columns instead of the blob.
class JsonSubColumnReader
{
public:
    /// Read a specific path from shredded data, returning a typed column.
    /// Returns nullptr if the path doesn't exist in the shredded schema.
    static ColumnPtr readPath(
        const ShreddedJsonData & data,
        const String & path);

    /// Reconstruct the full JSON blob from sub-columns (for queries that need the whole object).
    /// Falls back to the original blob column.
    static ColumnPtr readFullBlob(const ShreddedJsonData & data);

    /// Check if a path is available in shredded form.
    static bool hasPath(const ShreddedJsonData & data, const String & path);
};

} // namespace DB::DM
