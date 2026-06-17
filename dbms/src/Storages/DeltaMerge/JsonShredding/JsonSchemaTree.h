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
#include <TiDB/Decode/JsonBinary.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB::DM
{

/// The inferred type for a JSON leaf path after schema inference.
enum class JsonLeafType : UInt8
{
    Int64 = 0,
    UInt64 = 1,
    Float64 = 2,
    String = 3,
    Bool = 4,
    Mixed = 5, // Multiple types observed — store as raw JSON string
    Null = 6, // Only nulls observed so far
};

String jsonLeafTypeToString(JsonLeafType t);

/// Represents one node in the inferred JSON schema tree.
/// Each node corresponds to a JSON key at some nesting level.
struct JsonSchemaNode
{
    String key; // This node's key name (empty for root)
    JsonLeafType leaf_type = JsonLeafType::Null;
    bool is_leaf = false;
    bool is_array = false;
    bool is_un_inferable = false; // Marked when type conflicts or sparsity threshold hit
    UInt64 occurrence_count = 0; // How many rows contain this path

    /// Children keyed by their name
    std::unordered_map<String, std::shared_ptr<JsonSchemaNode>> children;

    /// Full dot-separated path from root (e.g., "details.publisher")
    String fullPath() const;

    /// Set by parent during tree construction
    String parent_path;
};

using JsonSchemaNodePtr = std::shared_ptr<JsonSchemaNode>;

/// Configuration for JSON schema inference.
struct JsonShreddingConfig
{
    /// Maximum number of leaf columns to infer per segment.
    /// If inference produces more leaves than this, prune lowest-frequency paths.
    UInt32 max_leaves = 128;

    /// Maximum number of children a single node can have before being marked un-inferable.
    UInt32 max_children_per_node = 64;

    /// If a node has more children than this threshold, apply sparsity check.
    UInt32 sparse_children_check_threshold = 8;

    /// Sparsity ratio: if average child occurrence / parent occurrence < 1/ratio, mark un-inferable.
    UInt32 sparse_children_ratio = 100; // 1% threshold

    /// Absolute sparsity: if a leaf path appears in < 1/ratio of total rows, prune it.
    UInt32 absolute_sparse_ratio = 100; // 1% threshold

    /// Minimum rows in a segment to trigger schema inference (skip for tiny segments).
    UInt64 min_rows_for_inference = 100;
};

/// A flattened leaf path with its inferred type, ready for columnar storage.
struct JsonShreddedColumn
{
    String path; // Dot-separated keypath (e.g., "details.publisher")
    JsonLeafType type; // Inferred type for this path
    UInt64 occurrence_count; // How many rows had this path present
    bool is_array; // If this path points to an array
};

/// The result of schema inference on a set of JSON rows.
struct JsonInferredSchema
{
    std::vector<JsonShreddedColumn> columns; // Leaf paths to shred into physical columns
    UInt64 total_rows = 0; // Total rows analyzed
    UInt64 rows_with_json = 0; // Rows that had non-null JSON

    bool empty() const { return columns.empty(); }
    size_t numColumns() const { return columns.size(); }

    /// Check if a specific path exists in this schema.
    bool hasPath(const String & path) const
    {
        for (const auto & col : columns)
            if (col.path == path)
                return true;
        return false;
    }

    /// Get the type for a specific path, or Null if not found.
    JsonLeafType getPathType(const String & path) const
    {
        for (const auto & col : columns)
            if (col.path == path)
                return col.type;
        return JsonLeafType::Null;
    }
};

/// JsonSchemaTree: The core schema inference engine.
///
/// Given a batch of binary JSON values (as stored in TiFlash's ColumnString),
/// infers a schema tree by scanning the JSON structure, then flattens it into
/// a set of leaf paths suitable for columnar storage.
///
/// Follows the SingleStore approach:
/// 1. Loop through JSON rows, merge keypaths into a tree
/// 2. Detect type conflicts → mark as un-inferable (store as raw string)
/// 3. Apply sparsity pruning → paths appearing in <1% of rows stored as blob
/// 4. Limit total leaves to max_leaves
class JsonSchemaTree
{
public:
    explicit JsonSchemaTree(const JsonShreddingConfig & config = {});

    /// Add a single binary JSON value to the schema tree.
    /// The data should be the raw binary JSON bytes (as stored in ColumnString).
    void addRow(const StringRef & json_binary_data);

    /// Add a NULL JSON value (row has no JSON data).
    void addNull();

    /// After all rows have been added, finalize the schema:
    /// apply pruning, sparsity checks, and flatten to leaf columns.
    JsonInferredSchema finalize();

    /// Reset the tree for reuse.
    void reset();

    UInt64 totalRows() const { return total_rows_; }

    /// Merge two finalized schemas into one unified schema.
    /// Union of all paths; type conflicts resolved via promotion to Mixed.
    /// Occurrence counts are summed; total_rows is summed.
    static JsonInferredSchema mergeSchemas(
        const JsonInferredSchema & schema_a,
        const JsonInferredSchema & schema_b);

    /// Promote two leaf types to a common type.
    /// Same types stay; different types promote to Mixed.
    static JsonLeafType promoteTypes(JsonLeafType a, JsonLeafType b);

private:
    void mergeObject(JsonSchemaNodePtr & node, const StringRef & obj_data);
    void mergeValue(JsonSchemaNodePtr & node, JsonBinary::JsonType type_code);

    void pruneTree(JsonSchemaNodePtr & node, UInt64 parent_occurrence);
    void flattenTree(
        const JsonSchemaNodePtr & node,
        const String & prefix,
        std::vector<JsonShreddedColumn> & out);

    static JsonLeafType typeCodeToLeafType(JsonBinary::JsonType type_code);

    JsonShreddingConfig config_;
    JsonSchemaNodePtr root_;
    UInt64 total_rows_ = 0;
    UInt64 non_null_rows_ = 0;
};

} // namespace DB::DM
