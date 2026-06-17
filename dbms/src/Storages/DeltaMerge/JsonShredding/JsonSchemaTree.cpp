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

#include <Storages/DeltaMerge/JsonShredding/JsonBinaryNavigator.h>
#include <Storages/DeltaMerge/JsonShredding/JsonSchemaTree.h>

#include <algorithm>

namespace DB::DM
{

String jsonLeafTypeToString(JsonLeafType t)
{
    switch (t)
    {
    case JsonLeafType::Int64:
        return "Int64";
    case JsonLeafType::UInt64:
        return "UInt64";
    case JsonLeafType::Float64:
        return "Float64";
    case JsonLeafType::String:
        return "String";
    case JsonLeafType::Bool:
        return "Bool";
    case JsonLeafType::Mixed:
        return "Mixed";
    case JsonLeafType::Null:
        return "Null";
    }
    return "Unknown";
}

String JsonSchemaNode::fullPath() const
{
    if (parent_path.empty())
        return key;
    return parent_path + "." + key;
}

// --- JsonSchemaTree implementation ---

JsonSchemaTree::JsonSchemaTree(const JsonShreddingConfig & config)
    : config_(config)
    , root_(std::make_shared<JsonSchemaNode>())
{
    root_->key = "";
    root_->is_leaf = false;
}

void JsonSchemaTree::addRow(const StringRef & json_binary_data)
{
    total_rows_++;

    if (json_binary_data.size == 0)
        return;

    non_null_rows_++;

    // Binary JSON: first byte is type, rest is data
    auto type_code = static_cast<JsonBinary::JsonType>(json_binary_data.data[0]);
    StringRef data(json_binary_data.data + 1, json_binary_data.size - 1);

    if (type_code == JsonBinary::TYPE_CODE_OBJECT)
    {
        root_->occurrence_count++;
        mergeObject(root_, data);
    }
    else if (type_code == JsonBinary::TYPE_CODE_ARRAY)
    {
        root_->occurrence_count++;
        root_->is_array = true;
        root_->is_un_inferable = true;
    }
    else
    {
        // Top-level scalar: not shred-able
        root_->occurrence_count++;
        root_->is_un_inferable = true;
    }
}

void JsonSchemaTree::addNull()
{
    total_rows_++;
}

void JsonSchemaTree::mergeObject(JsonSchemaNodePtr & node, const StringRef & obj_data)
{
    UInt32 elem_count = JsonBinaryNavigator::getObjectElementCount(obj_data);

    for (UInt32 i = 0; i < elem_count; ++i)
    {
        StringRef key_ref = JsonBinaryNavigator::getObjectKey(obj_data, i);
        if (key_ref.size == 0 && key_ref.data == nullptr)
            continue;

        String key_str(key_ref.data, key_ref.size);

        // Find or create child node
        auto it = node->children.find(key_str);
        if (it == node->children.end())
        {
            auto child = std::make_shared<JsonSchemaNode>();
            child->key = key_str;
            child->parent_path = node->key.empty() ? "" : node->fullPath();
            node->children[key_str] = child;
            it = node->children.find(key_str);
        }

        auto & child = it->second;
        child->occurrence_count++;

        // Get the value type and data
        auto val = JsonBinaryNavigator::getObjectValue(obj_data, i);

        if (val.type == JsonBinary::TYPE_CODE_OBJECT)
        {
            if (child->is_leaf && child->leaf_type != JsonLeafType::Null)
            {
                // Type conflict: was a leaf, now an object
                child->is_un_inferable = true;
            }
            else
            {
                child->is_leaf = false;
                mergeObject(child, val.data);
            }
        }
        else if (val.type == JsonBinary::TYPE_CODE_ARRAY)
        {
            child->is_array = true;
            child->is_leaf = true;
            child->leaf_type = JsonLeafType::Mixed;
        }
        else
        {
            // Scalar value
            if (!child->children.empty())
            {
                // Was previously an object, now a scalar — type conflict
                child->is_un_inferable = true;
            }
            else
            {
                child->is_leaf = true;
                mergeValue(child, val.type);
            }
        }
    }
}

void JsonSchemaTree::mergeValue(JsonSchemaNodePtr & node, JsonBinary::JsonType type_code)
{
    JsonLeafType new_type = typeCodeToLeafType(type_code);

    if (node->leaf_type == JsonLeafType::Null)
    {
        node->leaf_type = new_type;
    }
    else if (node->leaf_type != new_type)
    {
        node->leaf_type = JsonLeafType::Mixed;
    }
}

JsonLeafType JsonSchemaTree::typeCodeToLeafType(JsonBinary::JsonType type_code)
{
    switch (type_code)
    {
    case JsonBinary::TYPE_CODE_INT64:
        return JsonLeafType::Int64;
    case JsonBinary::TYPE_CODE_UINT64:
        return JsonLeafType::UInt64;
    case JsonBinary::TYPE_CODE_FLOAT64:
        return JsonLeafType::Float64;
    case JsonBinary::TYPE_CODE_STRING:
        return JsonLeafType::String;
    case JsonBinary::TYPE_CODE_LITERAL:
        return JsonLeafType::Bool;
    case JsonBinary::TYPE_CODE_ARRAY:
        return JsonLeafType::Mixed;
    case JsonBinary::TYPE_CODE_OBJECT:
        return JsonLeafType::Mixed;
    default:
        return JsonLeafType::Mixed;
    }
}

void JsonSchemaTree::pruneTree(JsonSchemaNodePtr & node, UInt64 parent_occurrence)
{
    if (node->is_un_inferable)
        return;

    // Check max children
    if (node->children.size() > config_.max_children_per_node)
    {
        node->is_un_inferable = true;
        node->children.clear();
        node->is_leaf = true;
        node->leaf_type = JsonLeafType::Mixed;
        return;
    }

    // Sparsity check: if too many children and average occurrence is too low
    if (node->children.size() > config_.sparse_children_check_threshold && parent_occurrence > 0)
    {
        UInt64 total_child_occurrences = 0;
        for (auto & [k, child] : node->children)
            total_child_occurrences += child->occurrence_count;

        UInt64 avg_occurrence = total_child_occurrences / node->children.size();
        if (avg_occurrence * config_.sparse_children_ratio < parent_occurrence)
        {
            node->is_un_inferable = true;
            node->children.clear();
            node->is_leaf = true;
            node->leaf_type = JsonLeafType::Mixed;
            return;
        }
    }

    // Recurse into children, pruning sparse ones
    for (auto it = node->children.begin(); it != node->children.end();)
    {
        auto & child = it->second;

        // Absolute sparsity: if this path appears in < 1/ratio of total rows, prune
        if (total_rows_ > 0
            && child->occurrence_count * config_.absolute_sparse_ratio < total_rows_)
        {
            it = node->children.erase(it);
            continue;
        }

        pruneTree(child, node->occurrence_count);
        ++it;
    }
}

void JsonSchemaTree::flattenTree(
    const JsonSchemaNodePtr & node,
    const String & prefix,
    std::vector<JsonShreddedColumn> & out)
{
    for (auto & [key, child] : node->children)
    {
        String path = prefix.empty() ? key : (prefix + "." + key);

        if (child->is_un_inferable)
        {
            out.push_back(JsonShreddedColumn{
                .path = path,
                .type = JsonLeafType::Mixed,
                .occurrence_count = child->occurrence_count,
                .is_array = child->is_array,
            });
        }
        else if (child->is_leaf || child->children.empty())
        {
            out.push_back(JsonShreddedColumn{
                .path = path,
                .type = child->leaf_type,
                .occurrence_count = child->occurrence_count,
                .is_array = child->is_array,
            });
        }
        else
        {
            flattenTree(child, path, out);
        }
    }
}

JsonInferredSchema JsonSchemaTree::finalize()
{
    JsonInferredSchema result;
    result.total_rows = total_rows_;
    result.rows_with_json = non_null_rows_;

    if (total_rows_ < config_.min_rows_for_inference)
        return result;

    if (root_->is_un_inferable)
        return result;

    // Phase 1: Prune the tree
    pruneTree(root_, total_rows_);

    // Phase 2: Flatten to leaf columns
    flattenTree(root_, "", result.columns);

    // Phase 3: Enforce max_leaves limit — keep most frequent paths
    if (result.columns.size() > config_.max_leaves)
    {
        std::sort(result.columns.begin(), result.columns.end(), [](const auto & a, const auto & b) {
            return a.occurrence_count > b.occurrence_count;
        });
        result.columns.resize(config_.max_leaves);
    }

    // Sort by path for deterministic output
    std::sort(result.columns.begin(), result.columns.end(), [](const auto & a, const auto & b) {
        return a.path < b.path;
    });

    return result;
}

void JsonSchemaTree::reset()
{
    root_ = std::make_shared<JsonSchemaNode>();
    root_->key = "";
    root_->is_leaf = false;
    total_rows_ = 0;
    non_null_rows_ = 0;
}

} // namespace DB::DM
