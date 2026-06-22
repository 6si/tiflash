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
#include <Storages/DeltaMerge/JsonShredding/JsonSegmentMerger.h>

namespace DB::DM
{

JsonSegmentMerger::JsonSegmentMerger(const JsonShreddingConfig & config)
    : config_(config)
    , shredder_(config)
{}

ShreddedJsonData JsonSegmentMerger::mergeSegments(
    const ShreddedJsonData & segment_a,
    const ShreddedJsonData & segment_b)
{
    // If either segment has no sub-columns, re-shred from blobs
    if (segment_a.sub_columns.empty() && segment_b.sub_columns.empty())
    {
        // Both are pre-shredding segments — just concatenate blobs
        ShreddedJsonData result;
        auto merged_blob = ColumnString::create();

        if (segment_a.original_blob)
        {
            const auto & blob_a = assert_cast<const ColumnString &>(*segment_a.original_blob);
            for (size_t i = 0; i < blob_a.size(); ++i)
            {
                auto ref = blob_a.getDataAt(i);
                merged_blob->insertData(ref.data, ref.size);
            }
        }
        if (segment_b.original_blob)
        {
            const auto & blob_b = assert_cast<const ColumnString &>(*segment_b.original_blob);
            for (size_t i = 0; i < blob_b.size(); ++i)
            {
                auto ref = blob_b.getDataAt(i);
                merged_blob->insertData(ref.data, ref.size);
            }
        }

        result.original_blob = std::move(merged_blob);
        // No sub-columns — backward compat path
        return result;
    }

    // Check if schemas match for fast path
    if (schemasMatch(segment_a.schema, segment_b.schema))
        return mergeIdenticalSchemas(segment_a, segment_b);

    // Schemas differ — must re-infer and re-shred
    auto unified_schema = JsonSchemaTree::mergeSchemas(segment_a.schema, segment_b.schema);
    return mergeWithReShred(segment_a, segment_b, unified_schema);
}

ShreddedJsonData JsonSegmentMerger::flushDelta(
    const ShreddedJsonData & stable,
    const ColumnString & delta_rows)
{
    // Combine stable blob + delta rows into one column, then re-shred
    auto combined = ColumnString::create();

    // Add stable rows
    if (stable.original_blob)
    {
        const auto & stable_blob = assert_cast<const ColumnString &>(*stable.original_blob);
        for (size_t i = 0; i < stable_blob.size(); ++i)
        {
            auto ref = stable_blob.getDataAt(i);
            combined->insertData(ref.data, ref.size);
        }
    }

    // Add delta rows
    for (size_t i = 0; i < delta_rows.size(); ++i)
    {
        auto ref = delta_rows.getDataAt(i);
        combined->insertData(ref.data, ref.size);
    }

    // Re-infer schema from combined data and re-shred
    return shredder_.shred(*combined);
}

ColumnPtr JsonSegmentMerger::readPathAcrossSegments(
    const std::vector<const ShreddedJsonData *> & segments,
    const String & path)
{
    // For each segment, read the path's sub-column if it exists,
    // otherwise fill NULLs for that segment's rows.
    MutableColumnPtr result_inner = ColumnString::create();
    auto result = ColumnNullable::create(std::move(result_inner), ColumnUInt8::create());

    for (const auto * seg : segments)
    {
        if (!seg || !seg->original_blob)
            continue;

        size_t num_rows = seg->original_blob->size();

        // Check if this segment has the path shredded
        ColumnPtr sub_col = JsonSubColumnReader::readPath(*seg, path);
        if (sub_col)
        {
            // Segment has this path — copy the sub-column values preserving null_map sentinels
            const auto & src_nullable = assert_cast<const ColumnNullable &>(*sub_col);
            auto & dst_nullable = assert_cast<ColumnNullable &>(*result);
            dst_nullable.getNestedColumn().insertRangeFrom(
                src_nullable.getNestedColumn(), 0, src_nullable.size());
            const auto & src_null_map = src_nullable.getNullMapData();
            auto & dst_null_map = dst_nullable.getNullMapData();
            dst_null_map.insert(dst_null_map.end(), src_null_map.begin(), src_null_map.end());
        }
        else
        {
            // Segment doesn't have this path — fill NULLs
            for (size_t i = 0; i < num_rows; ++i)
                result->insertDefault();
        }
    }

    return result;
}

JsonInferredSchema JsonSegmentMerger::unifySchemas(
    const std::vector<const ShreddedJsonData *> & segments)
{
    JsonInferredSchema unified;
    for (const auto * seg : segments)
    {
        if (!seg)
            continue;
        unified = JsonSchemaTree::mergeSchemas(unified, seg->schema);
    }
    return unified;
}

bool JsonSegmentMerger::schemasMatch(
    const JsonInferredSchema & schema_a,
    const JsonInferredSchema & schema_b)
{
    if (schema_a.numColumns() != schema_b.numColumns())
        return false;

    // Both schemas are sorted by path (guaranteed by finalize/mergeSchemas)
    for (size_t i = 0; i < schema_a.columns.size(); ++i)
    {
        if (schema_a.columns[i].path != schema_b.columns[i].path)
            return false;
        if (schema_a.columns[i].type != schema_b.columns[i].type)
            return false;
    }
    return true;
}

ShreddedJsonData JsonSegmentMerger::mergeIdenticalSchemas(
    const ShreddedJsonData & segment_a,
    const ShreddedJsonData & segment_b)
{
    ShreddedJsonData result;

    // Concatenate blobs
    auto merged_blob = ColumnString::create();
    if (segment_a.original_blob)
    {
        const auto & blob_a = assert_cast<const ColumnString &>(*segment_a.original_blob);
        for (size_t i = 0; i < blob_a.size(); ++i)
        {
            auto ref = blob_a.getDataAt(i);
            merged_blob->insertData(ref.data, ref.size);
        }
    }
    if (segment_b.original_blob)
    {
        const auto & blob_b = assert_cast<const ColumnString &>(*segment_b.original_blob);
        for (size_t i = 0; i < blob_b.size(); ++i)
        {
            auto ref = blob_b.getDataAt(i);
            merged_blob->insertData(ref.data, ref.size);
        }
    }
    result.original_blob = std::move(merged_blob);

    // Schema is the same — just copy it
    result.schema = segment_a.schema;
    result.schema.total_rows = segment_a.schema.total_rows + segment_b.schema.total_rows;
    result.schema.rows_with_json = segment_a.schema.rows_with_json + segment_b.schema.rows_with_json;

    // Concatenate sub-columns
    for (size_t col_idx = 0; col_idx < segment_a.sub_columns.size(); ++col_idx)
    {
        const auto & sub_a = segment_a.sub_columns[col_idx];
        const auto & sub_b = segment_b.sub_columns[col_idx];

        JsonSubColumn merged_sub;
        merged_sub.path = sub_a.path;
        merged_sub.type = sub_a.type;
        merged_sub.is_array = sub_a.is_array;

        // Clone sub_a's data and append sub_b's data.
        // Must use insertRangeFrom (not insert-via-Field) to preserve null_map=2 sentinel
        // (JSON null literal: key present, value is JSON null — distinct from SQL NULL).
        auto cloned = sub_a.data->cloneFullColumn();
        if (sub_b.data && sub_b.data->size() > 0)
        {
            auto & cloned_nullable = assert_cast<ColumnNullable &>(*cloned);
            const auto & src_nullable = assert_cast<const ColumnNullable &>(*sub_b.data);
            // Append nested column data verbatim (preserves raw bytes)
            cloned_nullable.getNestedColumn().insertRangeFrom(
                src_nullable.getNestedColumn(), 0, src_nullable.size());
            // Append null_map verbatim (preserves sentinel value 2 for JSON null)
            const auto & src_null_map = src_nullable.getNullMapData();
            auto & dst_null_map = cloned_nullable.getNullMapData();
            dst_null_map.insert(dst_null_map.end(), src_null_map.begin(), src_null_map.end());
        }
        merged_sub.data = std::move(cloned);

        result.sub_columns.push_back(std::move(merged_sub));
    }

    // Update occurrence counts in schema
    for (size_t i = 0; i < result.schema.columns.size(); ++i)
    {
        result.schema.columns[i].occurrence_count
            = segment_a.schema.columns[i].occurrence_count + segment_b.schema.columns[i].occurrence_count;
    }

    return result;
}

ShreddedJsonData JsonSegmentMerger::mergeWithReShred(
    const ShreddedJsonData & segment_a,
    const ShreddedJsonData & segment_b,
    const JsonInferredSchema & unified_schema)
{
    // Combine all blobs and re-shred with the unified schema
    auto combined = ColumnString::create();

    if (segment_a.original_blob)
    {
        const auto & blob_a = assert_cast<const ColumnString &>(*segment_a.original_blob);
        for (size_t i = 0; i < blob_a.size(); ++i)
        {
            auto ref = blob_a.getDataAt(i);
            combined->insertData(ref.data, ref.size);
        }
    }
    if (segment_b.original_blob)
    {
        const auto & blob_b = assert_cast<const ColumnString &>(*segment_b.original_blob);
        for (size_t i = 0; i < blob_b.size(); ++i)
        {
            auto ref = blob_b.getDataAt(i);
            combined->insertData(ref.data, ref.size);
        }
    }

    // Re-shred using the unified schema
    return shredder_.shredWithSchema(*combined, unified_schema);
}

} // namespace DB::DM
