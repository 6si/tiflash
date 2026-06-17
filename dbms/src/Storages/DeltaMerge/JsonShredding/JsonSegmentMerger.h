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

#include <Storages/DeltaMerge/JsonShredding/JsonSchemaTree.h>
#include <Storages/DeltaMerge/JsonShredding/JsonShredder.h>

#include <vector>

namespace DB::DM
{

/// JsonSegmentMerger: Handles merging shredded JSON data across segments
/// during compaction and delta-to-stable flush.
///
/// Scenarios handled:
/// 1. Merge two segments with same schema → fast path (concatenate sub-columns)
/// 2. Merge two segments with different schemas → re-infer unified schema, re-shred
/// 3. Delta flush into stable → include delta rows in schema inference for new stable
/// 4. Cross-segment read with schema widening → fill NULLs for missing paths
class JsonSegmentMerger
{
public:
    explicit JsonSegmentMerger(const JsonShreddingConfig & config = {});

    /// Merge two ShreddedJsonData instances into one.
    /// If schemas are identical, uses the fast path (concatenate sub-columns).
    /// If schemas differ, re-shreds from the original blobs using a unified schema.
    ShreddedJsonData mergeSegments(
        const ShreddedJsonData & segment_a,
        const ShreddedJsonData & segment_b);

    /// Flush delta rows into an existing stable segment's shredded data.
    /// Re-infers schema from combined data (stable + delta) and re-shreds.
    /// Returns the new stable segment's shredded data.
    ShreddedJsonData flushDelta(
        const ShreddedJsonData & stable,
        const ColumnString & delta_rows);

    /// Read across multiple segments with potentially different schemas.
    /// Produces a unified view: paths present in any segment are included,
    /// with NULLs filled for segments that don't have a given path.
    /// Returns the unified sub-column for a specific path across all segments.
    static ColumnPtr readPathAcrossSegments(
        const std::vector<const ShreddedJsonData *> & segments,
        const String & path);

    /// Compute the unified schema across multiple segments.
    static JsonInferredSchema unifySchemas(
        const std::vector<const ShreddedJsonData *> & segments);

    /// Check if two schemas are identical (same paths, same types).
    static bool schemasMatch(
        const JsonInferredSchema & schema_a,
        const JsonInferredSchema & schema_b);

private:
    /// Fast path: schemas match, just concatenate sub-columns
    ShreddedJsonData mergeIdenticalSchemas(
        const ShreddedJsonData & segment_a,
        const ShreddedJsonData & segment_b);

    /// Slow path: re-shred both segments with a unified schema
    ShreddedJsonData mergeWithReShred(
        const ShreddedJsonData & segment_a,
        const ShreddedJsonData & segment_b,
        const JsonInferredSchema & unified_schema);

    JsonShreddingConfig config_;
    JsonShredder shredder_;
};

} // namespace DB::DM
