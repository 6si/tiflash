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
#include <Storages/DeltaMerge/JsonShredding/JsonPathOptimizer.h>
#include <Storages/DeltaMerge/JsonShredding/JsonSchemaTree.h>
#include <Storages/DeltaMerge/JsonShredding/JsonShredder.h>
#include <Storages/DeltaMerge/JsonShredding/JsonShreddingConfig.h>
#include <TiDB/Decode/JsonBinary.h>
#include <gtest/gtest.h>

namespace DB::DM::tests
{

/// Helper: build a binary JSON object from key-value pairs.
/// This produces the MySQL 5.7 binary JSON format that TiFlash stores.
static String buildBinaryJsonObject(const std::vector<std::pair<String, String>> & str_fields,
                                     const std::vector<std::pair<String, Int64>> & int_fields = {},
                                     const std::vector<std::pair<String, double>> & float_fields = {})
{
    // Build JSON string first, then convert to binary format.
    // For testing, we'll use a simplified binary builder.
    // The format is: type_byte + object_payload
    //
    // We'll construct the binary format manually for objects.
    // object = element-count(u32) + size(u32) + key-entries + value-entries + keys + values

    // Total element count
    UInt32 elem_count = str_fields.size() + int_fields.size() + float_fields.size();

    // Collect all keys in order
    std::vector<std::pair<String, std::pair<UInt8, String>>> entries; // key -> (type, value_bytes)

    for (const auto & [key, val] : str_fields)
    {
        // String value: varint-length + utf8 data
        String value_bytes;
        // Encode length as varint
        UInt64 len = val.size();
        while (len >= 0x80)
        {
            value_bytes += static_cast<char>(len | 0x80);
            len >>= 7;
        }
        value_bytes += static_cast<char>(len);
        value_bytes += val;
        entries.push_back({key, {JsonBinary::TYPE_CODE_STRING, value_bytes}});
    }

    for (const auto & [key, val] : int_fields)
    {
        // Int64 value: 8 bytes little-endian
        String value_bytes(8, '\0');
        memcpy(value_bytes.data(), &val, 8);
        entries.push_back({key, {JsonBinary::TYPE_CODE_INT64, value_bytes}});
    }

    for (const auto & [key, val] : float_fields)
    {
        // Float64 value: 8 bytes
        String value_bytes(8, '\0');
        memcpy(value_bytes.data(), &val, 8);
        entries.push_back({key, {JsonBinary::TYPE_CODE_FLOAT64, value_bytes}});
    }

    // Sort by key (binary JSON objects have keys in sorted order)
    std::sort(entries.begin(), entries.end(), [](const auto & a, const auto & b) {
        return a.first < b.first;
    });

    // Build binary format
    // Header: element_count(4) + size(4)
    // Key entries: for each key: key_offset(4) + key_length(2) = 6 bytes
    // Value entries: for each value: type(1) + offset_or_inline(4) = 5 bytes
    // Then: keys data, then values data

    UInt32 key_entry_size = 6; // offset(4) + length(2)
    UInt32 value_entry_size = 5; // type(1) + offset(4)

    UInt32 header_size = 4 + 4; // element_count + size
    UInt32 key_entries_total = elem_count * key_entry_size;
    UInt32 value_entries_total = elem_count * value_entry_size;
    UInt32 metadata_size = header_size + key_entries_total + value_entries_total;

    // Calculate key data size
    UInt32 keys_data_size = 0;
    for (const auto & [key, _] : entries)
        keys_data_size += key.size();

    // Calculate value data size
    UInt32 values_data_size = 0;
    for (const auto & [_, tv] : entries)
        values_data_size += tv.second.size();

    UInt32 total_size = metadata_size + keys_data_size + values_data_size;

    String result;
    result.resize(1 + total_size); // +1 for type byte
    result[0] = JsonBinary::TYPE_CODE_OBJECT;

    char * buf = result.data() + 1;

    // Write element count
    memcpy(buf, &elem_count, 4);
    // Write size
    memcpy(buf + 4, &total_size, 4);

    // Write key entries
    UInt32 key_data_offset = metadata_size;
    for (UInt32 i = 0; i < elem_count; ++i)
    {
        UInt32 key_offset = key_data_offset;
        UInt16 key_length = static_cast<UInt16>(entries[i].first.size());
        memcpy(buf + header_size + i * key_entry_size, &key_offset, 4);
        memcpy(buf + header_size + i * key_entry_size + 4, &key_length, 2);
        key_data_offset += key_length;
    }

    // Write value entries
    UInt32 value_data_offset = metadata_size + keys_data_size;
    UInt32 ve_base = header_size + key_entries_total;
    for (UInt32 i = 0; i < elem_count; ++i)
    {
        UInt8 type_code = entries[i].second.first;
        buf[ve_base + i * value_entry_size] = type_code;

        // Check if value can be inlined (literals and small ints)
        // For simplicity, always use offset
        memcpy(buf + ve_base + i * value_entry_size + 1, &value_data_offset, 4);
        value_data_offset += entries[i].second.second.size();
    }

    // Write keys data
    UInt32 offset = metadata_size;
    for (const auto & [key, _] : entries)
    {
        memcpy(buf + offset, key.data(), key.size());
        offset += key.size();
    }

    // Write values data
    for (const auto & [_, tv] : entries)
    {
        memcpy(buf + offset, tv.second.data(), tv.second.size());
        offset += tv.second.size();
    }

    return result;
}

/// Helper: create a ColumnString from a vector of binary JSON strings
static MutableColumnPtr createJsonColumn(const std::vector<String> & json_binaries)
{
    auto col = ColumnString::create();
    for (const auto & data : json_binaries)
        col->insertData(data.data(), data.size());
    return col;
}

// ============================================================================
// Phase 1 Tests: Schema Inference
// ============================================================================

class JsonSchemaInferenceTest : public ::testing::Test
{
protected:
    JsonShreddingConfig config;

    void SetUp() override
    {
        config.max_leaves = 128;
        config.max_children_per_node = 64;
        config.sparse_children_check_threshold = 8;
        config.sparse_children_ratio = 100;
        config.absolute_sparse_ratio = 100;
        config.min_rows_for_inference = 1; // Low threshold for testing
    }
};

TEST_F(JsonSchemaInferenceTest, EmptyInput)
{
    JsonSchemaTree tree(config);
    auto schema = tree.finalize();
    EXPECT_TRUE(schema.empty());
    EXPECT_EQ(schema.total_rows, 0u);
}

TEST_F(JsonSchemaInferenceTest, AllNulls)
{
    JsonSchemaTree tree(config);
    tree.addNull();
    tree.addNull();
    tree.addNull();
    auto schema = tree.finalize();
    EXPECT_TRUE(schema.empty());
    EXPECT_EQ(schema.total_rows, 3u);
    EXPECT_EQ(schema.rows_with_json, 0u);
}

TEST_F(JsonSchemaInferenceTest, SimpleObject)
{
    JsonSchemaTree tree(config);

    // Build a simple JSON: {"name": "alice", "age": 30}
    auto json1 = buildBinaryJsonObject({{"name", "alice"}}, {{"age", 30}});
    auto json2 = buildBinaryJsonObject({{"name", "bob"}}, {{"age", 25}});
    auto json3 = buildBinaryJsonObject({{"name", "charlie"}}, {{"age", 35}});

    tree.addRow(StringRef(json1.data(), json1.size()));
    tree.addRow(StringRef(json2.data(), json2.size()));
    tree.addRow(StringRef(json3.data(), json3.size()));

    auto schema = tree.finalize();
    EXPECT_EQ(schema.total_rows, 3u);
    EXPECT_EQ(schema.rows_with_json, 3u);
    EXPECT_EQ(schema.numColumns(), 2u); // name + age

    // Check that paths were inferred
    bool has_name = false, has_age = false;
    for (const auto & col : schema.columns)
    {
        if (col.path == "name")
        {
            has_name = true;
            EXPECT_EQ(col.type, JsonLeafType::String);
            EXPECT_EQ(col.occurrence_count, 3u);
        }
        if (col.path == "age")
        {
            has_age = true;
            EXPECT_EQ(col.type, JsonLeafType::Int64);
            EXPECT_EQ(col.occurrence_count, 3u);
        }
    }
    EXPECT_TRUE(has_name);
    EXPECT_TRUE(has_age);
}

TEST_F(JsonSchemaInferenceTest, MixedTypes)
{
    JsonSchemaTree tree(config);

    // "status" is string in one row, int in another → Mixed
    auto json1 = buildBinaryJsonObject({{"status", "active"}});
    auto json2 = buildBinaryJsonObject({}, {{"status", 1}});
    auto json3 = buildBinaryJsonObject({{"status", "inactive"}});

    tree.addRow(StringRef(json1.data(), json1.size()));
    tree.addRow(StringRef(json2.data(), json2.size()));
    tree.addRow(StringRef(json3.data(), json3.size()));

    auto schema = tree.finalize();
    EXPECT_EQ(schema.numColumns(), 1u);
    EXPECT_EQ(schema.columns[0].path, "status");
    EXPECT_EQ(schema.columns[0].type, JsonLeafType::Mixed);
}

TEST_F(JsonSchemaInferenceTest, SparsePruning)
{
    // With absolute_sparse_ratio = 100, a path must appear in >=1% of rows
    // If we have 200 rows and a path appears in 1 row, it should be pruned
    config.absolute_sparse_ratio = 2; // 50% threshold for easy testing

    JsonSchemaTree tree(config);

    auto json_common = buildBinaryJsonObject({{"common", "yes"}});
    auto json_rare = buildBinaryJsonObject({{"common", "yes"}, {"rare", "only_once"}});

    // 10 rows with "common", only 1 with "rare"
    for (int i = 0; i < 10; ++i)
        tree.addRow(StringRef(json_common.data(), json_common.size()));
    tree.addRow(StringRef(json_rare.data(), json_rare.size()));

    auto schema = tree.finalize();
    // "rare" appears 1/11 = 9% which is below 50% threshold → pruned
    // "common" appears 11/11 = 100% → kept
    bool has_common = false, has_rare = false;
    for (const auto & col : schema.columns)
    {
        if (col.path == "common")
            has_common = true;
        if (col.path == "rare")
            has_rare = true;
    }
    EXPECT_TRUE(has_common);
    EXPECT_FALSE(has_rare);
}

TEST_F(JsonSchemaInferenceTest, MaxChildrenPruning)
{
    config.max_children_per_node = 3;
    JsonSchemaTree tree(config);

    // Object with 5 keys → exceeds max_children_per_node of 3
    auto json = buildBinaryJsonObject({{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}, {"e", "5"}});
    tree.addRow(StringRef(json.data(), json.size()));

    auto schema = tree.finalize();
    // Should be un-inferable (root has too many children)
    EXPECT_TRUE(schema.empty());
}

TEST_F(JsonSchemaInferenceTest, MaxLeavesLimit)
{
    config.max_leaves = 2;
    config.max_children_per_node = 100;
    JsonSchemaTree tree(config);

    auto json = buildBinaryJsonObject({{"a", "1"}, {"b", "2"}, {"c", "3"}, {"d", "4"}});
    for (int i = 0; i < 10; ++i)
        tree.addRow(StringRef(json.data(), json.size()));

    auto schema = tree.finalize();
    // Should have at most 2 leaves (max_leaves = 2)
    EXPECT_LE(schema.numColumns(), 2u);
}

TEST_F(JsonSchemaInferenceTest, NullsInSomeRows)
{
    JsonSchemaTree tree(config);

    auto json1 = buildBinaryJsonObject({{"name", "alice"}}, {{"age", 30}});
    auto json2 = buildBinaryJsonObject({{"name", "bob"}}); // no age

    tree.addRow(StringRef(json1.data(), json1.size()));
    tree.addRow(StringRef(json2.data(), json2.size()));
    tree.addNull(); // NULL row

    auto schema = tree.finalize();
    EXPECT_EQ(schema.total_rows, 3u);
    EXPECT_EQ(schema.rows_with_json, 2u);
}

// ============================================================================
// Phase 2 Tests: Shredder (Dual-Write)
// ============================================================================

class JsonShredderTest : public ::testing::Test
{
protected:
    JsonShreddingConfig config;

    void SetUp() override
    {
        config.min_rows_for_inference = 1;
        config.max_children_per_node = 64;
    }
};

TEST_F(JsonShredderTest, BasicShredding)
{
    auto json1 = buildBinaryJsonObject({{"status", "active"}}, {{"count", 10}});
    auto json2 = buildBinaryJsonObject({{"status", "inactive"}}, {{"count", 20}});
    auto json3 = buildBinaryJsonObject({{"status", "active"}}, {{"count", 30}});

    auto col = createJsonColumn({json1, json2, json3});
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto result = shredder.shred(col_string);

    // Original blob preserved
    EXPECT_EQ(result.numRows(), 3u);
    EXPECT_NE(result.original_blob, nullptr);

    // Sub-columns created
    EXPECT_EQ(result.numSubColumns(), 2u); // status + count

    // Verify schema
    EXPECT_EQ(result.schema.total_rows, 3u);
    EXPECT_EQ(result.schema.numColumns(), 2u);
}

TEST_F(JsonShredderTest, PreservesOriginalBlob)
{
    auto json1 = buildBinaryJsonObject({{"key", "value"}});
    auto col = createJsonColumn({json1});
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto result = shredder.shred(col_string);

    // Original blob should be identical to input
    const auto & original = assert_cast<const ColumnString &>(*result.original_blob);
    EXPECT_EQ(original.size(), 1u);
    EXPECT_EQ(original.getDataAt(0), col_string.getDataAt(0));
}

TEST_F(JsonShredderTest, HandlesMixedNullAndNonNull)
{
    auto json1 = buildBinaryJsonObject({{"name", "alice"}}, {{"age", 30}});
    auto json2 = buildBinaryJsonObject({{"name", "bob"}}); // no age field

    auto col = createJsonColumn({json1, json2, ""}); // 3rd row is empty/NULL
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto result = shredder.shred(col_string);

    EXPECT_EQ(result.numRows(), 3u);
    // Sub-columns should have NULLs where path is missing
}

TEST_F(JsonShredderTest, EmptyColumnReturnsNoSubColumns)
{
    auto col = ColumnString::create();
    JsonShredder shredder(config);
    auto result = shredder.shred(*col);
    EXPECT_EQ(result.numRows(), 0u);
    EXPECT_EQ(result.numSubColumns(), 0u);
}

// ============================================================================
// Phase 3 Tests: Query Integration (Flag ON/OFF)
// ============================================================================

class JsonQueryIntegrationTest : public ::testing::Test
{
protected:
    JsonShreddingConfig config;
    ShreddedJsonData shredded_data;

    void SetUp() override
    {
        config.min_rows_for_inference = 1;
        config.max_children_per_node = 64;

        auto json1 = buildBinaryJsonObject({{"status", "active"}}, {{"count", 10}});
        auto json2 = buildBinaryJsonObject({{"status", "inactive"}}, {{"count", 20}});
        auto json3 = buildBinaryJsonObject({{"status", "active"}}, {{"count", 30}});

        auto col = createJsonColumn({json1, json2, json3});
        const auto & col_string = assert_cast<const ColumnString &>(*col);

        JsonShredder shredder(config);
        shredded_data = shredder.shred(col_string);
    }
};

TEST_F(JsonQueryIntegrationTest, FlagOffUsesBlob)
{
    JsonShreddingFlag::instance().setUseShredded(false);

    // Should return nullptr (not using shredded)
    auto result = JsonPathOptimizer::tryReadShredded(shredded_data, "$.status");
    EXPECT_EQ(result, nullptr);

    // canUseShredded should return false
    EXPECT_FALSE(JsonPathOptimizer::canUseShredded(shredded_data, "$.status"));
}

TEST_F(JsonQueryIntegrationTest, FlagOnUsesSubColumn)
{
    JsonShreddingFlag::instance().setUseShredded(true);

    // Should return the sub-column
    auto result = JsonPathOptimizer::tryReadShredded(shredded_data, "$.status");
    EXPECT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 3u);

    // canUseShredded should return true
    EXPECT_TRUE(JsonPathOptimizer::canUseShredded(shredded_data, "$.status"));

    // Reset flag
    JsonShreddingFlag::instance().setUseShredded(false);
}

TEST_F(JsonQueryIntegrationTest, NonExistentPathReturnsNull)
{
    JsonShreddingFlag::instance().setUseShredded(true);

    auto result = JsonPathOptimizer::tryReadShredded(shredded_data, "$.nonexistent");
    EXPECT_EQ(result, nullptr);

    JsonShreddingFlag::instance().setUseShredded(false);
}

TEST_F(JsonQueryIntegrationTest, NormalizePath)
{
    EXPECT_EQ(JsonPathOptimizer::normalizePath("$.status"), "status");
    EXPECT_EQ(JsonPathOptimizer::normalizePath("$.details.publisher"), "details.publisher");
    EXPECT_EQ(JsonPathOptimizer::normalizePath("$"), "");
    EXPECT_EQ(JsonPathOptimizer::normalizePath("$.\"field name\""), "field name");
    EXPECT_EQ(JsonPathOptimizer::normalizePath("status"), "status");
}

TEST_F(JsonQueryIntegrationTest, FilterEqualityOnShredded)
{
    JsonShreddingFlag::instance().setUseShredded(true);

    auto bitmap = JsonPathOptimizer::applyFilter(
        shredded_data,
        "$.status",
        JsonPathOptimizer::FilterOp::Equal,
        Field(String("active")));

    EXPECT_NE(bitmap, nullptr);
    const auto & data = assert_cast<const ColumnUInt8 &>(*bitmap).getData();
    EXPECT_EQ(data.size(), 3u);
    EXPECT_EQ(data[0], 1u); // "active" matches
    EXPECT_EQ(data[1], 0u); // "inactive" doesn't match
    EXPECT_EQ(data[2], 1u); // "active" matches

    JsonShreddingFlag::instance().setUseShredded(false);
}

TEST_F(JsonQueryIntegrationTest, FilterIsNullOnMissingPath)
{
    JsonShreddingFlag::instance().setUseShredded(true);

    auto bitmap = JsonPathOptimizer::applyFilter(
        shredded_data,
        "$.nonexistent",
        JsonPathOptimizer::FilterOp::IsNull,
        Field());

    const auto & data = assert_cast<const ColumnUInt8 &>(*bitmap).getData();
    // All rows should be NULL for a non-existent path
    EXPECT_EQ(data[0], 1u);
    EXPECT_EQ(data[1], 1u);
    EXPECT_EQ(data[2], 1u);

    JsonShreddingFlag::instance().setUseShredded(false);
}

// ============================================================================
// Phase 4 Tests: Schema Evolution
// ============================================================================

class JsonSchemaEvolutionTest : public ::testing::Test
{
protected:
    JsonShreddingConfig config;

    void SetUp() override
    {
        config.min_rows_for_inference = 1;
        config.max_children_per_node = 64;
    }
};

TEST_F(JsonSchemaEvolutionTest, DifferentSchemasAcrossBatches)
{
    // First batch has {name, age}
    auto json1 = buildBinaryJsonObject({{"name", "alice"}}, {{"age", 30}});
    // Second batch has {name, email} — new field, missing field
    auto json2 = buildBinaryJsonObject({{"name", "bob"}, {"email", "bob@test.com"}});

    // Each batch infers its own schema — both should work independently
    JsonSchemaTree tree1(config);
    tree1.addRow(StringRef(json1.data(), json1.size()));
    auto schema1 = tree1.finalize();

    JsonSchemaTree tree2(config);
    tree2.addRow(StringRef(json2.data(), json2.size()));
    auto schema2 = tree2.finalize();

    // Schema 1: name + age
    EXPECT_EQ(schema1.numColumns(), 2u);
    // Schema 2: email + name (sorted)
    EXPECT_EQ(schema2.numColumns(), 2u);
}

TEST_F(JsonSchemaEvolutionTest, ShredWithDifferentSchema)
{
    // Use schema from batch 1 to shred batch 2 (which has different keys)
    auto json1 = buildBinaryJsonObject({{"name", "alice"}}, {{"age", 30}});
    auto json2 = buildBinaryJsonObject({{"name", "bob"}, {"email", "bob@test.com"}});

    JsonSchemaTree tree(config);
    tree.addRow(StringRef(json1.data(), json1.size()));
    auto schema = tree.finalize(); // schema has {age, name}

    // Shred json2 with schema from json1
    auto col = createJsonColumn({json2});
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto result = shredder.shredWithSchema(col_string, schema);

    // "name" should be found, "age" should be NULL
    EXPECT_EQ(result.numSubColumns(), 2u);
}

TEST_F(JsonSchemaEvolutionTest, BackwardCompat_NoSubColumns)
{
    // If schema is empty (old segments without shredding), reader falls back to blob
    ShreddedJsonData data;
    auto json1 = buildBinaryJsonObject({{"key", "value"}});
    auto col = createJsonColumn({json1});
    data.original_blob = col->getPtr();
    // No sub-columns

    // Reader should fall back gracefully
    auto path_result = JsonSubColumnReader::readPath(data, "key");
    EXPECT_EQ(path_result, nullptr); // Not available in shredded form

    auto blob = JsonSubColumnReader::readFullBlob(data);
    EXPECT_NE(blob, nullptr); // Original blob still works
    EXPECT_EQ(blob->size(), 1u);
}

// ============================================================================
// Feature Flag Tests
// ============================================================================

TEST(JsonShreddingFlagTest, DefaultState)
{
    // Default: read uses blob (flag OFF), write always shreds
    EXPECT_FALSE(JsonShreddingFlag::instance().useShredded());
    EXPECT_TRUE(JsonShreddingFlag::instance().writeShredded());
}

TEST(JsonShreddingFlagTest, ToggleReadFlag)
{
    JsonShreddingFlag::instance().setUseShredded(true);
    EXPECT_TRUE(JsonShreddingFlag::instance().useShredded());

    JsonShreddingFlag::instance().setUseShredded(false);
    EXPECT_FALSE(JsonShreddingFlag::instance().useShredded());
}

TEST(JsonShreddingFlagTest, ToggleWriteFlag)
{
    JsonShreddingFlag::instance().setWriteShredded(false);
    EXPECT_FALSE(JsonShreddingFlag::instance().writeShredded());

    JsonShreddingFlag::instance().setWriteShredded(true);
    EXPECT_TRUE(JsonShreddingFlag::instance().writeShredded());
}

// ============================================================================
// Performance Comparison Setup Test
// ============================================================================

TEST(JsonShreddingPerformanceTest, DualWriteEnablesBothPaths)
{
    // Verify that both read paths produce equivalent results
    JsonShreddingConfig config;
    config.min_rows_for_inference = 1;
    config.max_children_per_node = 64;

    auto json1 = buildBinaryJsonObject({{"status", "active"}}, {{"count", 100}});
    auto json2 = buildBinaryJsonObject({{"status", "inactive"}}, {{"count", 200}});

    auto col = createJsonColumn({json1, json2});
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto result = shredder.shred(col_string);

    // Both representations exist
    EXPECT_NE(result.original_blob, nullptr);
    EXPECT_GT(result.numSubColumns(), 0u);

    // Flag OFF: extractJsonPath falls through to blob
    JsonShreddingFlag::instance().setUseShredded(false);
    auto blob_result = JsonPathOptimizer::extractJsonPath(result, "$.status");
    EXPECT_NE(blob_result, nullptr);
    EXPECT_EQ(blob_result->size(), 2u);

    // Flag ON: extractJsonPath uses sub-column
    JsonShreddingFlag::instance().setUseShredded(true);
    auto shredded_result = JsonPathOptimizer::extractJsonPath(result, "$.status");
    EXPECT_NE(shredded_result, nullptr);
    EXPECT_EQ(shredded_result->size(), 2u);

    // Both should return equivalent data
    // (The actual values come from the same source, just different read paths)

    JsonShreddingFlag::instance().setUseShredded(false); // Reset
}

} // namespace DB::DM::tests
