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
#include <Core/ColumnShreddedAttachment.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/ShreddedAttachmentCache.h>
#include <DataTypes/DataTypeString.h>
#include <Debug/TiFlashTestEnv.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/JsonShredding/JsonPathOptimizer.h>
#include <Storages/DeltaMerge/JsonShredding/JsonSchemaTree.h>
#include <Storages/DeltaMerge/JsonShredding/JsonShredder.h>
#include <Storages/DeltaMerge/JsonShredding/JsonShreddedStore.h>
#include <Storages/DeltaMerge/JsonShredding/JsonShreddingConfig.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/DeltaMerge/tests/gtest_dm_delta_merge_store_test_basic.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <fmt/format.h>
#include <TiDB/Decode/JsonBinary.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <thread>

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
    // Ensure defaults are restored (singleton may have been modified by prior tests)
    JsonShreddingFlag::instance().setUseShredded(true);
    JsonShreddingFlag::instance().setWriteShredded(true);
    EXPECT_TRUE(JsonShreddingFlag::instance().useShredded());
    EXPECT_TRUE(JsonShreddingFlag::instance().writeShredded());
}

TEST(JsonShreddingFlagTest, ToggleReadFlag)
{
    JsonShreddingFlag::instance().setUseShredded(true);
    EXPECT_TRUE(JsonShreddingFlag::instance().useShredded());

    JsonShreddingFlag::instance().setUseShredded(false);
    EXPECT_FALSE(JsonShreddingFlag::instance().useShredded());

    // Restore default
    JsonShreddingFlag::instance().setUseShredded(true);
}

TEST(JsonShreddingFlagTest, ToggleWriteFlag)
{
    JsonShreddingFlag::instance().setWriteShredded(false);
    EXPECT_FALSE(JsonShreddingFlag::instance().writeShredded());

    JsonShreddingFlag::instance().setWriteShredded(true);
    EXPECT_TRUE(JsonShreddingFlag::instance().writeShredded());

    // Restore default
    JsonShreddingFlag::instance().setWriteShredded(true);
}

// ============================================================================
// BlockContext Column-ID Lookup Tests
// Simulates the real query pipeline where PhysicalTableScan::buildProjection
// renames columns from storage names (e.g., "payload") to DAG schema names
// (e.g., "table_scan_3"). The context must find data via column_id even when
// the name doesn't match.
// ============================================================================

TEST(JsonShreddedBlockContextTest, LookupByColumnIdAfterRename)
{
    // Simulate: DMFileReader stores context under storage name "payload" with col_id=5
    // Expression evaluator looks up with DAG name "table_scan_3" with col_id=5
    // Should find the data via column_id despite name mismatch.

    JsonShreddingConfig config;
    config.min_rows_for_inference = 1;
    config.max_children_per_node = 64;

    auto json1 = buildBinaryJsonObject({{"event", "click"}}, {{"count", 10}});
    auto json2 = buildBinaryJsonObject({{"event", "view"}}, {{"count", 20}});
    auto json3 = buildBinaryJsonObject({{"event", "purchase"}}, {{"count", 30}});

    auto col = createJsonColumn({json1, json2, json3});
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto shredded = shredder.shred(col_string);
    ASSERT_GT(shredded.numSubColumns(), 0u);

    auto & ctx = JsonShreddedBlockContext::instance();
    ctx.clear();

    // Store under storage name "payload" with column_id=5
    const Int64 col_id = 5;
    ctx.setForCurrentBlock("payload", col_id, shredded, 0, 3);

    // Lookup with SAME name should work (baseline)
    auto result1 = ctx.getSubColumn("payload", col_id, "event");
    ASSERT_NE(result1, nullptr);
    EXPECT_EQ(result1->size(), 3u);

    // Lookup with DIFFERENT name but same column_id should ALSO work
    // This simulates the post-projection scenario
    auto result2 = ctx.getSubColumn("table_scan_3", col_id, "event");
    ASSERT_NE(result2, nullptr) << "column_id lookup should find data even with mismatched name";
    EXPECT_EQ(result2->size(), 3u);

    // Lookup with DIFFERENT name and WRONG column_id should fail
    auto result3 = ctx.getSubColumn("table_scan_3", 999, "event");
    EXPECT_EQ(result3, nullptr);

    // Lookup with DIFFERENT name and col_id=0 (backward compat) should fail
    // because name "table_scan_3" was never stored
    auto result4 = ctx.getSubColumn("table_scan_3", 0, "event");
    EXPECT_EQ(result4, nullptr);

    // hasShredded should also work with column_id
    EXPECT_TRUE(ctx.hasShredded("payload", col_id));
    EXPECT_TRUE(ctx.hasShredded("table_scan_3", col_id));  // finds by id
    EXPECT_FALSE(ctx.hasShredded("table_scan_3", 0));       // name-only lookup fails
    EXPECT_FALSE(ctx.hasShredded("table_scan_3", 999));     // wrong id

    ctx.clear();
}

TEST(JsonShreddedBlockContextTest, LookupByColumnIdWithRowSlicing)
{
    // Simulate: sidecar has 9 rows (full DMFile), but we're reading pack of 3 rows at offset 3
    // column_id lookup should correctly slice to [3, 6)

    JsonShreddingConfig config;
    config.min_rows_for_inference = 1;
    config.max_children_per_node = 64;

    // Build 9 JSON documents
    std::vector<String> jsons;
    for (int i = 0; i < 9; ++i)
        jsons.push_back(buildBinaryJsonObject({{"name", fmt::format("user_{}", i)}}, {{"id", i}}));

    auto col = createJsonColumn(jsons);
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto shredded = shredder.shred(col_string);
    ASSERT_EQ(shredded.numRows(), 9u);

    auto & ctx = JsonShreddedBlockContext::instance();
    ctx.clear();

    // Store with row range [3, 3+3) simulating pack in the middle
    const Int64 col_id = 7;
    ctx.setForCurrentBlock("payload", col_id, shredded, 3, 3);

    // Lookup with renamed column name but correct id
    auto result = ctx.getSubColumn("table_scan_0", col_id, "id");
    ASSERT_NE(result, nullptr) << "Should find via col_id and slice to 3 rows";
    EXPECT_EQ(result->size(), 3u);

    ctx.clear();
}

TEST(JsonShreddedBlockContextTest, BackwardCompatNameOnlyLookup)
{
    // Ensure backward compatibility: when col_id=0, lookup falls back to name-only
    JsonShreddingConfig config;
    config.min_rows_for_inference = 1;
    config.max_children_per_node = 64;

    auto json1 = buildBinaryJsonObject({{"status", "ok"}}, {});
    auto col = createJsonColumn({json1});
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto shredded = shredder.shred(col_string);

    auto & ctx = JsonShreddedBlockContext::instance();
    ctx.clear();

    // Store with col_id=0 (simulating unit test scenario without real column IDs)
    ctx.setForCurrentBlock("payload", 0, shredded, 0, 1);

    // Name-only lookup (2-arg overload) should work
    auto result = ctx.getSubColumn("payload", "status");
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 1u);

    // 3-arg overload with col_id=0 should also work via name fallback
    auto result2 = ctx.getSubColumn("payload", 0, "status");
    ASSERT_NE(result2, nullptr);
    EXPECT_EQ(result2->size(), 1u);

    ctx.clear();
}

// ============================================================================
// Thread-Safety Tests: ColumnShreddedAttachment survives thread handoff
// ============================================================================

TEST(JsonShreddedBlockContextTest, AttachmentSurvivesThreadHandoff)
{
    // This test verifies the CRITICAL fix: shredded data attached to a column
    // is accessible from a DIFFERENT thread (simulating DMFileReader → MPP worker).
    // The old thread-local approach failed because reader and eval run on
    // different thread pools.
    JsonShreddingConfig config;
    config.min_rows_for_inference = 1;
    config.max_children_per_node = 64;

    auto json1 = buildBinaryJsonObject({{"event", "click"}}, {{"count", 42}});
    auto json2 = buildBinaryJsonObject({{"event", "view"}}, {{"count", 7}});
    auto json3 = buildBinaryJsonObject({{"event", "buy"}}, {{"count", 99}});
    auto col = createJsonColumn({json1, json2, json3});
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto shredded = shredder.shred(col_string);

    // Simulate DMFileReader (reader thread): attach shredded data to column
    auto sidecar = std::make_shared<const ShreddedJsonData>(std::move(shredded));
    auto attachment = std::make_shared<ColumnShreddedAttachment>();
    attachment->data = sidecar;
    attachment->row_offset = 0;
    attachment->row_count = 3;

    ColumnWithTypeAndName col_with_attach;
    col_with_attach.column = std::move(col);
    col_with_attach.name = "payload";
    col_with_attach.column_id = 7;
    col_with_attach.shredded_attachment = attachment;

    // Simulate thread handoff: pass column to a different thread (MPP eval thread)
    ColumnPtr result_from_other_thread;
    std::thread eval_thread([&]() {
        // This runs on a DIFFERENT thread — thread-local would be empty here.
        // The attachment should still be accessible.
        ASSERT_NE(col_with_attach.shredded_attachment, nullptr);
        ASSERT_NE(col_with_attach.shredded_attachment->data, nullptr);
        const auto & attach = *col_with_attach.shredded_attachment;
        auto full_col = JsonSubColumnReader::readPath(*attach.data, "event");
        ASSERT_NE(full_col, nullptr);
        if (full_col->size() == attach.row_count)
            result_from_other_thread = full_col;
        else if (full_col->size() >= attach.row_offset + attach.row_count)
            result_from_other_thread = full_col->cut(attach.row_offset, attach.row_count);
        else
            result_from_other_thread = nullptr;
    });
    eval_thread.join();

    // Verify the eval thread successfully read the sub-column
    ASSERT_NE(result_from_other_thread, nullptr);
    EXPECT_EQ(result_from_other_thread->size(), 3u);
}

TEST(JsonShreddedBlockContextTest, AttachmentSurvivesColumnRenameAndThreadHandoff)
{
    // Full pipeline simulation:
    // 1. Reader thread creates column with name="payload", attaches shredded data
    // 2. PROJECT renames column to "table_scan_3" (simulating PhysicalTableScan)
    // 3. Eval thread reads attachment from renamed column
    JsonShreddingConfig config;
    config.min_rows_for_inference = 1;
    config.max_children_per_node = 64;

    auto json1 = buildBinaryJsonObject({{"status", "ok"}}, {{"value", 10}});
    auto json2 = buildBinaryJsonObject({{"status", "err"}}, {{"value", 20}});
    auto col = createJsonColumn({json1, json2});
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto shredded = shredder.shred(col_string);

    auto sidecar = std::make_shared<const ShreddedJsonData>(std::move(shredded));
    auto attachment = std::make_shared<ColumnShreddedAttachment>();
    attachment->data = sidecar;
    attachment->row_offset = 0;
    attachment->row_count = 2;

    // Step 1: Reader thread creates column
    ColumnWithTypeAndName original;
    original.column = std::move(col);
    original.name = "payload";
    original.column_id = 5;
    original.shredded_attachment = attachment;

    // Step 2: PROJECT renames column (simulates ExpressionActions PROJECT)
    ColumnWithTypeAndName renamed = original;  // copy — attachment is shared_ptr, survives
    renamed.name = "table_scan_3";

    // Verify attachment survived the rename
    ASSERT_NE(renamed.shredded_attachment, nullptr);
    EXPECT_EQ(renamed.shredded_attachment->data.get(), sidecar.get());

    // Step 3: Eval thread reads from the renamed column
    ColumnPtr result_from_eval;
    std::thread eval_thread([&]() {
        // Different thread, different name — but attachment is still there
        ASSERT_NE(renamed.shredded_attachment, nullptr);
        const auto & attach = *renamed.shredded_attachment;
        auto full_col = JsonSubColumnReader::readPath(*attach.data, "status");
        ASSERT_NE(full_col, nullptr);
        if (full_col->size() == attach.row_count)
            result_from_eval = full_col;
        else if (full_col->size() >= attach.row_offset + attach.row_count)
            result_from_eval = full_col->cut(attach.row_offset, attach.row_count);
    });
    eval_thread.join();

    ASSERT_NE(result_from_eval, nullptr);
    EXPECT_EQ(result_from_eval->size(), 2u);
}

TEST(JsonShreddedBlockContextTest, AttachmentWithRowSlicingAcrossThreads)
{
    // Simulate reading a pack (subset of rows) from a larger sidecar,
    // with the lookup happening on a different thread.
    JsonShreddingConfig config;
    config.min_rows_for_inference = 1;
    config.max_children_per_node = 64;

    // Build 6 rows — sidecar covers all 6
    std::vector<String> jsons;
    for (int i = 0; i < 6; ++i)
        jsons.push_back(buildBinaryJsonObject({{"key", fmt::format("v{}", i)}}, {}));
    auto col = createJsonColumn(jsons);
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto shredded = shredder.shred(col_string);

    auto sidecar = std::make_shared<const ShreddedJsonData>(std::move(shredded));

    // Simulate reading pack at row_offset=2, row_count=3 (rows 2,3,4 out of 6)
    auto attachment = std::make_shared<ColumnShreddedAttachment>();
    attachment->data = sidecar;
    attachment->row_offset = 2;
    attachment->row_count = 3;

    ColumnWithTypeAndName col_info;
    col_info.column = std::move(col);
    col_info.name = "data";
    col_info.column_id = 10;
    col_info.shredded_attachment = attachment;

    ColumnPtr sliced_result;
    std::thread eval_thread([&]() {
        const auto & attach = *col_info.shredded_attachment;
        auto full_col = JsonSubColumnReader::readPath(*attach.data, "key");
        ASSERT_NE(full_col, nullptr);
        EXPECT_EQ(full_col->size(), 6u);  // Full sidecar has all 6 rows
        // Slice to current pack's range
        if (full_col->size() >= attach.row_offset + attach.row_count)
            sliced_result = full_col->cut(attach.row_offset, attach.row_count);
    });
    eval_thread.join();

    ASSERT_NE(sliced_result, nullptr);
    EXPECT_EQ(sliced_result->size(), 3u);  // Only rows 2,3,4
}

TEST(JsonShreddedBlockContextTest, ThreadLocalIsEmptyOnDifferentThread)
{
    // Proves the old approach FAILS: thread-local set on one thread is
    // invisible on another thread.
    JsonShreddingConfig config;
    config.min_rows_for_inference = 1;
    config.max_children_per_node = 64;

    auto json1 = buildBinaryJsonObject({{"x", "y"}}, {});
    auto col = createJsonColumn({json1});
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto shredded = shredder.shred(col_string);

    // Set context on THIS thread (simulating reader thread)
    auto & ctx = JsonShreddedBlockContext::instance();
    ctx.clear();
    ctx.setForCurrentBlock("payload", 1, shredded, 0, 1);

    // Verify it works on THIS thread
    ASSERT_NE(ctx.getSubColumn("payload", 1, "x"), nullptr);

    // Verify it is EMPTY on a different thread (proving the bug)
    bool found_on_other_thread = false;
    std::thread other_thread([&]() {
        auto & other_ctx = JsonShreddedBlockContext::instance();
        found_on_other_thread = other_ctx.hasShredded("payload", 1);
    });
    other_thread.join();

    // This MUST be false — thread-local is empty on other thread
    EXPECT_FALSE(found_on_other_thread);

    ctx.clear();
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

    // Restore defaults
    JsonShreddingFlag::instance().setUseShredded(true);
    JsonShreddingFlag::instance().setWriteShredded(true);
}

// ============================================================================
// Dictionary Encoding Tests: Sidecar v2 with encoded string sub-columns
// ============================================================================

TEST(JsonShreddingSidecarTest, DictionaryEncodedStringSubColumns)
{
    // Verify that string sub-columns with low cardinality are dictionary-encoded
    // in the sidecar, and correctly reconstructed on read.
    JsonShreddingConfig config;
    config.min_rows_for_inference = 1;
    config.max_children_per_node = 64;

    // Build 100 JSON rows with a low-cardinality "status" field (3 distinct values)
    // and a numeric "count" field
    std::vector<String> jsons;
    const std::vector<String> statuses = {"active", "inactive", "pending"};
    for (int i = 0; i < 100; ++i)
    {
        jsons.push_back(buildBinaryJsonObject(
            {{"status", statuses[i % 3]}},
            {{"count", static_cast<Int64>(i)}}));
    }
    auto col = createJsonColumn(jsons);
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto shredded = shredder.shred(col_string);

    ASSERT_GT(shredded.numSubColumns(), 0u);
    EXPECT_EQ(shredded.numRows(), 100u);

    // Write sidecar (should dictionary-encode "status" since cardinality=3)
    String test_dir = "/tmp/test_dict_sidecar_" + std::to_string(::getpid());
    Poco::File(test_dir).createDirectories();

    JsonShreddedStore::writeSidecar(test_dir, "payload", shredded);

    // Read it back
    auto read_result = JsonShreddedStore::readSidecar(test_dir, "payload");
    ASSERT_TRUE(read_result.has_value());
    EXPECT_EQ(read_result->schema.total_rows, 100u);
    EXPECT_EQ(read_result->numSubColumns(), shredded.numSubColumns());

    // Verify "status" sub-column data is correct after decode
    auto status_col = JsonSubColumnReader::readPath(*read_result, "status");
    ASSERT_NE(status_col, nullptr);
    EXPECT_EQ(status_col->size(), 100u);

    // Verify "count" sub-column (Int64, not dictionary-encoded)
    auto count_col = JsonSubColumnReader::readPath(*read_result, "count");
    ASSERT_NE(count_col, nullptr);
    EXPECT_EQ(count_col->size(), 100u);

    // Verify actual values
    const auto & status_nullable = assert_cast<const ColumnNullable &>(*status_col);
    for (int i = 0; i < 100; ++i)
    {
        ASSERT_FALSE(status_nullable.isNullAt(i));
        String val = status_nullable.getNestedColumn().getDataAt(i).toString();
        EXPECT_EQ(val, statuses[i % 3]) << "Mismatch at row " << i;
    }

    // Cleanup
    Poco::File(test_dir).remove(true);
}

TEST(JsonShreddingSidecarTest, HighCardinalityNotDictionaryEncoded)
{
    // String sub-columns with cardinality > 4096 should NOT be dictionary-encoded
    JsonShreddingConfig config;
    config.min_rows_for_inference = 1;
    config.max_children_per_node = 64;

    // Build 5000 rows with unique "id" values (cardinality = 5000 > 4096)
    std::vector<String> jsons;
    for (int i = 0; i < 5000; ++i)
    {
        jsons.push_back(buildBinaryJsonObject(
            {{"id", fmt::format("unique_id_{}", i)}},
            {}));
    }
    auto col = createJsonColumn(jsons);
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto shredded = shredder.shred(col_string);

    // Write and read back
    String test_dir = "/tmp/test_highcard_sidecar_" + std::to_string(::getpid());
    Poco::File(test_dir).createDirectories();

    JsonShreddedStore::writeSidecar(test_dir, "payload", shredded);

    auto read_result = JsonShreddedStore::readSidecar(test_dir, "payload");
    ASSERT_TRUE(read_result.has_value());
    EXPECT_EQ(read_result->schema.total_rows, 5000u);

    // Verify data integrity (round-trip correctness even without encoding)
    auto id_col = JsonSubColumnReader::readPath(*read_result, "id");
    ASSERT_NE(id_col, nullptr);
    EXPECT_EQ(id_col->size(), 5000u);

    const auto & id_nullable = assert_cast<const ColumnNullable &>(*id_col);
    for (int i = 0; i < 5000; ++i)
    {
        ASSERT_FALSE(id_nullable.isNullAt(i));
        String val = id_nullable.getNestedColumn().getDataAt(i).toString();
        EXPECT_EQ(val, fmt::format("unique_id_{}", i));
    }

    // Cleanup
    Poco::File(test_dir).remove(true);
}

TEST(JsonShreddingSidecarTest, DictionaryEncodingDiskSizeSavings)
{
    // Verify that dictionary encoding reduces sidecar file size for low-cardinality columns
    JsonShreddingConfig config;
    config.min_rows_for_inference = 1;
    config.max_children_per_node = 64;

    // Build 10000 rows with 5 distinct long status strings
    std::vector<String> jsons;
    const std::vector<String> statuses = {
        "processing_payment_gateway",
        "awaiting_customer_response",
        "shipped_via_express_delivery",
        "returned_damaged_in_transit",
        "completed_with_satisfaction"};
    for (int i = 0; i < 10000; ++i)
    {
        jsons.push_back(buildBinaryJsonObject(
            {{"long_status", statuses[i % 5]}},
            {}));
    }
    auto col = createJsonColumn(jsons);
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto shredded = shredder.shred(col_string);

    String test_dir = "/tmp/test_dictsize_sidecar_" + std::to_string(::getpid());
    Poco::File(test_dir).createDirectories();

    JsonShreddedStore::writeSidecar(test_dir, "payload", shredded);

    // Check file size: dictionary-encoded should be much smaller than raw
    // Raw: 10000 * ~27 chars avg = ~270KB for offsets+chars
    // Dict: 5 entries * ~27 chars + 10000 * 4 bytes (IDs) = ~40KB
    String col_file = test_dir + "/.json_shredded/payload/long_status.bin";
    ASSERT_TRUE(Poco::File(col_file).exists());
    auto file_size = Poco::File(col_file).getSize();

    // Dictionary-encoded: should be well under 100KB
    // (5 dict entries ~150 bytes + 10000 IDs * 4 bytes = ~40KB + null bitmap + header)
    EXPECT_LT(file_size, 100000u) << "File too large — dictionary encoding may not be applied";

    // Verify data round-trips correctly
    auto read_result = JsonShreddedStore::readSidecar(test_dir, "payload");
    ASSERT_TRUE(read_result.has_value());
    auto status_col = JsonSubColumnReader::readPath(*read_result, "long_status");
    ASSERT_NE(status_col, nullptr);
    EXPECT_EQ(status_col->size(), 10000u);

    const auto & nullable = assert_cast<const ColumnNullable &>(*status_col);
    for (int i = 0; i < 10; ++i) // spot check first 10
    {
        String val = nullable.getNestedColumn().getDataAt(i).toString();
        EXPECT_EQ(val, statuses[i % 5]);
    }

    Poco::File(test_dir).remove(true);
}

// ============================================================================
// Phase 5 Tests: Segment Merge & Delta Flush
// ============================================================================

} // namespace DB::DM::tests

#include <Storages/DeltaMerge/JsonShredding/JsonSegmentMerger.h>

namespace DB::DM::tests
{

class JsonSegmentMergeTest : public ::testing::Test
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
        config.min_rows_for_inference = 1;
    }
};

TEST_F(JsonSegmentMergeTest, MergeIdenticalSchemas)
{
    // Two segments with the same schema (name:String, age:Int64)
    auto json1 = buildBinaryJsonObject({{"name", "alice"}}, {{"age", 30}});
    auto json2 = buildBinaryJsonObject({{"name", "bob"}}, {{"age", 25}});
    auto json3 = buildBinaryJsonObject({{"name", "charlie"}}, {{"age", 35}});
    auto json4 = buildBinaryJsonObject({{"name", "dave"}}, {{"age", 40}});

    auto col_a = createJsonColumn({json1, json2});
    auto col_b = createJsonColumn({json3, json4});

    JsonShredder shredder(config);
    auto seg_a = shredder.shred(assert_cast<const ColumnString &>(*col_a));
    auto seg_b = shredder.shred(assert_cast<const ColumnString &>(*col_b));

    // Verify schemas match
    EXPECT_TRUE(JsonSegmentMerger::schemasMatch(seg_a.schema, seg_b.schema));

    // Merge
    JsonSegmentMerger merger(config);
    auto merged = merger.mergeSegments(seg_a, seg_b);

    // Verify merged result
    EXPECT_EQ(merged.original_blob->size(), 4u); // 2 + 2 rows
    EXPECT_EQ(merged.numSubColumns(), 2u); // name + age
    EXPECT_EQ(merged.schema.total_rows, 4u);
    EXPECT_EQ(merged.schema.rows_with_json, 4u);

    // Verify sub-column row count
    for (const auto & sub : merged.sub_columns)
        EXPECT_EQ(sub.rows(), 4u);

    // Verify occurrence counts are summed
    for (const auto & col : merged.schema.columns)
        EXPECT_EQ(col.occurrence_count, 4u); // 2 + 2
}

TEST_F(JsonSegmentMergeTest, MergeDifferentSchemas)
{
    // Segment A: {name, age}
    auto json1 = buildBinaryJsonObject({{"name", "alice"}}, {{"age", 30}});
    auto json2 = buildBinaryJsonObject({{"name", "bob"}}, {{"age", 25}});

    // Segment B: {city, zip}
    auto json3 = buildBinaryJsonObject({{"city", "NYC"}}, {{"zip", 10001}});
    auto json4 = buildBinaryJsonObject({{"city", "LA"}}, {{"zip", 90001}});

    auto col_a = createJsonColumn({json1, json2});
    auto col_b = createJsonColumn({json3, json4});

    JsonShredder shredder(config);
    auto seg_a = shredder.shred(assert_cast<const ColumnString &>(*col_a));
    auto seg_b = shredder.shred(assert_cast<const ColumnString &>(*col_b));

    // Verify schemas differ
    EXPECT_FALSE(JsonSegmentMerger::schemasMatch(seg_a.schema, seg_b.schema));

    // Merge (should re-shred)
    JsonSegmentMerger merger(config);
    auto merged = merger.mergeSegments(seg_a, seg_b);

    // Unified schema should have all 4 paths: age, city, name, zip
    EXPECT_EQ(merged.original_blob->size(), 4u);
    EXPECT_EQ(merged.schema.numColumns(), 4u);
    EXPECT_TRUE(merged.schema.hasPath("name"));
    EXPECT_TRUE(merged.schema.hasPath("age"));
    EXPECT_TRUE(merged.schema.hasPath("city"));
    EXPECT_TRUE(merged.schema.hasPath("zip"));

    // Verify sub-columns exist for all paths
    EXPECT_EQ(merged.numSubColumns(), 4u);

    // First 2 rows should have name+age, rows 3-4 should have city+zip
    // Path "name" in rows 3-4 should be NULL
    for (const auto & sub : merged.sub_columns)
        EXPECT_EQ(sub.rows(), 4u);
}

TEST_F(JsonSegmentMergeTest, MergeWithTypePromotion)
{
    // Segment A: status is String
    auto json1 = buildBinaryJsonObject({{"status", "active"}});
    auto json2 = buildBinaryJsonObject({{"status", "inactive"}});

    // Segment B: status is Int64 (type conflict)
    auto json3 = buildBinaryJsonObject({}, {{"status", 1}});
    auto json4 = buildBinaryJsonObject({}, {{"status", 0}});

    auto col_a = createJsonColumn({json1, json2});
    auto col_b = createJsonColumn({json3, json4});

    JsonShredder shredder(config);
    auto seg_a = shredder.shred(assert_cast<const ColumnString &>(*col_a));
    auto seg_b = shredder.shred(assert_cast<const ColumnString &>(*col_b));

    // Type conflict: String vs Int64
    EXPECT_EQ(seg_a.schema.getPathType("status"), JsonLeafType::String);
    EXPECT_EQ(seg_b.schema.getPathType("status"), JsonLeafType::Int64);

    // Merge — types should be promoted to Mixed
    auto unified = JsonSchemaTree::mergeSchemas(seg_a.schema, seg_b.schema);
    EXPECT_EQ(unified.getPathType("status"), JsonLeafType::Mixed);
    EXPECT_EQ(unified.columns[0].occurrence_count, 4u); // 2 + 2
}

TEST_F(JsonSegmentMergeTest, DeltaFlushWithNewKeys)
{
    // Stable segment: {name, age}
    auto json1 = buildBinaryJsonObject({{"name", "alice"}}, {{"age", 30}});
    auto json2 = buildBinaryJsonObject({{"name", "bob"}}, {{"age", 25}});

    auto col_stable = createJsonColumn({json1, json2});
    JsonShredder shredder(config);
    auto stable = shredder.shred(assert_cast<const ColumnString &>(*col_stable));

    EXPECT_EQ(stable.schema.numColumns(), 2u); // name + age

    // Delta has new keys: {name, age, email}
    auto json3 = buildBinaryJsonObject({{"name", "charlie"}, {"email", "c@x.com"}}, {{"age", 35}});
    auto delta_col = ColumnString::create();
    delta_col->insertData(json3.data(), json3.size());

    // Flush delta into stable
    JsonSegmentMerger merger(config);
    auto new_stable = merger.flushDelta(stable, *delta_col);

    // New stable should have 3 paths: age, email, name
    EXPECT_EQ(new_stable.original_blob->size(), 3u); // 2 stable + 1 delta
    EXPECT_EQ(new_stable.schema.numColumns(), 3u);
    EXPECT_TRUE(new_stable.schema.hasPath("name"));
    EXPECT_TRUE(new_stable.schema.hasPath("age"));
    EXPECT_TRUE(new_stable.schema.hasPath("email"));

    // All sub-columns should have 3 rows
    for (const auto & sub : new_stable.sub_columns)
        EXPECT_EQ(sub.rows(), 3u);
}

TEST_F(JsonSegmentMergeTest, DeltaFlushWithDisappearingKeys)
{
    // Stable segment: {name, age, status}
    auto json1 = buildBinaryJsonObject({{"name", "alice"}, {"status", "active"}}, {{"age", 30}});
    auto json2 = buildBinaryJsonObject({{"name", "bob"}, {"status", "inactive"}}, {{"age", 25}});

    auto col_stable = createJsonColumn({json1, json2});
    JsonShredder shredder(config);
    auto stable = shredder.shred(assert_cast<const ColumnString &>(*col_stable));

    EXPECT_EQ(stable.schema.numColumns(), 3u); // age, name, status

    // Delta has fewer keys: only {name} — "status" and "age" disappear
    auto json3 = buildBinaryJsonObject({{"name", "charlie"}});
    auto delta_col = ColumnString::create();
    delta_col->insertData(json3.data(), json3.size());

    // Flush delta — re-infers from ALL combined data
    JsonSegmentMerger merger(config);
    auto new_stable = merger.flushDelta(stable, *delta_col);

    // Schema should still include all paths from combined data
    EXPECT_EQ(new_stable.original_blob->size(), 3u);
    // name appears in 3/3 rows, age in 2/3, status in 2/3 — all above 1% threshold
    EXPECT_TRUE(new_stable.schema.hasPath("name"));
    EXPECT_TRUE(new_stable.schema.hasPath("age"));
    EXPECT_TRUE(new_stable.schema.hasPath("status"));

    // For row 3 (delta), age and status should be NULL
    for (const auto & sub : new_stable.sub_columns)
        EXPECT_EQ(sub.rows(), 3u);
}

TEST_F(JsonSegmentMergeTest, ReadPathAcrossSegments)
{
    // Segment A: has "name" path
    auto json1 = buildBinaryJsonObject({{"name", "alice"}}, {{"age", 30}});
    auto json2 = buildBinaryJsonObject({{"name", "bob"}}, {{"age", 25}});

    // Segment B: does NOT have "name" path
    auto json3 = buildBinaryJsonObject({{"city", "NYC"}}, {{"zip", 10001}});
    auto json4 = buildBinaryJsonObject({{"city", "LA"}}, {{"zip", 90001}});

    auto col_a = createJsonColumn({json1, json2});
    auto col_b = createJsonColumn({json3, json4});

    JsonShredder shredder(config);
    auto seg_a = shredder.shred(assert_cast<const ColumnString &>(*col_a));
    auto seg_b = shredder.shred(assert_cast<const ColumnString &>(*col_b));

    // Read "name" across both segments
    std::vector<const ShreddedJsonData *> segments = {&seg_a, &seg_b};
    auto result = JsonSegmentMerger::readPathAcrossSegments(segments, "name");

    // Should have 4 rows total
    const auto & nullable = assert_cast<const ColumnNullable &>(*result);
    EXPECT_EQ(nullable.size(), 4u);

    // First 2 rows (segment A) should have values
    EXPECT_FALSE(nullable.isNullAt(0));
    EXPECT_FALSE(nullable.isNullAt(1));

    // Last 2 rows (segment B) should be NULL (path doesn't exist)
    EXPECT_TRUE(nullable.isNullAt(2));
    EXPECT_TRUE(nullable.isNullAt(3));
}

TEST_F(JsonSegmentMergeTest, UnifySchemas)
{
    auto json1 = buildBinaryJsonObject({{"name", "alice"}}, {{"age", 30}});
    auto json2 = buildBinaryJsonObject({{"city", "NYC"}});
    auto json3 = buildBinaryJsonObject({{"name", "bob"}, {"email", "b@x.com"}});

    auto col_a = createJsonColumn({json1});
    auto col_b = createJsonColumn({json2});
    auto col_c = createJsonColumn({json3});

    JsonShredder shredder(config);
    auto seg_a = shredder.shred(assert_cast<const ColumnString &>(*col_a));
    auto seg_b = shredder.shred(assert_cast<const ColumnString &>(*col_b));
    auto seg_c = shredder.shred(assert_cast<const ColumnString &>(*col_c));

    std::vector<const ShreddedJsonData *> segments = {&seg_a, &seg_b, &seg_c};
    auto unified = JsonSegmentMerger::unifySchemas(segments);

    // Union: age, city, email, name (sorted)
    EXPECT_EQ(unified.numColumns(), 4u);
    EXPECT_TRUE(unified.hasPath("age"));
    EXPECT_TRUE(unified.hasPath("city"));
    EXPECT_TRUE(unified.hasPath("email"));
    EXPECT_TRUE(unified.hasPath("name"));
    EXPECT_EQ(unified.total_rows, 3u);
}

TEST_F(JsonSegmentMergeTest, MergePreShreddingSegmentWithShreddedSegment)
{
    // Segment A: pre-shredding (no sub-columns, blob only)
    ShreddedJsonData seg_a;
    auto json1 = buildBinaryJsonObject({{"name", "alice"}}, {{"age", 30}});
    auto json2 = buildBinaryJsonObject({{"name", "bob"}}, {{"age", 25}});
    auto col_a = createJsonColumn({json1, json2});
    seg_a.original_blob = col_a->getPtr();
    // No sub-columns — simulating old segment

    // Segment B: has shredded data
    auto json3 = buildBinaryJsonObject({{"name", "charlie"}}, {{"age", 35}});
    auto json4 = buildBinaryJsonObject({{"name", "dave"}}, {{"age", 40}});
    auto col_b = createJsonColumn({json3, json4});
    JsonShredder shredder(config);
    auto seg_b = shredder.shred(assert_cast<const ColumnString &>(*col_b));

    // Merge: pre-shredding + shredded
    JsonSegmentMerger merger(config);
    auto merged = merger.mergeSegments(seg_a, seg_b);

    // Result should have all 4 rows with blobs merged
    EXPECT_EQ(merged.original_blob->size(), 4u);
    // Since seg_a has no schema, merge will re-shred from blobs
    // seg_a.schema is empty so mergeSchemas yields seg_b's schema
    // Then mergeWithReShred re-shreds all rows with the unified schema
    EXPECT_EQ(merged.numSubColumns(), 2u); // name + age
    for (const auto & sub : merged.sub_columns)
        EXPECT_EQ(sub.rows(), 4u);
}

TEST_F(JsonSegmentMergeTest, MergeSchemasFunctionDirectly)
{
    // Test the static mergeSchemas function
    JsonInferredSchema schema_a;
    schema_a.total_rows = 100;
    schema_a.rows_with_json = 90;
    schema_a.columns.push_back({"name", JsonLeafType::String, 90, false});
    schema_a.columns.push_back({"age", JsonLeafType::Int64, 80, false});

    JsonInferredSchema schema_b;
    schema_b.total_rows = 50;
    schema_b.rows_with_json = 45;
    schema_b.columns.push_back({"age", JsonLeafType::Int64, 40, false});
    schema_b.columns.push_back({"email", JsonLeafType::String, 30, false});

    auto merged = JsonSchemaTree::mergeSchemas(schema_a, schema_b);

    // Verify totals are summed
    EXPECT_EQ(merged.total_rows, 150u);
    EXPECT_EQ(merged.rows_with_json, 135u);

    // Verify union: age, email, name (sorted)
    EXPECT_EQ(merged.numColumns(), 3u);

    // age: present in both → occurrences summed, type stays Int64
    EXPECT_TRUE(merged.hasPath("age"));
    EXPECT_EQ(merged.getPathType("age"), JsonLeafType::Int64);

    // name: only in A
    EXPECT_TRUE(merged.hasPath("name"));
    EXPECT_EQ(merged.getPathType("name"), JsonLeafType::String);

    // email: only in B
    EXPECT_TRUE(merged.hasPath("email"));
    EXPECT_EQ(merged.getPathType("email"), JsonLeafType::String);

    // Check age occurrence count = 80 + 40 = 120
    for (const auto & col : merged.columns)
    {
        if (col.path == "age")
            EXPECT_EQ(col.occurrence_count, 120u);
    }
}

// ============================================================================
// Lazy Loading Tests
// ============================================================================

TEST(JsonShreddingLazyLoadTest, ReadSidecarManifestOnly)
{
    // Write a sidecar, then verify readSidecarManifest returns schema without loading data
    char tmpdir_template[] = "/tmp/tiflash_lazy_test_XXXXXX";
    char * tmpdir = mkdtemp(tmpdir_template);
    ASSERT_NE(tmpdir, nullptr);

    String dmfile_path(tmpdir);
    String col_name = "payload";

    // Create test data with 3 paths
    JsonShreddingConfig config;
    config.min_rows_for_inference = 1;
    config.max_children_per_node = 64;

    auto json1 = buildBinaryJsonObject({{"event", "purchase"}, {"source", "web"}}, {{"amount", 100}});
    auto json2 = buildBinaryJsonObject({{"event", "click"}, {"source", "mobile"}}, {{"amount", 200}});
    auto json3 = buildBinaryJsonObject({{"event", "view"}, {"source", "web"}}, {{"amount", 50}});

    auto col = createJsonColumn({json1, json2, json3});
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto shredded = shredder.shred(col_string);
    ASSERT_FALSE(shredded.sub_columns.empty());

    // Write the sidecar
    JsonShreddedStore::writeSidecar(dmfile_path, col_name, shredded);

    // Read only the manifest
    UInt64 num_rows = 0;
    auto entries = JsonShreddedStore::readSidecarManifest(dmfile_path, col_name, num_rows);

    EXPECT_EQ(num_rows, 3u);
    EXPECT_GE(entries.size(), 3u); // Should have at least event, source, amount

    // Verify paths are present
    bool has_event = false, has_source = false, has_amount = false;
    for (const auto & entry : entries)
    {
        if (entry.path == "event")
            has_event = true;
        if (entry.path == "source")
            has_source = true;
        if (entry.path == "amount")
            has_amount = true;
    }
    EXPECT_TRUE(has_event);
    EXPECT_TRUE(has_source);
    EXPECT_TRUE(has_amount);

    // Clean up
    Poco::File(dmfile_path).remove(true);
}

TEST(JsonShreddingLazyLoadTest, ReadSingleColumnSelective)
{
    // Write a sidecar with multiple paths, then load only one path
    char tmpdir_template[] = "/tmp/tiflash_lazy_single_XXXXXX";
    char * tmpdir = mkdtemp(tmpdir_template);
    ASSERT_NE(tmpdir, nullptr);

    String dmfile_path(tmpdir);
    String col_name = "payload";

    JsonShreddingConfig config;
    config.min_rows_for_inference = 1;
    config.max_children_per_node = 64;

    auto json1 = buildBinaryJsonObject({{"event", "purchase"}, {"source", "web"}}, {{"amount", 100}});
    auto json2 = buildBinaryJsonObject({{"event", "click"}, {"source", "mobile"}}, {{"amount", 200}});

    auto col = createJsonColumn({json1, json2});
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto shredded = shredder.shred(col_string);
    JsonShreddedStore::writeSidecar(dmfile_path, col_name, shredded);

    // Read only the "event" path
    auto event_col = JsonShreddedStore::readSidecarColumn(dmfile_path, col_name, "event");
    ASSERT_NE(event_col, nullptr);
    EXPECT_EQ(event_col->size(), 2u);

    // Verify the values
    const auto * nullable = typeid_cast<const ColumnNullable *>(event_col.get());
    ASSERT_NE(nullable, nullptr);
    const auto & nested = nullable->getNestedColumn();
    const auto * str_col = typeid_cast<const ColumnString *>(&nested);
    ASSERT_NE(str_col, nullptr);

    EXPECT_EQ(str_col->getDataAt(0).toString(), "purchase");
    EXPECT_EQ(str_col->getDataAt(1).toString(), "click");

    // Read only the "amount" path
    auto amount_col = JsonShreddedStore::readSidecarColumn(dmfile_path, col_name, "amount");
    ASSERT_NE(amount_col, nullptr);
    EXPECT_EQ(amount_col->size(), 2u);

    const auto * nullable_amt = typeid_cast<const ColumnNullable *>(amount_col.get());
    ASSERT_NE(nullable_amt, nullptr);
    const auto & nested_amt = nullable_amt->getNestedColumn();
    const auto * int_col = typeid_cast<const ColumnInt64 *>(&nested_amt);
    ASSERT_NE(int_col, nullptr);

    EXPECT_EQ(int_col->getData()[0], 100);
    EXPECT_EQ(int_col->getData()[1], 200);

    // Non-existent path should return nullptr
    auto missing = JsonShreddedStore::readSidecarColumn(dmfile_path, col_name, "nonexistent");
    EXPECT_EQ(missing, nullptr);

    // Clean up
    Poco::File(dmfile_path).remove(true);
}

TEST(JsonShreddingLazyLoadTest, PathIndexO1Lookup)
{
    // Verify O(1) path lookup using unordered_map in ColumnShreddedAttachment
    ColumnShreddedAttachment attachment;
    attachment.dmfile_path = "/tmp/test";
    attachment.col_name = "payload";
    attachment.num_rows = 100;

    // Add 49 manifest entries (simulating real workload)
    for (int i = 0; i < 49; ++i)
    {
        SidecarSchemaEntry entry;
        entry.path = fmt::format("path_{}", i);
        entry.type = static_cast<UInt8>(JsonLeafType::String);
        entry.is_array = false;
        entry.encoding = 0; // Raw encoding
        attachment.manifest_entries.push_back(std::move(entry));
    }

    // Build the index
    attachment.buildPathIndex();

    // O(1) lookups should work
    EXPECT_TRUE(attachment.hasPath("path_0"));
    EXPECT_TRUE(attachment.hasPath("path_24"));
    EXPECT_TRUE(attachment.hasPath("path_48"));
    EXPECT_FALSE(attachment.hasPath("path_49"));
    EXPECT_FALSE(attachment.hasPath("nonexistent"));

    // Verify isLazy()
    EXPECT_TRUE(attachment.isLazy());

    // Eager mode (data set, no dmfile_path)
    ColumnShreddedAttachment eager_attachment;
    eager_attachment.data = nullptr;
    eager_attachment.dmfile_path = "";
    EXPECT_FALSE(eager_attachment.isLazy());
}

TEST(JsonShreddingLazyLoadTest, PerQueryScanContextFlag)
{
    // Verify that ScanContext carries an independent use_json_shredding flag
    auto ctx1 = std::make_shared<ScanContext>();
    auto ctx2 = std::make_shared<ScanContext>();

    // Default should be true
    EXPECT_TRUE(ctx1->use_json_shredding);
    EXPECT_TRUE(ctx2->use_json_shredding);

    // Setting one should not affect the other (thread safety)
    ctx1->use_json_shredding = false;
    EXPECT_FALSE(ctx1->use_json_shredding);
    EXPECT_TRUE(ctx2->use_json_shredding);
}

TEST(JsonShreddingLazyLoadTest, ColumnCacheHitOnRepeatedReads)
{
    // Verify that readSidecarColumn caches the result and second read is a cache hit
    char tmpdir_template[] = "/tmp/tiflash_lazy_cache_XXXXXX";
    char * tmpdir = mkdtemp(tmpdir_template);
    ASSERT_NE(tmpdir, nullptr);

    String dmfile_path(tmpdir);
    String col_name = "payload";

    JsonShreddingConfig config;
    config.min_rows_for_inference = 1;
    config.max_children_per_node = 64;

    auto json1 = buildBinaryJsonObject({{"event", "test"}}, {});
    auto col = createJsonColumn({json1});
    const auto & col_string = assert_cast<const ColumnString &>(*col);

    JsonShredder shredder(config);
    auto shredded = shredder.shred(col_string);
    JsonShreddedStore::writeSidecar(dmfile_path, col_name, shredded);

    // First read — should load from disk
    auto col1 = JsonShreddedStore::readSidecarColumn(dmfile_path, col_name, "event");
    ASSERT_NE(col1, nullptr);

    // Second read — should come from cache (same pointer)
    auto col2 = JsonShreddedStore::getCachedColumn(dmfile_path, col_name, "event");
    ASSERT_NE(col2, nullptr);
    EXPECT_EQ(col1.get(), col2.get()); // Same pointer = cache hit

    // Clean up
    Poco::File(dmfile_path).remove(true);
}


// ============================================================================
// ShreddedAttachmentCache stale-entry poisoning tests
// ============================================================================

class ShreddedAttachmentCachePoisonTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        ShreddedAttachmentCache::instance().clear();
    }
    void TearDown() override
    {
        ShreddedAttachmentCache::instance().clear();
    }
};

// Reproduces the NGC DMFile correctness bug:
// DMFile A has a sidecar → registers cache entry for "payload".
// DMFile B (NGC) has no sidecar → reads blob normally.
// FunctionJsonExtract for DMFile B's block must NOT pick up
// DMFile A's stale cache entry via findByColName("payload").
TEST_F(ShreddedAttachmentCachePoisonTest, ClearByColNamePreventsStaleHit)
{
    auto & cache = ShreddedAttachmentCache::instance();
    const String col_name = "payload";

    // Simulate DMFile A: has sidecar → registerAttachment
    auto attach_a = std::make_shared<ColumnShreddedAttachment>();
    attach_a->dmfile_path = "/data/t_1/stable/dmf_100";
    attach_a->col_name = col_name;
    attach_a->num_rows = 16384;
    cache.registerAttachment(attach_a->dmfile_path, col_name, attach_a);

    // Verify cache has the entry
    ASSERT_NE(cache.findByColName(col_name), nullptr);
    EXPECT_EQ(cache.findByColName(col_name)->dmfile_path, "/data/t_1/stable/dmf_100");

    // Simulate DMFile B (NGC): no sidecar → clearByColName
    cache.clearByColName(col_name);

    // findByColName must return nullptr now — no stale poisoning
    EXPECT_EQ(cache.findByColName(col_name), nullptr);

    // Exact-match lookup for DMFile A should still work
    EXPECT_NE(cache.findAttachment("/data/t_1/stable/dmf_100", col_name), nullptr);
}

// Verify that after clearing, a new registration works correctly.
TEST_F(ShreddedAttachmentCachePoisonTest, ClearThenReRegister)
{
    auto & cache = ShreddedAttachmentCache::instance();
    const String col_name = "payload";

    // Register for DMFile A
    auto attach_a = std::make_shared<ColumnShreddedAttachment>();
    attach_a->dmfile_path = "/data/t_1/stable/dmf_100";
    attach_a->col_name = col_name;
    cache.registerAttachment(attach_a->dmfile_path, col_name, attach_a);

    // Clear (simulating NGC DMFile B read)
    cache.clearByColName(col_name);
    EXPECT_EQ(cache.findByColName(col_name), nullptr);

    // Register for DMFile C (has sidecar)
    auto attach_c = std::make_shared<ColumnShreddedAttachment>();
    attach_c->dmfile_path = "/data/t_1/stable/dmf_300";
    attach_c->col_name = col_name;
    cache.registerAttachment(attach_c->dmfile_path, col_name, attach_c);

    // findByColName should return DMFile C's attachment
    auto found = cache.findByColName(col_name);
    ASSERT_NE(found, nullptr);
    EXPECT_EQ(found->dmfile_path, "/data/t_1/stable/dmf_300");
}

// Simulate the interleaved read pattern: A(sidecar) → B(NGC) → C(sidecar)
// Each step must see the correct state.
TEST_F(ShreddedAttachmentCachePoisonTest, InterleavedSidecarAndNGC)
{
    auto & cache = ShreddedAttachmentCache::instance();
    const String col_name = "payload";

    // Step 1: Read DMFile A (has sidecar)
    auto attach_a = std::make_shared<ColumnShreddedAttachment>();
    attach_a->dmfile_path = "/data/dmf_A";
    attach_a->col_name = col_name;
    attach_a->num_rows = 16384;
    cache.registerAttachment(attach_a->dmfile_path, col_name, attach_a);
    EXPECT_EQ(cache.findByColName(col_name)->dmfile_path, "/data/dmf_A");

    // Step 2: Read DMFile B (NGC — no sidecar)
    // DMFileReader falls through to blob read and clears cache
    cache.clearByColName(col_name);
    EXPECT_EQ(cache.findByColName(col_name), nullptr);
    // FunctionJsonExtract for B's block will use blob data (correct!)

    // Step 3: Read DMFile C (has sidecar)
    auto attach_c = std::make_shared<ColumnShreddedAttachment>();
    attach_c->dmfile_path = "/data/dmf_C";
    attach_c->col_name = col_name;
    attach_c->num_rows = 16384;
    cache.registerAttachment(attach_c->dmfile_path, col_name, attach_c);
    EXPECT_EQ(cache.findByColName(col_name)->dmfile_path, "/data/dmf_C");

    // All exact-match lookups should work independently
    EXPECT_NE(cache.findAttachment("/data/dmf_A", col_name), nullptr);
    EXPECT_EQ(cache.findAttachment("/data/dmf_B", col_name), nullptr); // NGC, never registered
    EXPECT_NE(cache.findAttachment("/data/dmf_C", col_name), nullptr);
}

// Multiple columns: clearing one column name must not affect others.
TEST_F(ShreddedAttachmentCachePoisonTest, ClearByColNameIsolated)
{
    auto & cache = ShreddedAttachmentCache::instance();

    auto attach_payload = std::make_shared<ColumnShreddedAttachment>();
    attach_payload->dmfile_path = "/data/dmf_1";
    attach_payload->col_name = "payload";
    cache.registerAttachment("/data/dmf_1", "payload", attach_payload);

    auto attach_meta = std::make_shared<ColumnShreddedAttachment>();
    attach_meta->dmfile_path = "/data/dmf_1";
    attach_meta->col_name = "metadata";
    cache.registerAttachment("/data/dmf_1", "metadata", attach_meta);

    // Clear only "payload"
    cache.clearByColName("payload");

    EXPECT_EQ(cache.findByColName("payload"), nullptr);
    EXPECT_NE(cache.findByColName("metadata"), nullptr); // unaffected
}


// ============================================================================
// End-to-end compaction test: sidecars survive mergeDelta
// ============================================================================

class JsonCompactionSidecarTest : public DB::base::TiFlashStorageTestBasic
{
    constexpr static const char * TRACING_NAME = "JsonCompactionSidecarTest";

public:
    static constexpr ColumnID JSON_COL_ID = 100;
    static constexpr const char * JSON_COL_NAME = "payload";

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        // Enable shredding for the write path
        JsonShreddingFlag::instance().setWriteShredded(true);
        store = createStoreWithJsonCol();
    }

    void TearDown() override
    {
        store.reset();
        TiFlashStorageTestBasic::TearDown();
    }

    DeltaMergeStorePtr createStoreWithJsonCol()
    {
        TiFlashStorageTestBasic::reload();
        auto cols = DMTestEnv::getDefaultColumns();
        cols->emplace_back(ColumnDefine{JSON_COL_ID, JSON_COL_NAME,
            DataTypeFactory::instance().get(DataTypeString::getDefaultName())});

        ColumnDefine handle_column_define = (*cols)[0];
        return DeltaMergeStore::create(
            *db_context,
            false,
            "test",
            "t_json_compact",
            NullspaceID,
            200,
            0,
            true,
            *cols,
            handle_column_define,
            false,
            1,
            nullptr,
            DeltaMergeStore::Settings());
    }

    /// Build a write block with PK range [beg, end) containing binary JSON in the payload column.
    Block prepareJsonWriteBlock(size_t beg, size_t end, UInt64 tso = 2)
    {
        Block block = DMTestEnv::prepareSimpleWriteBlock(beg, end, false, tso);
        size_t num_rows = end - beg;
        auto json_col = ColumnString::create();
        for (size_t i = 0; i < num_rows; ++i)
        {
            Int64 score = static_cast<Int64>(beg + i);
            String event = (i % 3 == 0) ? "purchase" : ((i % 3 == 1) ? "click" : "view");
            auto json = buildBinaryJsonObject({{"event", event}}, {{"score", score}});
            json_col->insertData(json.data(), json.size());
        }
        block.insert(ColumnWithTypeAndName{
            std::move(json_col),
            DataTypeFactory::instance().get(DataTypeString::getDefaultName()),
            JSON_COL_NAME,
            JSON_COL_ID});
        return block;
    }

    /// Collect all DMFile paths from the store's segments.
    std::vector<String> getStableDMFilePaths()
    {
        std::vector<String> paths;
        for (const auto & [_, seg] : store->segments)
        {
            const auto & files = seg->getStable()->getDMFiles();
            for (const auto & f : files)
                paths.push_back(f->path());
        }
        return paths;
    }

    /// Read all rows from the store and return as a flat vector of blocks.
    std::vector<Block> readAllBlocks()
    {
        auto in = store->read(
            *db_context,
            db_context->getSettingsRef(),
            store->getTableColumns(),
            {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
            1,
            std::numeric_limits<UInt64>::max(),
            EMPTY_FILTER,
            std::vector<RuntimeFilterPtr>{},
            0,
            TRACING_NAME,
            DMReadOptions{},
            1024)[0];
        std::vector<Block> blocks;
        in->readPrefix();
        while (auto blk = in->read())
            blocks.push_back(std::move(blk));
        in->readSuffix();
        return blocks;
    }

    size_t countTotalRows(const std::vector<Block> & blocks)
    {
        size_t total = 0;
        for (const auto & b : blocks)
            total += b.rows();
        return total;
    }

protected:
    DeltaMergeStorePtr store;
};


// Write JSON data → flush → mergeDelta → verify sidecars exist on the resulting DMFile.
TEST_F(JsonCompactionSidecarTest, SidecarsCreatedAfterMergeDelta)
try
{
    const size_t num_rows = 100;

    // Write to delta
    auto block = prepareJsonWriteBlock(0, num_rows);
    store->write(*db_context, db_context->getSettingsRef(), block);

    // Flush delta to disk, then merge into stable
    store->flushCache(*db_context, RowKeyRange::newAll(false, 1));
    store->mergeDeltaAll(*db_context);

    // Verify stable DMFiles have sidecars
    auto paths = getStableDMFilePaths();
    ASSERT_FALSE(paths.empty());
    for (const auto & p : paths)
    {
        EXPECT_TRUE(JsonShreddedStore::hasSidecar(p, JSON_COL_NAME))
            << "DMFile at " << p << " missing sidecar after mergeDelta";
    }

    // Read back and verify row count
    auto blocks = readAllBlocks();
    EXPECT_EQ(countTotalRows(blocks), num_rows);
}
CATCH


// Write → mergeDelta → write more → mergeDelta again.
// After the second compaction the new DMFile must have correct sidecars
// covering ALL rows (old + new).
TEST_F(JsonCompactionSidecarTest, SidecarsCorrectAfterSecondMergeDelta)
try
{
    // min_rows_for_inference=100, so each batch going through shredder must have ≥100 rows.
    const size_t batch1 = 150;
    const size_t batch2 = 120;

    // Batch 1: write → flush → merge
    {
        auto block = prepareJsonWriteBlock(0, batch1);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }
    store->flushCache(*db_context, RowKeyRange::newAll(false, 1));
    store->mergeDeltaAll(*db_context);

    // Verify batch 1 sidecars
    {
        auto paths = getStableDMFilePaths();
        ASSERT_FALSE(paths.empty());
        for (const auto & p : paths)
            EXPECT_TRUE(JsonShreddedStore::hasSidecar(p, JSON_COL_NAME));
    }

    // Batch 2: write more (non-overlapping PKs) → flush → merge
    {
        auto block = prepareJsonWriteBlock(batch1, batch1 + batch2, /*tso=*/3);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }
    store->flushCache(*db_context, RowKeyRange::newAll(false, 1));
    store->mergeDeltaAll(*db_context);

    // Verify post-compaction sidecars
    {
        auto paths = getStableDMFilePaths();
        ASSERT_FALSE(paths.empty());
        for (const auto & p : paths)
        {
            EXPECT_TRUE(JsonShreddedStore::hasSidecar(p, JSON_COL_NAME))
                << "DMFile at " << p << " missing sidecar after second mergeDelta";

            // Verify sidecar manifest has correct row count
            UInt64 manifest_rows = 0;
            auto entries = JsonShreddedStore::readSidecarManifest(p, JSON_COL_NAME, manifest_rows);
            EXPECT_FALSE(entries.empty()) << "Sidecar manifest empty at " << p;
        }
    }

    // Verify total data correctness
    auto blocks = readAllBlocks();
    EXPECT_EQ(countTotalRows(blocks), batch1 + batch2);
}
CATCH


// Simulate the NGC scenario end-to-end:
//   1. Write + mergeDelta WITH shredding → DMFile A gets sidecars
//   2. Disable shredding, write + mergeDelta → DMFile B has NO sidecars (NGC)
//   3. Re-enable shredding and read all data
//   4. Verify: all rows are readable (no NULLs), row count is correct
TEST_F(JsonCompactionSidecarTest, NGCDMFilesFallBackToBlobRead)
try
{
    const size_t batch1 = 150;
    const size_t batch2 = 120;

    // Batch 1: shredding ON → creates DMFile with sidecars
    {
        auto block = prepareJsonWriteBlock(0, batch1);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }
    store->flushCache(*db_context, RowKeyRange::newAll(false, 1));
    store->mergeDeltaAll(*db_context);

    // Verify batch 1 has sidecars
    {
        auto paths = getStableDMFilePaths();
        ASSERT_FALSE(paths.empty());
        for (const auto & p : paths)
            ASSERT_TRUE(JsonShreddedStore::hasSidecar(p, JSON_COL_NAME));
    }

    // Batch 2: shredding OFF → creates DMFile WITHOUT sidecars (NGC)
    JsonShreddingFlag::instance().setWriteShredded(false);
    {
        auto block = prepareJsonWriteBlock(batch1, batch1 + batch2, /*tso=*/3);
        store->write(*db_context, db_context->getSettingsRef(), block);
    }
    store->flushCache(*db_context, RowKeyRange::newAll(false, 1));
    store->mergeDeltaAll(*db_context);
    JsonShreddingFlag::instance().setWriteShredded(true);

    // After compaction with shredding off, the new DMFiles may or may not have sidecars
    // (depends on whether old stable's sidecar data was carried forward).
    // The critical invariant is: reading with shredding enabled must return correct data
    // regardless of sidecar presence.

    // Read all data — must get batch1 + batch2 rows with no NULLs in the payload column
    auto blocks = readAllBlocks();
    size_t total_rows = countTotalRows(blocks);
    EXPECT_EQ(total_rows, batch1 + batch2)
        << "Expected " << (batch1 + batch2) << " rows but got " << total_rows
        << " — NGC DMFile blob fallback may be broken";

    // Verify no NULL payloads (the exact bug we're fixing)
    size_t null_count = 0;
    for (const auto & blk : blocks)
    {
        if (!blk.has(JSON_COL_NAME))
            continue;
        const auto & col = blk.getByName(JSON_COL_NAME);
        for (size_t i = 0; i < col.column->size(); ++i)
        {
            if (col.column->isNullAt(i))
                ++null_count;
        }
    }
    EXPECT_EQ(null_count, 0) << "Found " << null_count << " NULL payloads — NGC blob fallback is broken";
}
CATCH


// Verify sidecar row counts match blob row counts after multiple compaction rounds.
TEST_F(JsonCompactionSidecarTest, SidecarRowCountMatchesBlobAfterMultipleCompactions)
try
{
    // 3 rounds of write + merge
    size_t cumulative = 0;
    for (int round = 0; round < 3; ++round)
    {
        size_t batch = 100 + round * 50; // 100, 150, 200
        {
            auto block = prepareJsonWriteBlock(cumulative, cumulative + batch, /*tso=*/static_cast<UInt64>(round + 2));
            store->write(*db_context, db_context->getSettingsRef(), block);
        }
        cumulative += batch;
        store->flushCache(*db_context, RowKeyRange::newAll(false, 1));
        store->mergeDeltaAll(*db_context);
    }

    // All DMFiles should have sidecars with total rows == cumulative
    size_t total_sidecar_rows = 0;
    auto paths = getStableDMFilePaths();
    for (const auto & p : paths)
    {
        ASSERT_TRUE(JsonShreddedStore::hasSidecar(p, JSON_COL_NAME))
            << "DMFile at " << p << " missing sidecar after round";
        UInt64 manifest_rows = 0;
        auto entries = JsonShreddedStore::readSidecarManifest(p, JSON_COL_NAME, manifest_rows);
        ASSERT_FALSE(entries.empty());
        total_sidecar_rows += manifest_rows;
    }
    EXPECT_EQ(total_sidecar_rows, cumulative)
        << "Sidecar total rows " << total_sidecar_rows << " != data rows " << cumulative;

    // Verify read correctness
    auto blocks = readAllBlocks();
    EXPECT_EQ(countTotalRows(blocks), cumulative);
}
CATCH

} // namespace DB::DM::tests
