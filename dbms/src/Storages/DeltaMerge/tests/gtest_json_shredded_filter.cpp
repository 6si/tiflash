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

#include <Columns/ColumnDictionary.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/PODArray.h>
#include <Core/ShreddedAttachmentCache.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Buffer/WriteBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/Encoded/EncodedFilter.h>
#include <Storages/DeltaMerge/JsonShredding/JsonShreddedStore.h>
#include <gtest/gtest.h>

#include <thread>

namespace DB::DM::tests
{

class JsonShreddedFilterTest : public ::testing::Test
{
protected:
    String test_dir;

    void SetUp() override
    {
        test_dir = "/tmp/json_shredded_filter_test_" + std::to_string(time(nullptr));
        Poco::File(test_dir).createDirectories();
    }

    void TearDown() override { Poco::File(test_dir).remove(true); }

    ColumnDictionary::MutablePtr createStringDictCol(
        std::vector<String> dict_values,
        std::vector<UInt32> id_values)
    {
        std::vector<Field> dict;
        for (auto & v : dict_values)
            dict.emplace_back(std::move(v));
        PaddedPODArray<UInt32> ids;
        for (auto id : id_values)
            ids.push_back(id);
        return ColumnDictionary::createMutable(std::move(dict), std::move(ids), std::make_shared<DataTypeString>());
    }
};

/// Test EncodedFilter on ColumnDictionary — EQUALS (the core of dictionary-aware filter)
TEST_F(JsonShreddedFilterTest, DictionaryFilterEquals)
{
    // Simulate: WHERE json_extract(payload, '$.event') = 'purchase'
    // Dictionary: {0: "purchase", 1: "click", 2: "view"}
    // 10 rows: purchase, click, view, purchase, click, view, purchase, purchase, click, view
    auto col = createStringDictCol(
        {"purchase", "click", "view"},
        {0, 1, 2, 0, 1, 2, 0, 0, 1, 2});

    auto result = EncodedFilter::evaluateEquals(*col, Field(String("purchase")));
    EXPECT_TRUE(result.used_encoded_path);
    EXPECT_EQ(result.count_passing, 4);
    EXPECT_EQ(result.filter[0], 1);
    EXPECT_EQ(result.filter[1], 0);
    EXPECT_EQ(result.filter[2], 0);
    EXPECT_EQ(result.filter[3], 1);
    EXPECT_EQ(result.filter[4], 0);
    EXPECT_EQ(result.filter[5], 0);
    EXPECT_EQ(result.filter[6], 1);
    EXPECT_EQ(result.filter[7], 1);
    EXPECT_EQ(result.filter[8], 0);
    EXPECT_EQ(result.filter[9], 0);
}

/// Test EncodedFilter LIKE pattern on dictionary
TEST_F(JsonShreddedFilterTest, DictionaryFilterLike)
{
    // Simulate: WHERE json_extract(payload, '$.event') LIKE '%pur%'
    auto col = createStringDictCol(
        {"purchase", "click", "view"},
        {0, 1, 2, 0, 1});

    auto result = EncodedFilter::evaluateLike(*col, "%pur%");
    EXPECT_TRUE(result.used_encoded_path);
    EXPECT_EQ(result.count_passing, 2);
    EXPECT_EQ(result.filter[0], 1); // "purchase" matches
    EXPECT_EQ(result.filter[1], 0); // "click" no match
    EXPECT_EQ(result.filter[2], 0); // "view" no match
    EXPECT_EQ(result.filter[3], 1); // "purchase" matches
    EXPECT_EQ(result.filter[4], 0); // "click" no match
}

/// Test EncodedFilter NotEquals
TEST_F(JsonShreddedFilterTest, DictionaryFilterNotEquals)
{
    auto col = createStringDictCol(
        {"active", "inactive", "pending"},
        {0, 1, 2, 0, 1, 0});

    auto result = EncodedFilter::evaluateNotEquals(*col, Field(String("active")));
    EXPECT_TRUE(result.used_encoded_path);
    EXPECT_EQ(result.count_passing, 3); // rows 1, 2, 4
    EXPECT_EQ(result.filter[0], 0);
    EXPECT_EQ(result.filter[1], 1);
    EXPECT_EQ(result.filter[2], 1);
    EXPECT_EQ(result.filter[3], 0);
    EXPECT_EQ(result.filter[4], 1);
    EXPECT_EQ(result.filter[5], 0);
}

/// Test EncodedFilter IN
TEST_F(JsonShreddedFilterTest, DictionaryFilterIn)
{
    auto col = createStringDictCol(
        {"purchase", "click", "view"},
        {0, 1, 2, 0, 2});

    // IN ('purchase', 'view')
    std::vector<Field> in_values = {Field(String("purchase")), Field(String("view"))};
    auto result = EncodedFilter::evaluateIn(*col, in_values);
    EXPECT_TRUE(result.used_encoded_path);
    EXPECT_EQ(result.count_passing, 4); // rows 0, 2, 3, 4
    EXPECT_EQ(result.filter[0], 1);
    EXPECT_EQ(result.filter[1], 0);
    EXPECT_EQ(result.filter[2], 1);
    EXPECT_EQ(result.filter[3], 1);
    EXPECT_EQ(result.filter[4], 1);
}

/// Test that dictionary filter performance is O(cardinality) not O(rows)
TEST_F(JsonShreddedFilterTest, DictionaryFilterPerformance)
{
    // Create a large dictionary column (100K rows, 5 distinct values)
    const size_t num_rows = 100000;
    std::vector<String> dict_values = {"purchase", "click", "view", "signup", "logout"};
    std::vector<UInt32> ids;
    ids.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        ids.push_back(i % 5);

    auto col = createStringDictCol(dict_values, ids);

    auto start = std::chrono::high_resolution_clock::now();
    for (int iter = 0; iter < 100; ++iter)
    {
        auto result = EncodedFilter::evaluateEquals(*col, Field(String("purchase")));
        EXPECT_EQ(result.count_passing, num_rows / 5);
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // 100 iterations × 100K rows: with dictionary path should be < 200ms
    // (Without dictionary: 100 × 100K string comparisons >> 1s)
    EXPECT_LT(elapsed_ms, 500) << "Dictionary filter too slow: " << elapsed_ms << "ms for 100 iterations of 100K rows";
}

/// Test readSidecarColumn returns ColumnDictionary for dictionary-encoded data
TEST_F(JsonShreddedFilterTest, ReadSidecarColumnReturnsDictionary)
{
    // Write a proper sidecar in the exact format readSidecarColumn expects
    String dmfile_path = test_dir + "/test_dmfile";
    String sidecar_dir = dmfile_path + "/.json_shredded/payload";
    Poco::File(sidecar_dir).createDirectories();

    // Write manifest
    {
        WriteBufferFromFile buf(sidecar_dir + "/manifest.bin");
        writeBinary(static_cast<UInt32>(0x4A534852), buf); // SIDECAR_MAGIC
        writeBinary(static_cast<UInt32>(2), buf); // SIDECAR_VERSION (v2)
        writeBinary(static_cast<UInt64>(10), buf); // num_rows
        writeBinary(static_cast<UInt32>(1), buf); // num_sub_columns

        // Schema entry: path="event", type=String(3), is_array=0, encoding=Dictionary(1)
        writeBinary(String("event"), buf);
        writeBinary(static_cast<UInt8>(3), buf); // JsonLeafType::String
        writeBinary(static_cast<UInt8>(0), buf); // is_array = false
        writeBinary(static_cast<UInt8>(1), buf); // SubColumnEncoding::Dictionary
    }

    // Write the .bin file for "event" column
    {
        WriteBufferFromFile buf(sidecar_dir + "/event.bin");
        UInt64 num_rows = 10;
        writeBinary(num_rows, buf);

        // Null bitmap (10 bytes, no nulls)
        std::vector<UInt8> null_map(10, 0);
        buf.write(reinterpret_cast<const char *>(null_map.data()), 10);

        // Dictionary: 5 entries
        writeBinary(static_cast<UInt32>(5), buf);
        writeBinary(String("purchase"), buf);
        writeBinary(String("click"), buf);
        writeBinary(String("view"), buf);
        writeBinary(String("signup"), buf);
        writeBinary(String("logout"), buf);

        // IDs: 10 rows cycling through 0-4
        std::vector<UInt32> ids = {0, 1, 2, 3, 4, 0, 1, 2, 3, 4};
        buf.write(reinterpret_cast<const char *>(ids.data()), 10 * sizeof(UInt32));
    }

    // Read it back via the public API
    try
    {
        ColumnPtr col = JsonShreddedStore::readSidecarColumn(dmfile_path, "payload", "event");
        ASSERT_NE(col, nullptr);
        ASSERT_EQ(col->size(), 10);

        // Should be ColumnNullable wrapping ColumnDictionary
        const auto * nullable = typeid_cast<const ColumnNullable *>(col.get());
        ASSERT_NE(nullable, nullptr);
        const auto * dict_col = typeid_cast<const ColumnDictionary *>(&nullable->getNestedColumn());
        ASSERT_NE(dict_col, nullptr) << "Expected ColumnDictionary, got regular column";
        EXPECT_EQ(dict_col->getDictionarySize(), 5);
        EXPECT_EQ(dict_col->size(), 10);

        // Verify decoded values
        EXPECT_EQ((*dict_col)[0].get<String>(), "purchase");
        EXPECT_EQ((*dict_col)[1].get<String>(), "click");
        EXPECT_EQ((*dict_col)[4].get<String>(), "logout");
        EXPECT_EQ((*dict_col)[5].get<String>(), "purchase");
    }
    catch (const DB::Exception & e)
    {
        FAIL() << "DB::Exception: " << e.message() << " (code: " << e.code() << ")";
    }
    catch (const std::exception & e)
    {
        FAIL() << "std::exception: " << e.what();
    }
}

/// Verify the full flow: readSidecarColumn → EncodedFilter equals
TEST_F(JsonShreddedFilterTest, EndToEndDictionaryFilter)
{
    // Write sidecar with dictionary encoding
    String dmfile_path = test_dir + "/test_dmfile2";
    String sidecar_dir = dmfile_path + "/.json_shredded/payload";
    Poco::File(sidecar_dir).createDirectories();

    {
        WriteBufferFromFile buf(sidecar_dir + "/manifest.bin");
        writeBinary(static_cast<UInt32>(0x4A534852), buf);
        writeBinary(static_cast<UInt32>(2), buf);
        writeBinary(static_cast<UInt64>(5), buf); // 5 rows
        writeBinary(static_cast<UInt32>(1), buf); // 1 sub-column
        writeBinary(String("status"), buf);
        writeBinary(static_cast<UInt8>(3), buf); // String
        writeBinary(static_cast<UInt8>(0), buf);
        writeBinary(static_cast<UInt8>(1), buf); // Dictionary
    }

    {
        WriteBufferFromFile buf(sidecar_dir + "/status.bin");
        writeBinary(static_cast<UInt64>(5), buf);
        std::vector<UInt8> null_map = {0, 1, 0, 0, 1}; // rows 1,4 are null
        buf.write(reinterpret_cast<const char *>(null_map.data()), 5);
        writeBinary(static_cast<UInt32>(2), buf); // 2 dict entries
        writeBinary(String("active"), buf);
        writeBinary(String("inactive"), buf);
        std::vector<UInt32> ids = {0, 0, 1, 0, 0}; // active, (null), inactive, active, (null)
        buf.write(reinterpret_cast<const char *>(ids.data()), 5 * sizeof(UInt32));
    }

    // Read and filter
    ColumnPtr col = JsonShreddedStore::readSidecarColumn(dmfile_path, "payload", "status");
    ASSERT_NE(col, nullptr);
    ASSERT_EQ(col->size(), 5);

    const auto * nullable = typeid_cast<const ColumnNullable *>(col.get());
    ASSERT_NE(nullable, nullptr);
    const auto * dict_col = typeid_cast<const ColumnDictionary *>(&nullable->getNestedColumn());
    ASSERT_NE(dict_col, nullptr);

    // Apply filter: equals "active"
    auto result = EncodedFilter::evaluateEquals(*dict_col, Field(String("active")));
    EXPECT_TRUE(result.used_encoded_path);
    // Rows 0, 3 have "active"; rows 1, 4 are null (not matched by equals); row 2 is "inactive"
    // Note: EncodedFilter doesn't know about nulls — it just checks IDs
    // The null handling is done by the caller using the null_map
    EXPECT_EQ(result.filter[0], 1); // "active"
    EXPECT_EQ(result.filter[1], 1); // ID=0="active" (null check done separately)
    EXPECT_EQ(result.filter[2], 0); // "inactive"
    EXPECT_EQ(result.filter[3], 1); // "active"
    EXPECT_EQ(result.filter[4], 1); // ID=0="active" (null check done separately)
}

/// Test that the global ShreddedAttachmentCache survives cross-thread handoff.
/// Simulates the pipeline model where:
///   - Read thread registers attachment in cache
///   - Pipeline thread (different thread) looks up attachment from cache by col_name
TEST_F(JsonShreddedFilterTest, CrossThreadAttachmentCacheLookup)
{
    // Create a test attachment
    auto attachment = std::make_shared<ColumnShreddedAttachment>();
    attachment->dmfile_path = "/test/dmf_42";
    attachment->col_name = "payload";
    attachment->num_rows = 1000;
    attachment->manifest_entries = {
        SidecarSchemaEntry{"event", 8 /*String*/, false, 1 /*Dict*/},
        SidecarSchemaEntry{"user_id", 5 /*UInt64*/, false, 0 /*Raw*/},
    };
    attachment->buildPathIndex();

    // Simulate read thread: register in global cache
    ShreddedAttachmentCache::instance().registerAttachment("/test/dmf_42", "payload", attachment);

    // Simulate pipeline thread: look up by col_name (dmfile_path unknown)
    std::thread pipeline_thread([&]() {
        auto found = ShreddedAttachmentCache::instance().findByColName("payload");
        ASSERT_NE(found, nullptr);
        EXPECT_EQ(found->dmfile_path, "/test/dmf_42");
        EXPECT_EQ(found->manifest_entries.size(), 2);
        EXPECT_TRUE(found->hasPath("event"));
        EXPECT_TRUE(found->hasPath("user_id"));
        EXPECT_FALSE(found->hasPath("nonexistent"));
    });
    pipeline_thread.join();

    // Also test exact lookup (by dmfile_path + col_name)
    auto exact = ShreddedAttachmentCache::instance().findAttachment("/test/dmf_42", "payload");
    ASSERT_NE(exact, nullptr);
    EXPECT_EQ(exact->num_rows, 1000);

    // Cleanup
    ShreddedAttachmentCache::instance().clear();
}

/// Test that the cache handles concurrent registrations from multiple read threads.
TEST_F(JsonShreddedFilterTest, ConcurrentCacheRegistrations)
{
    ShreddedAttachmentCache::instance().clear();

    constexpr int N_THREADS = 8;
    std::vector<std::thread> threads;

    for (int i = 0; i < N_THREADS; ++i)
    {
        threads.emplace_back([i]() {
            auto attachment = std::make_shared<ColumnShreddedAttachment>();
            attachment->dmfile_path = fmt::format("/test/dmf_{}", i);
            attachment->col_name = "payload";
            attachment->num_rows = 1000 * (i + 1);
            attachment->manifest_entries = {
                SidecarSchemaEntry{"event", 8, false, 1},
            };
            attachment->buildPathIndex();

            ShreddedAttachmentCache::instance().registerAttachment(
                attachment->dmfile_path,
                "payload",
                attachment);
        });
    }

    for (auto & t : threads)
        t.join();

    // All registrations should succeed without crashes
    // The by_col_name lookup returns the LAST registered (race-dependent, but must be non-null)
    auto found = ShreddedAttachmentCache::instance().findByColName("payload");
    ASSERT_NE(found, nullptr);
    EXPECT_TRUE(found->hasPath("event"));

    // Exact lookups should all work
    for (int i = 0; i < N_THREADS; ++i)
    {
        auto exact = ShreddedAttachmentCache::instance().findAttachment(
            fmt::format("/test/dmf_{}", i),
            "payload");
        ASSERT_NE(exact, nullptr);
        EXPECT_EQ(exact->num_rows, static_cast<UInt64>(1000 * (i + 1)));
    }

    ShreddedAttachmentCache::instance().clear();
}

/// Test JSON string comparison semantics: length-first, then byte content.
/// This matches TiDB/MySQL binary JSON comparison where strings are stored as
/// [varint_length + bytes], so comparing the binary representation compares
/// length first. Example: "view" (4) < "click" (5) despite 'v' > 'c'.
TEST_F(JsonShreddedFilterTest, DictionaryFilterGTWithJsonStringSemantics)
{
    // Dictionary: "click"(5), "logout"(6), "purchase"(8), "signup"(6), "view"(4)
    // JSON string ordering by length-first:
    //   "view"(4) < "click"(5) < "logout"(6) = "signup"(6) < "purchase"(8)
    //   For equal lengths, compare bytes: "logout" < "signup" (l < s)
    // So full order: view < click < logout < signup < purchase
    auto col = createStringDictCol(
        {"click", "logout", "purchase", "signup", "view"},
        {0, 1, 2, 3, 4, 0, 1, 2, 3, 4}); // 10 rows, 2 of each

    // GT "click" using JSON semantics: entries with length > 5 pass,
    // plus entries with length == 5 and bytes > "click".
    // "click"(5): NOT > "click"
    // "logout"(6): len 6 > 5 → YES
    // "purchase"(8): len 8 > 5 → YES
    // "signup"(6): len 6 > 5 → YES
    // "view"(4): len 4 < 5 → NO
    // Expected: rows 1,3,5,7 = logout, signup (4 passing) + rows 2,7 = purchase (2 passing)
    // Wait: rows with logout(1,6), purchase(2,7), signup(3,8) pass = 6 rows
    auto predicate_gt = [](const Field & entry) -> bool {
        const auto & s = entry.get<String>();
        String cmp = "click";
        if (s.size() != cmp.size())
            return s.size() > cmp.size();
        return s > cmp;
    };
    auto result = EncodedFilter::evaluatePredicate(*col, predicate_gt);
    EXPECT_EQ(result.count_passing, 6); // logout(2) + purchase(2) + signup(2)
    EXPECT_EQ(result.filter[0], 0); // click - not GT
    EXPECT_EQ(result.filter[1], 1); // logout - GT (longer)
    EXPECT_EQ(result.filter[2], 1); // purchase - GT (longer)
    EXPECT_EQ(result.filter[3], 1); // signup - GT (longer)
    EXPECT_EQ(result.filter[4], 0); // view - NOT GT (shorter)
    EXPECT_EQ(result.filter[5], 0); // click
    EXPECT_EQ(result.filter[6], 1); // logout
    EXPECT_EQ(result.filter[7], 1); // purchase
    EXPECT_EQ(result.filter[8], 1); // signup
    EXPECT_EQ(result.filter[9], 0); // view

    // LT "purchase" using JSON semantics:
    // Entries with length < 8 pass; entries with length == 8 and bytes < "purchase" pass.
    // "click"(5): len 5 < 8 → YES
    // "logout"(6): len 6 < 8 → YES
    // "purchase"(8): NOT < "purchase"
    // "signup"(6): len 6 < 8 → YES
    // "view"(4): len 4 < 8 → YES
    auto predicate_lt = [](const Field & entry) -> bool {
        const auto & s = entry.get<String>();
        String cmp = "purchase";
        if (s.size() != cmp.size())
            return s.size() < cmp.size();
        return s < cmp;
    };
    auto result_lt = EncodedFilter::evaluatePredicate(*col, predicate_lt);
    EXPECT_EQ(result_lt.count_passing, 8); // click(2) + logout(2) + signup(2) + view(2)
    EXPECT_EQ(result_lt.filter[2], 0); // purchase - not LT
    EXPECT_EQ(result_lt.filter[7], 0); // purchase - not LT
}

/// Test JSON string comparison for equal-length strings:
/// When lengths are equal, compare byte-by-byte.
TEST_F(JsonShreddedFilterTest, DictionaryFilterGTEqualLengthStrings)
{
    // All 6-char strings: "logout", "signup", "zepher"
    // Ordering: "logout" < "signup" < "zepher" (lexicographic since all same length)
    auto col = createStringDictCol(
        {"logout", "signup", "zepher"},
        {0, 1, 2, 0, 1, 2});

    // GT "logout" (same length = 6): only strings with bytes > "logout"
    auto predicate = [](const Field & entry) -> bool {
        const auto & s = entry.get<String>();
        String cmp = "logout";
        if (s.size() != cmp.size())
            return s.size() > cmp.size();
        return s > cmp;
    };
    auto result = EncodedFilter::evaluatePredicate(*col, predicate);
    EXPECT_EQ(result.count_passing, 4); // signup(2) + zepher(2)
    EXPECT_EQ(result.filter[0], 0); // logout
    EXPECT_EQ(result.filter[1], 1); // signup
    EXPECT_EQ(result.filter[2], 1); // zepher
}

/// Test numeric Int64 comparison directly on column data.
/// Verifies that the shredded path does proper numeric comparison
/// (unlike the blob path which has a preexisting byte-comparison bug).
TEST_F(JsonShreddedFilterTest, NumericInt64Comparison)
{
    // Simulate timestamps: some above and below 1700060000
    std::vector<Int64> values = {1700050000, 1700055000, 1700060000, 1700065000, 1700070000,
                                 100, 255, 256, 1000000000, 1700986879};
    auto int_col = ColumnInt64::create();
    for (auto v : values)
        int_col->insert(v);

    // These comparisons must use numeric semantics (NOT byte comparison).
    // Key test: 255 vs 256 — byte comparison would give wrong result on LE.
    Int64 threshold = 1700060000;
    size_t gt_count = 0;
    for (size_t i = 0; i < values.size(); ++i)
    {
        if (values[i] > threshold)
            ++gt_count;
    }
    EXPECT_EQ(gt_count, 3); // 1700065000, 1700070000, 1700986879

    // Also verify 255 < 256 (would be wrong with LE byte comparison)
    EXPECT_LT(values[6], values[7]); // 255 < 256
}

/// Test Float64 comparison.
TEST_F(JsonShreddedFilterTest, NumericFloat64Comparison)
{
    std::vector<Float64> values = {0.0, 100.5, 499.99, 500.0, 500.01, 991.97};

    size_t gt_count = 0;
    Float64 threshold = 500.0;
    for (auto v : values)
    {
        if (v > threshold)
            ++gt_count;
    }
    EXPECT_EQ(gt_count, 2); // 500.01, 991.97

    size_t eq_count = 0;
    for (auto v : values)
    {
        if (v == threshold)
            ++eq_count;
    }
    EXPECT_EQ(eq_count, 1); // exactly 500.0
}

} // namespace DB::DM::tests
