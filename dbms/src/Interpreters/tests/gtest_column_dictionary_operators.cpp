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
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/JoinUtils.h>
#include <Interpreters/sortBlock.h>
#include <gtest/gtest.h>

namespace DB::tests
{

class ColumnDictionaryOperatorsTest : public ::testing::Test
{
protected:
    static ColumnDictionary::MutablePtr createStatusColumn()
    {
        // Simulates a low-cardinality "status" column with 5 distinct values, 10 rows
        std::vector<Field> dict = {
            Field(String("active")),
            Field(String("inactive")),
            Field(String("pending")),
            Field(String("deleted")),
            Field(String("archived"))};
        PaddedPODArray<UInt32> ids;
        ids.push_back(0); // active
        ids.push_back(1); // inactive
        ids.push_back(2); // pending
        ids.push_back(0); // active
        ids.push_back(3); // deleted
        ids.push_back(4); // archived
        ids.push_back(0); // active
        ids.push_back(1); // inactive
        ids.push_back(2); // pending
        ids.push_back(3); // deleted
        return ColumnDictionary::createMutable(std::move(dict), std::move(ids), std::make_shared<DataTypeString>());
    }

    static Block createBlockWithDictColumn()
    {
        Block block;
        // String key column (dictionary-encoded)
        block.insert(ColumnWithTypeAndName{
            createStatusColumn()->getPtr(),
            std::make_shared<DataTypeString>(),
            "status"});
        // Int64 value column
        auto int_col = ColumnInt64::create();
        for (int i = 0; i < 10; ++i)
            int_col->insert(Field(static_cast<Int64>(100 + i)));
        block.insert(ColumnWithTypeAndName{
            std::move(int_col),
            std::make_shared<DataTypeInt64>(),
            "revenue"});
        return block;
    }
};

// Bug #3: ColumnDictionary flowing into hash join's extractAndMaterializeKeyColumns
// caused SIGSEGV in HashMethodStringBin because it does assert_cast<const ColumnString &>
TEST_F(ColumnDictionaryOperatorsTest, JoinExtractAndMaterializeKeyColumns)
{
    Block block = createBlockWithDictColumn();
    Columns materialized_columns;
    Strings key_names = {"status"};

    // This should NOT crash — previously it would pass ColumnDictionary to HashMethodStringBin
    // which does assert_cast<const ColumnString &> → SIGSEGV
    ColumnRawPtrs key_columns = extractAndMaterializeKeyColumns(block, materialized_columns, key_names);

    ASSERT_EQ(key_columns.size(), 1);
    // After materialization, the column should be ColumnString (not ColumnDictionary)
    const auto * col_string = typeid_cast<const ColumnString *>(key_columns[0]);
    ASSERT_NE(col_string, nullptr) << "Key column should be materialized to ColumnString";
    ASSERT_EQ(col_string->size(), 10);
    // Verify data integrity
    EXPECT_EQ(col_string->getDataAt(0).toString(), "active");
    EXPECT_EQ(col_string->getDataAt(1).toString(), "inactive");
    EXPECT_EQ(col_string->getDataAt(4).toString(), "deleted");
    EXPECT_EQ(col_string->getDataAt(5).toString(), "archived");
}

// Hash join with multiple key columns, one dictionary-encoded
TEST_F(ColumnDictionaryOperatorsTest, JoinExtractMultipleKeyColumns)
{
    Block block = createBlockWithDictColumn();
    // Add a second string key column (non-dictionary)
    auto region_col = ColumnString::create();
    for (int i = 0; i < 10; ++i)
        region_col->insert(Field(String("region_" + std::to_string(i % 3))));
    block.insert(ColumnWithTypeAndName{
        std::move(region_col),
        std::make_shared<DataTypeString>(),
        "region"});

    Columns materialized_columns;
    Strings key_names = {"status", "region"};

    ColumnRawPtrs key_columns = extractAndMaterializeKeyColumns(block, materialized_columns, key_names);

    ASSERT_EQ(key_columns.size(), 2);
    // First key (dictionary) should be materialized
    ASSERT_NE(typeid_cast<const ColumnString *>(key_columns[0]), nullptr);
    // Second key (already ColumnString) should remain
    ASSERT_NE(typeid_cast<const ColumnString *>(key_columns[1]), nullptr);
}

// Bug #3: ColumnDictionary flowing into sortBlock's multi-column fast path
// caused SIGSEGV via static_cast<const ColumnString *> in ColumnStringCompare::intoTarget
TEST_F(ColumnDictionaryOperatorsTest, SortBlockWithDictionaryColumn)
{
    Block block = createBlockWithDictColumn();

    SortDescription sort_desc;
    sort_desc.emplace_back("status", 1, 1); // ASC, nulls last

    // This should NOT crash — previously ColumnDictionary would reach ColumnStringCompare::intoTarget
    // which does static_cast<const ColumnString *> → SIGSEGV
    sortBlock(block, sort_desc);

    // Verify sort order: active(x3), archived(x1), deleted(x2), inactive(x2), pending(x2)
    const auto & sorted_col = block.getByName("status").column;
    ASSERT_EQ(sorted_col->size(), 10);
    EXPECT_EQ(sorted_col->getDataAt(0).toString(), "active");
    EXPECT_EQ(sorted_col->getDataAt(1).toString(), "active");
    EXPECT_EQ(sorted_col->getDataAt(2).toString(), "active");
    EXPECT_EQ(sorted_col->getDataAt(3).toString(), "archived");
    EXPECT_EQ(sorted_col->getDataAt(4).toString(), "deleted");
    EXPECT_EQ(sorted_col->getDataAt(5).toString(), "deleted");
    EXPECT_EQ(sorted_col->getDataAt(6).toString(), "inactive");
    EXPECT_EQ(sorted_col->getDataAt(7).toString(), "inactive");
    EXPECT_EQ(sorted_col->getDataAt(8).toString(), "pending");
    EXPECT_EQ(sorted_col->getDataAt(9).toString(), "pending");
}

// Multi-column sort: dictionary string + integer
TEST_F(ColumnDictionaryOperatorsTest, SortBlockMultiColumnWithDictionary)
{
    Block block = createBlockWithDictColumn();

    SortDescription sort_desc;
    sort_desc.emplace_back("status", 1, 1);  // ASC
    sort_desc.emplace_back("revenue", -1, 1); // DESC

    sortBlock(block, sort_desc);

    // "active" rows had revenues 100, 103, 106 → sorted DESC: 106, 103, 100
    const auto & sorted_status = block.getByName("status").column;
    const auto & sorted_revenue = block.getByName("revenue").column;
    EXPECT_EQ(sorted_status->getDataAt(0).toString(), "active");
    EXPECT_EQ(sorted_status->getDataAt(1).toString(), "active");
    EXPECT_EQ(sorted_status->getDataAt(2).toString(), "active");
    EXPECT_EQ(sorted_revenue->getInt(0), 106);
    EXPECT_EQ(sorted_revenue->getInt(1), 103);
    EXPECT_EQ(sorted_revenue->getInt(2), 100);
}

// Sort with collation — this previously threw "Collations could be specified only for String columns"
// because NeedCollation does typeid_cast<const ColumnString *> on ColumnDictionary which returns nullptr
TEST_F(ColumnDictionaryOperatorsTest, SortBlockWithCollation)
{
    Block block = createBlockWithDictColumn();

    auto collator = TiDB::ITiDBCollator::getCollator(TiDB::ITiDBCollator::BINARY);
    SortDescription sort_desc;
    SortColumnDescription col_desc("status", 1, 1);
    col_desc.collator = collator;
    sort_desc.push_back(col_desc);

    // Should not crash or throw
    ASSERT_NO_THROW(sortBlock(block, sort_desc));

    const auto & sorted_col = block.getByName("status").column;
    EXPECT_EQ(sorted_col->getDataAt(0).toString(), "active");
}

// Sort with TopN limit
TEST_F(ColumnDictionaryOperatorsTest, SortBlockWithLimitTopN)
{
    Block block = createBlockWithDictColumn();

    SortDescription sort_desc;
    sort_desc.emplace_back("status", 1, 1);

    sortBlock(block, sort_desc, 3); // TopN=3

    // Only top 3 rows need to be correctly sorted (all "active")
    const auto & sorted_col = block.getByName("status").column;
    EXPECT_EQ(sorted_col->getDataAt(0).toString(), "active");
    EXPECT_EQ(sorted_col->getDataAt(1).toString(), "active");
    EXPECT_EQ(sorted_col->getDataAt(2).toString(), "active");
}

// Verify non-dictionary columns pass through unchanged
TEST_F(ColumnDictionaryOperatorsTest, JoinNonDictionaryColumnsUnchanged)
{
    auto col_string = ColumnString::create();
    col_string->insert(Field(String("hello")));
    col_string->insert(Field(String("world")));

    Block block;
    block.insert(ColumnWithTypeAndName{std::move(col_string), std::make_shared<DataTypeString>(), "key"});

    Columns materialized_columns;
    Strings key_names = {"key"};

    const IColumn * original_ptr = block.getByName("key").column.get();
    ColumnRawPtrs key_columns = extractAndMaterializeKeyColumns(block, materialized_columns, key_names);

    // Non-dictionary column should not be re-materialized
    EXPECT_EQ(key_columns[0], original_ptr);
    EXPECT_TRUE(materialized_columns.empty());
}

// Simulate concurrent access pattern: multiple threads materializing the same ColumnDictionary
TEST_F(ColumnDictionaryOperatorsTest, ConcurrentJoinMaterialization)
{
    ColumnPtr dict_col = createStatusColumn()->getPtr();
    Block block;
    block.insert(ColumnWithTypeAndName{dict_col, std::make_shared<DataTypeString>(), "status"});

    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int t = 0; t < 4; ++t)
    {
        threads.emplace_back([&]() {
            // Each thread creates its own materialization context (as in real hash join)
            Columns materialized_columns;
            Strings key_names = {"status"};
            ColumnRawPtrs key_columns = extractAndMaterializeKeyColumns(block, materialized_columns, key_names);

            // Verify the materialized column is valid ColumnString
            const auto * col_string = typeid_cast<const ColumnString *>(key_columns[0]);
            if (col_string && col_string->size() == 10 && col_string->getDataAt(0).toString() == "active")
                success_count.fetch_add(1);
        });
    }

    for (auto & t : threads)
        t.join();

    EXPECT_EQ(success_count.load(), 4);
}

} // namespace DB::tests
