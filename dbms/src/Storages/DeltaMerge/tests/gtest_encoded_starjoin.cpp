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
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/DeltaMerge/Encoded/EncodedStarJoin.h>
#include <gtest/gtest.h>

namespace DB::DM::tests
{

class EncodedStarJoinTest : public ::testing::Test
{
protected:
    ColumnDictionary::MutablePtr createIntDictCol(
        std::vector<Int64> dict_values,
        std::vector<UInt32> id_values)
    {
        std::vector<Field> dict;
        for (auto v : dict_values)
            dict.emplace_back(v);
        PaddedPODArray<UInt32> ids;
        for (auto id : id_values)
            ids.push_back(id);
        return ColumnDictionary::createMutable(std::move(dict), std::move(ids), std::make_shared<DataTypeInt64>());
    }
};

TEST_F(EncodedStarJoinTest, BuildDimensionTable)
{
    // Dimension table: id -> region_id
    // {1: 10, 2: 20, 3: 10, 4: 30}
    PaddedPODArray<Int64> dim_keys = {1, 2, 3, 4};
    PaddedPODArray<Int64> dim_values = {10, 20, 10, 30};

    auto table = EncodedStarJoin::buildDimensionTable(dim_keys, dim_values);

    EXPECT_EQ(table.key_to_value.size(), 4u);
    EXPECT_EQ(table.key_to_value[1], 10);
    EXPECT_EQ(table.key_to_value[2], 20);
    EXPECT_EQ(table.key_to_value[3], 10);
    EXPECT_EQ(table.key_to_value[4], 30);
    EXPECT_EQ(table.num_groups, 3u); // 3 distinct values: 10, 20, 30
}

TEST_F(EncodedStarJoinTest, FusedJoinGroupBySum)
{
    // Fact table: dim_id column (dictionary-encoded)
    // Dictionary: {0: 1, 1: 2, 2: 3, 3: 5}  (5 won't match)
    // IDs: [0, 1, 2, 0, 1, 3, 0]
    // Amounts: [100, 200, 300, 400, 500, 600, 700]
    //
    // Dimension table: id -> group_value
    // {1: 10, 2: 20, 3: 10}  (key 5 not in dimension → filtered out)
    //
    // Expected join+groupby+sum:
    //   group 10 (keys 1, 3): rows 0,2,3,6 → sum = 100+300+400+700 = 1500
    //   group 20 (key 2): rows 1,4 → sum = 200+500 = 700
    //   row 5 (key 5) → filtered out (no match)

    auto fact_col = createIntDictCol({1, 2, 3, 5}, {0, 1, 2, 0, 1, 3, 0});
    PaddedPODArray<Int64> amounts = {100, 200, 300, 400, 500, 600, 700};

    PaddedPODArray<Int64> dim_keys = {1, 2, 3};
    PaddedPODArray<Int64> dim_values = {10, 20, 10};
    auto dim_table = EncodedStarJoin::buildDimensionTable(dim_keys, dim_values);

    auto result = EncodedStarJoin::fusedJoinGroupBySum(*fact_col, amounts, dim_table);

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.num_groups, 2u);
    EXPECT_EQ(result.rows_joined, 6u);
    EXPECT_EQ(result.rows_not_joined, 1u);

    // Group 10: sum = 100 + 300 + 400 + 700 = 1500
    EXPECT_EQ(result.aggregated_values[0].get<Int64>(), 1500);
    EXPECT_EQ(result.group_counts[0], 4u);

    // Group 20: sum = 200 + 500 = 700
    EXPECT_EQ(result.aggregated_values[1].get<Int64>(), 700);
    EXPECT_EQ(result.group_counts[1], 2u);
}

TEST_F(EncodedStarJoinTest, FusedJoinGroupByCount)
{
    auto fact_col = createIntDictCol({1, 2, 3, 99}, {0, 1, 2, 3, 0, 1, 2, 3});

    PaddedPODArray<Int64> dim_keys = {1, 2, 3};
    PaddedPODArray<Int64> dim_values = {100, 200, 100};
    auto dim_table = EncodedStarJoin::buildDimensionTable(dim_keys, dim_values);

    auto result = EncodedStarJoin::fusedJoinGroupByCount(*fact_col, dim_table);

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.num_groups, 2u);
    EXPECT_EQ(result.rows_joined, 6u);
    EXPECT_EQ(result.rows_not_joined, 2u);

    // Group 100 (keys 1, 3): 4 rows
    EXPECT_EQ(result.aggregated_values[0].get<UInt64>(), 4u);
    // Group 200 (key 2): 2 rows
    EXPECT_EQ(result.aggregated_values[1].get<UInt64>(), 2u);
}

TEST_F(EncodedStarJoinTest, ComputeJoinFilter)
{
    auto fact_col = createIntDictCol({10, 20, 30, 40}, {0, 1, 2, 3, 0, 1});

    PaddedPODArray<Int64> dim_keys = {10, 30};
    PaddedPODArray<Int64> dim_values = {1, 1};
    auto dim_table = EncodedStarJoin::buildDimensionTable(dim_keys, dim_values);

    auto filter = EncodedStarJoin::computeJoinFilter(*fact_col, dim_table);

    ASSERT_EQ(filter.size(), 6u);
    EXPECT_EQ(filter[0], 1); // 10 matches
    EXPECT_EQ(filter[1], 0); // 20 no match
    EXPECT_EQ(filter[2], 1); // 30 matches
    EXPECT_EQ(filter[3], 0); // 40 no match
    EXPECT_EQ(filter[4], 1); // 10 matches
    EXPECT_EQ(filter[5], 0); // 20 no match
}

TEST_F(EncodedStarJoinTest, NoMatchesJoin)
{
    auto fact_col = createIntDictCol({100, 200, 300}, {0, 1, 2, 0, 1});
    PaddedPODArray<Int64> amounts = {10, 20, 30, 40, 50};

    PaddedPODArray<Int64> dim_keys = {999, 998};
    PaddedPODArray<Int64> dim_values = {1, 2};
    auto dim_table = EncodedStarJoin::buildDimensionTable(dim_keys, dim_values);

    auto result = EncodedStarJoin::fusedJoinGroupBySum(*fact_col, amounts, dim_table);

    ASSERT_TRUE(result.used_encoded_path);
    EXPECT_EQ(result.rows_joined, 0u);
    EXPECT_EQ(result.rows_not_joined, 5u);
    // All groups should have 0 sum
    for (size_t i = 0; i < result.num_groups; ++i)
        EXPECT_EQ(result.aggregated_values[i].get<Int64>(), 0);
}

TEST_F(EncodedStarJoinTest, AllMatchJoin)
{
    auto fact_col = createIntDictCol({1, 2}, {0, 1, 0, 1, 0, 1});
    PaddedPODArray<Int64> amounts = {10, 20, 30, 40, 50, 60};

    PaddedPODArray<Int64> dim_keys = {1, 2};
    PaddedPODArray<Int64> dim_values = {100, 200};
    auto dim_table = EncodedStarJoin::buildDimensionTable(dim_keys, dim_values);

    auto result = EncodedStarJoin::fusedJoinGroupBySum(*fact_col, amounts, dim_table);

    ASSERT_TRUE(result.used_encoded_path);
    EXPECT_EQ(result.rows_joined, 6u);
    EXPECT_EQ(result.rows_not_joined, 0u);
    // Group 100: keys 1 → rows 0,2,4 → sum = 10+30+50 = 90
    EXPECT_EQ(result.aggregated_values[0].get<Int64>(), 90);
    // Group 200: keys 2 → rows 1,3,5 → sum = 20+40+60 = 120
    EXPECT_EQ(result.aggregated_values[1].get<Int64>(), 120);
}

TEST_F(EncodedStarJoinTest, SingleDimensionEntry)
{
    auto fact_col = createIntDictCol({1, 2, 3}, {0, 1, 2, 0, 1, 2});
    PaddedPODArray<Int64> amounts = {5, 10, 15, 20, 25, 30};

    PaddedPODArray<Int64> dim_keys = {2};
    PaddedPODArray<Int64> dim_values = {42};
    auto dim_table = EncodedStarJoin::buildDimensionTable(dim_keys, dim_values);

    auto result = EncodedStarJoin::fusedJoinGroupBySum(*fact_col, amounts, dim_table);

    ASSERT_TRUE(result.used_encoded_path);
    EXPECT_EQ(result.num_groups, 1u);
    EXPECT_EQ(result.rows_joined, 2u);
    EXPECT_EQ(result.rows_not_joined, 4u);
    // Only key 2 matches: rows 1, 4 → sum = 10 + 25 = 35
    EXPECT_EQ(result.aggregated_values[0].get<Int64>(), 35);
}

TEST_F(EncodedStarJoinTest, IsApplicable)
{
    auto dict_col = createIntDictCol({1, 2, 3}, {0, 1, 2});
    EXPECT_TRUE(EncodedStarJoin::isApplicable(*dict_col, 100, true));
    EXPECT_FALSE(EncodedStarJoin::isApplicable(*dict_col, 100, false)); // no group-by

    // Non-dict column
    auto regular_col = ColumnVector<Int64>::create();
    regular_col->getData().push_back(1);
    EXPECT_FALSE(EncodedStarJoin::isApplicable(*regular_col, 100, true));

    // Dimension too large
    EXPECT_FALSE(EncodedStarJoin::isApplicable(*dict_col, 200000, true));
}

TEST_F(EncodedStarJoinTest, BuildDimensionTableWithStringGroups)
{
    PaddedPODArray<Int64> dim_keys = {1, 2, 3};
    std::vector<Field> dim_group_values = {Field(String("US")), Field(String("UK")), Field(String("DE"))};

    auto table = EncodedStarJoin::buildDimensionTableWithStringGroups(dim_keys, dim_group_values);

    EXPECT_EQ(table.num_groups, 3u);
    EXPECT_EQ(table.group_keys[0].get<String>(), "US");
    EXPECT_EQ(table.group_keys[1].get<String>(), "UK");
    EXPECT_EQ(table.group_keys[2].get<String>(), "DE");
}

TEST_F(EncodedStarJoinTest, LargeFactTable)
{
    // 3 distinct fact keys, 10000 rows
    std::vector<UInt32> ids;
    PaddedPODArray<Int64> amounts;
    for (int i = 0; i < 10000; ++i)
    {
        ids.push_back(i % 3);
        amounts.push_back(i);
    }
    auto fact_col = createIntDictCol({10, 20, 30}, ids);

    PaddedPODArray<Int64> dim_keys = {10, 20, 30};
    PaddedPODArray<Int64> dim_values = {1, 2, 3};
    auto dim_table = EncodedStarJoin::buildDimensionTable(dim_keys, dim_values);

    auto result = EncodedStarJoin::fusedJoinGroupBySum(*fact_col, amounts, dim_table);

    ASSERT_TRUE(result.used_encoded_path);
    EXPECT_EQ(result.rows_joined, 10000u);
    EXPECT_EQ(result.rows_not_joined, 0u);
    EXPECT_EQ(result.num_groups, 3u);
}

} // namespace DB::DM::tests
