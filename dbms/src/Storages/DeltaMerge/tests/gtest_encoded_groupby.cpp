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
#include <Storages/DeltaMerge/Encoded/EncodedGroupBy.h>
#include <gtest/gtest.h>

namespace DB::DM::tests
{

class EncodedGroupByTest : public ::testing::Test
{
protected:
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

TEST_F(EncodedGroupByTest, SumInt64Basic)
{
    // Groups: {0: "US", 1: "UK", 2: "DE"}
    // IDs:    [0, 1, 0, 2, 0, 1]
    // Values: [10, 20, 30, 40, 50, 60]
    // Expected: US=90, UK=80, DE=40
    auto group_col = createStringDictCol({"US", "UK", "DE"}, {0, 1, 0, 2, 0, 1});
    PaddedPODArray<Int64> values = {10, 20, 30, 40, 50, 60};

    auto result = EncodedGroupBy::sumInt64(*group_col, values);

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.num_groups, 3u);
    EXPECT_EQ(result.aggregated_values[0].get<Int64>(), 90); // US
    EXPECT_EQ(result.aggregated_values[1].get<Int64>(), 80); // UK
    EXPECT_EQ(result.aggregated_values[2].get<Int64>(), 40); // DE
    EXPECT_EQ(result.group_counts[0], 3u); // US has 3 rows
    EXPECT_EQ(result.group_counts[1], 2u); // UK has 2 rows
    EXPECT_EQ(result.group_counts[2], 1u); // DE has 1 row
}

TEST_F(EncodedGroupByTest, CountBasic)
{
    auto group_col = createStringDictCol({"active", "inactive", "pending"}, {0, 1, 0, 2, 0, 1, 2, 0});

    auto result = EncodedGroupBy::count(*group_col);

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.num_groups, 3u);
    EXPECT_EQ(result.aggregated_values[0].get<UInt64>(), 4u); // active
    EXPECT_EQ(result.aggregated_values[1].get<UInt64>(), 2u); // inactive
    EXPECT_EQ(result.aggregated_values[2].get<UInt64>(), 2u); // pending
}

TEST_F(EncodedGroupByTest, MinInt64)
{
    auto group_col = createStringDictCol({"A", "B"}, {0, 1, 0, 1, 0});
    PaddedPODArray<Int64> values = {5, 3, 1, 7, 9};

    auto result = EncodedGroupBy::minInt64(*group_col, values);

    ASSERT_TRUE(result.used_encoded_path);
    EXPECT_EQ(result.aggregated_values[0].get<Int64>(), 1); // min of A: min(5, 1, 9)
    EXPECT_EQ(result.aggregated_values[1].get<Int64>(), 3); // min of B: min(3, 7)
}

TEST_F(EncodedGroupByTest, MaxInt64)
{
    auto group_col = createStringDictCol({"A", "B"}, {0, 1, 0, 1, 0});
    PaddedPODArray<Int64> values = {5, 3, 1, 7, 9};

    auto result = EncodedGroupBy::maxInt64(*group_col, values);

    ASSERT_TRUE(result.used_encoded_path);
    EXPECT_EQ(result.aggregated_values[0].get<Int64>(), 9); // max of A: max(5, 1, 9)
    EXPECT_EQ(result.aggregated_values[1].get<Int64>(), 7); // max of B: max(3, 7)
}

TEST_F(EncodedGroupByTest, AnyInt64)
{
    auto group_col = createStringDictCol({"X", "Y"}, {0, 1, 0, 1});
    PaddedPODArray<Int64> values = {100, 200, 300, 400};

    auto result = EncodedGroupBy::anyInt64(*group_col, values);

    ASSERT_TRUE(result.used_encoded_path);
    EXPECT_EQ(result.aggregated_values[0].get<Int64>(), 100); // first value for X
    EXPECT_EQ(result.aggregated_values[1].get<Int64>(), 200); // first value for Y
}

TEST_F(EncodedGroupByTest, SumFloat64)
{
    auto group_col = createStringDictCol({"region1", "region2"}, {0, 1, 0, 1, 0});
    PaddedPODArray<Float64> values = {1.5, 2.5, 3.5, 4.5, 5.5};

    auto result = EncodedGroupBy::sumFloat64(*group_col, values);

    ASSERT_TRUE(result.used_encoded_path);
    EXPECT_DOUBLE_EQ(result.aggregated_values[0].get<Float64>(), 10.5); // 1.5 + 3.5 + 5.5
    EXPECT_DOUBLE_EQ(result.aggregated_values[1].get<Float64>(), 7.0);  // 2.5 + 4.5
}

TEST_F(EncodedGroupByTest, SingleGroup)
{
    auto group_col = createStringDictCol({"only_group"}, {0, 0, 0, 0, 0});
    PaddedPODArray<Int64> values = {10, 20, 30, 40, 50};

    auto result = EncodedGroupBy::sumInt64(*group_col, values);

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.num_groups, 1u);
    EXPECT_EQ(result.aggregated_values[0].get<Int64>(), 150);
    EXPECT_EQ(result.group_counts[0], 5u);
}

TEST_F(EncodedGroupByTest, ManyGroups)
{
    // Test with 100 distinct groups
    std::vector<String> dict_vals;
    for (int i = 0; i < 100; ++i)
        dict_vals.push_back("group_" + std::to_string(i));

    std::vector<UInt32> id_vals;
    PaddedPODArray<Int64> values;
    for (int i = 0; i < 1000; ++i)
    {
        id_vals.push_back(i % 100);
        values.push_back(i);
    }

    auto group_col = createStringDictCol(dict_vals, id_vals);
    auto result = EncodedGroupBy::sumInt64(*group_col, values);

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.num_groups, 100u);
    // Each group has 10 values: group_0 has {0, 100, 200, ..., 900} sum = 4500
    EXPECT_EQ(result.aggregated_values[0].get<Int64>(), 4500);
    EXPECT_EQ(result.group_counts[0], 10u);
}

TEST_F(EncodedGroupByTest, MultiAggregate)
{
    auto group_col = createStringDictCol({"A", "B", "C"}, {0, 1, 2, 0, 1, 2, 0});
    PaddedPODArray<Int64> values = {10, 20, 30, 40, 50, 60, 70};

    std::vector<std::pair<EncodedGroupBy::AggFunc, const PaddedPODArray<Int64> *>> specs = {
        {EncodedGroupBy::AggFunc::Sum, &values},
        {EncodedGroupBy::AggFunc::Count, nullptr},
        {EncodedGroupBy::AggFunc::Min, &values},
        {EncodedGroupBy::AggFunc::Max, &values},
    };

    auto result = EncodedGroupBy::multiAggregate(*group_col, specs);

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.num_groups, 3u);

    // SUM: A=120, B=70, C=90
    EXPECT_EQ(result.aggregated_values[0][0].get<Int64>(), 120);
    EXPECT_EQ(result.aggregated_values[0][1].get<Int64>(), 70);
    EXPECT_EQ(result.aggregated_values[0][2].get<Int64>(), 90);

    // COUNT: A=3, B=2, C=2
    EXPECT_EQ(result.aggregated_values[1][0].get<Int64>(), 3);
    EXPECT_EQ(result.aggregated_values[1][1].get<Int64>(), 2);
    EXPECT_EQ(result.aggregated_values[1][2].get<Int64>(), 2);

    // MIN: A=10, B=20, C=30
    EXPECT_EQ(result.aggregated_values[2][0].get<Int64>(), 10);
    EXPECT_EQ(result.aggregated_values[2][1].get<Int64>(), 20);
    EXPECT_EQ(result.aggregated_values[2][2].get<Int64>(), 30);

    // MAX: A=70, B=50, C=60
    EXPECT_EQ(result.aggregated_values[3][0].get<Int64>(), 70);
    EXPECT_EQ(result.aggregated_values[3][1].get<Int64>(), 50);
    EXPECT_EQ(result.aggregated_values[3][2].get<Int64>(), 60);
}

TEST_F(EncodedGroupByTest, NegativeValues)
{
    auto group_col = createIntDictCol({1, 2}, {0, 1, 0, 1});
    PaddedPODArray<Int64> values = {-10, -20, -30, -40};

    auto result = EncodedGroupBy::sumInt64(*group_col, values);

    ASSERT_TRUE(result.used_encoded_path);
    EXPECT_EQ(result.aggregated_values[0].get<Int64>(), -40); // -10 + -30
    EXPECT_EQ(result.aggregated_values[1].get<Int64>(), -60); // -20 + -40
}

TEST_F(EncodedGroupByTest, IsSuitableForEncodedGroupBy)
{
    auto col = createStringDictCol({"A", "B", "C"}, {0, 1, 2});
    EXPECT_TRUE(EncodedGroupBy::isSuitableForEncodedGroupBy(*col));

    // Regular column should not be suitable
    auto regular_col = ColumnVector<Int64>::create();
    regular_col->getData().push_back(1);
    EXPECT_FALSE(EncodedGroupBy::isSuitableForEncodedGroupBy(*regular_col));
}

} // namespace DB::DM::tests
