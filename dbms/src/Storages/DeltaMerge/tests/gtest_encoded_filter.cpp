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
#include <Storages/DeltaMerge/Encoded/EncodedFilter.h>
#include <gtest/gtest.h>

namespace DB::DM::tests
{

class EncodedFilterTest : public ::testing::Test
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

TEST_F(EncodedFilterTest, EqualsStringMatch)
{
    // Dictionary: {0: "red", 1: "green", 2: "blue"}
    // IDs: [0, 1, 2, 1, 0, 2, 1]
    auto col = createStringDictCol({"red", "green", "blue"}, {0, 1, 2, 1, 0, 2, 1});

    auto result = EncodedFilter::evaluateEquals(*col, Field(String("green")));

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.filter.size(), 7u);
    ASSERT_EQ(result.count_passing, 3u);
    // Positions 1, 3, 6 should pass
    EXPECT_EQ(result.filter[0], 0);
    EXPECT_EQ(result.filter[1], 1);
    EXPECT_EQ(result.filter[2], 0);
    EXPECT_EQ(result.filter[3], 1);
    EXPECT_EQ(result.filter[4], 0);
    EXPECT_EQ(result.filter[5], 0);
    EXPECT_EQ(result.filter[6], 1);
}

TEST_F(EncodedFilterTest, EqualsNoMatch)
{
    auto col = createStringDictCol({"red", "green", "blue"}, {0, 1, 2, 0});

    auto result = EncodedFilter::evaluateEquals(*col, Field(String("yellow")));

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.count_passing, 0u);
    for (size_t i = 0; i < result.filter.size(); ++i)
        EXPECT_EQ(result.filter[i], 0);
}

TEST_F(EncodedFilterTest, EqualsAllMatch)
{
    auto col = createStringDictCol({"only"}, {0, 0, 0, 0, 0});

    auto result = EncodedFilter::evaluateEquals(*col, Field(String("only")));

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.count_passing, 5u);
    for (size_t i = 0; i < result.filter.size(); ++i)
        EXPECT_EQ(result.filter[i], 1);
}

TEST_F(EncodedFilterTest, NotEquals)
{
    auto col = createStringDictCol({"red", "green", "blue"}, {0, 1, 2, 1, 0});

    auto result = EncodedFilter::evaluateNotEquals(*col, Field(String("red")));

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.count_passing, 3u);
    EXPECT_EQ(result.filter[0], 0); // red
    EXPECT_EQ(result.filter[1], 1); // green
    EXPECT_EQ(result.filter[2], 1); // blue
    EXPECT_EQ(result.filter[3], 1); // green
    EXPECT_EQ(result.filter[4], 0); // red
}

TEST_F(EncodedFilterTest, InPredicate)
{
    auto col = createStringDictCol({"US", "UK", "DE", "FR"}, {0, 1, 2, 3, 0, 1, 2});

    std::vector<Field> values = {Field(String("US")), Field(String("DE"))};
    auto result = EncodedFilter::evaluateIn(*col, values);

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.count_passing, 4u);
    EXPECT_EQ(result.filter[0], 1); // US
    EXPECT_EQ(result.filter[1], 0); // UK
    EXPECT_EQ(result.filter[2], 1); // DE
    EXPECT_EQ(result.filter[3], 0); // FR
    EXPECT_EQ(result.filter[4], 1); // US
    EXPECT_EQ(result.filter[5], 0); // UK
    EXPECT_EQ(result.filter[6], 1); // DE
}

TEST_F(EncodedFilterTest, InPredicateEmpty)
{
    auto col = createStringDictCol({"US", "UK"}, {0, 1, 0, 1});

    std::vector<Field> values = {};
    auto result = EncodedFilter::evaluateIn(*col, values);

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.count_passing, 0u);
}

TEST_F(EncodedFilterTest, LikePredicate)
{
    auto col = createStringDictCol({"apple", "application", "banana", "apply"}, {0, 1, 2, 3, 0, 1});

    auto result = EncodedFilter::evaluateLike(*col, "app%");

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.count_passing, 5u); // apple, application, apply (not banana)
    EXPECT_EQ(result.filter[0], 1); // apple
    EXPECT_EQ(result.filter[1], 1); // application
    EXPECT_EQ(result.filter[2], 0); // banana
    EXPECT_EQ(result.filter[3], 1); // apply
    EXPECT_EQ(result.filter[4], 1); // apple
    EXPECT_EQ(result.filter[5], 1); // application
}

TEST_F(EncodedFilterTest, LikeWithUnderscore)
{
    auto col = createStringDictCol({"cat", "car", "cab", "cap"}, {0, 1, 2, 3});

    auto result = EncodedFilter::evaluateLike(*col, "ca_");

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.count_passing, 4u); // all match "ca" + single char
}

TEST_F(EncodedFilterTest, IntegerEquals)
{
    auto col = createIntDictCol({100, 200, 300}, {0, 1, 2, 0, 1, 2, 0});

    auto result = EncodedFilter::evaluateEquals(*col, Field(Int64(200)));

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.count_passing, 2u);
    EXPECT_EQ(result.filter[1], 1);
    EXPECT_EQ(result.filter[4], 1);
}

TEST_F(EncodedFilterTest, GeneralPredicate)
{
    auto col = createIntDictCol({10, 20, 30, 40, 50}, {0, 1, 2, 3, 4, 0, 1});

    // Filter: value > 25
    auto result = EncodedFilter::evaluatePredicate(*col, [](const Field & f) {
        return f.get<Int64>() > 25;
    });

    ASSERT_TRUE(result.used_encoded_path);
    ASSERT_EQ(result.count_passing, 3u);
    EXPECT_EQ(result.filter[0], 0); // 10
    EXPECT_EQ(result.filter[1], 0); // 20
    EXPECT_EQ(result.filter[2], 1); // 30
    EXPECT_EQ(result.filter[3], 1); // 40
    EXPECT_EQ(result.filter[4], 1); // 50
}

TEST_F(EncodedFilterTest, TryApplyOnNonDictColumn)
{
    // Regular (non-dictionary) column — should return nullopt
    auto regular_col = ColumnVector<Int64>::create();
    regular_col->getData().push_back(1);
    regular_col->getData().push_back(2);
    regular_col->getData().push_back(3);

    auto result = EncodedFilter::tryApplyEquals(*regular_col, Field(Int64(2)));
    ASSERT_FALSE(result.has_value());
}

TEST_F(EncodedFilterTest, TryApplyOnDictColumn)
{
    auto col = createIntDictCol({1, 2, 3}, {0, 1, 2});

    auto result = EncodedFilter::tryApplyEquals(*col, Field(Int64(2)));
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->count_passing, 1u);
    EXPECT_EQ(result->filter[1], 1);
}

TEST_F(EncodedFilterTest, LargeColumnPerformance)
{
    // Simulate a column with 3 distinct values and 10000 rows
    std::vector<UInt32> id_values;
    for (size_t i = 0; i < 10000; ++i)
        id_values.push_back(i % 3);
    auto col = createStringDictCol({"active", "inactive", "pending"}, id_values);

    auto result = EncodedFilter::evaluateEquals(*col, Field(String("active")));

    ASSERT_TRUE(result.used_encoded_path);
    // ~3333 or 3334 rows should match
    ASSERT_GE(result.count_passing, 3333u);
    ASSERT_LE(result.count_passing, 3334u);
}

} // namespace DB::DM::tests
