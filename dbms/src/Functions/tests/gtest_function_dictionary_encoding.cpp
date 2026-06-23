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
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::tests
{

class FunctionDictionaryEncodingTest : public DB::tests::FunctionTest
{
protected:
    /// Create a ColumnDictionary with 3 distinct values, 512 rows
    static ColumnPtr createDictionaryColumn()
    {
        std::vector<Field> dict = {Field(String("active")), Field(String("inactive")), Field(String("pending"))};
        PaddedPODArray<UInt32> ids;
        ids.reserve(512);
        for (size_t i = 0; i < 512; ++i)
            ids.push_back(static_cast<UInt32>(i % 3));
        return ColumnDictionary::createMutable(std::move(dict), std::move(ids), std::make_shared<DataTypeString>());
    }

    /// Create data vector with low cardinality strings
    static std::vector<String> createLowCardinalityData(size_t n = 512)
    {
        std::vector<String> data;
        data.reserve(n);
        for (size_t i = 0; i < n; ++i)
        {
            switch (i % 3)
            {
            case 0:
                data.push_back("active");
                break;
            case 1:
                data.push_back("inactive");
                break;
            case 2:
                data.push_back("pending");
                break;
            }
        }
        return data;
    }
};

TEST_F(FunctionDictionaryEncodingTest, ComparisonWithLowCardinality)
{
    // Test: ColumnString with low cardinality + const comparison
    // The IFunction auto-encoding path should activate and produce correct results
    auto data = createLowCardinalityData();
    auto input_col = createColumn<String>(data);
    auto const_col = createConstColumn<String>(512, "active");

    auto result = executeFunction("equals", input_col, const_col);
    auto result_col = result.column;
    ASSERT_EQ(result_col->size(), 512);
    for (size_t i = 0; i < 512; ++i)
    {
        UInt64 expected = (i % 3 == 0) ? 1 : 0;
        ASSERT_EQ(result_col->getUInt(i), expected) << "Mismatch at row " << i;
    }
}

TEST_F(FunctionDictionaryEncodingTest, NotEqualsWithLowCardinality)
{
    auto data = createLowCardinalityData();
    auto input_col = createColumn<String>(data);
    auto const_col = createConstColumn<String>(512, "active");

    auto result = executeFunction("notEquals", input_col, const_col);
    auto result_col = result.column;
    ASSERT_EQ(result_col->size(), 512);
    for (size_t i = 0; i < 512; ++i)
    {
        UInt64 expected = (i % 3 != 0) ? 1 : 0;
        ASSERT_EQ(result_col->getUInt(i), expected) << "Mismatch at row " << i;
    }
}

TEST_F(FunctionDictionaryEncodingTest, LessThanWithLowCardinality)
{
    auto data = createLowCardinalityData();
    auto input_col = createColumn<String>(data);
    auto const_col = createConstColumn<String>(512, "inactive");

    // "active" < "inactive" = true, "inactive" < "inactive" = false, "pending" < "inactive" = false
    auto result = executeFunction("less", input_col, const_col);
    auto result_col = result.column;
    ASSERT_EQ(result_col->size(), 512);
    for (size_t i = 0; i < 512; ++i)
    {
        UInt64 expected = (i % 3 == 0) ? 1 : 0;
        ASSERT_EQ(result_col->getUInt(i), expected) << "Mismatch at row " << i;
    }
}

TEST_F(FunctionDictionaryEncodingTest, EqualsWithPendingTarget)
{
    auto data = createLowCardinalityData();
    auto input_col = createColumn<String>(data);
    auto const_col = createConstColumn<String>(512, "pending");

    auto result = executeFunction("equals", input_col, const_col);
    auto result_col = result.column;
    ASSERT_EQ(result_col->size(), 512);
    for (size_t i = 0; i < 512; ++i)
    {
        UInt64 expected = (i % 3 == 2) ? 1 : 0;
        ASSERT_EQ(result_col->getUInt(i), expected) << "Mismatch at row " << i;
    }
}

TEST_F(FunctionDictionaryEncodingTest, HighCardinalityFallsBack)
{
    // High cardinality (all unique): should NOT auto-encode but still correct
    std::vector<String> data;
    data.reserve(512);
    for (size_t i = 0; i < 512; ++i)
        data.push_back(fmt::format("unique_{}", i));

    auto input_col = createColumn<String>(data);
    auto const_col = createConstColumn<String>(512, "unique_100");

    auto result = executeFunction("equals", input_col, const_col);
    auto result_col = result.column;
    ASSERT_EQ(result_col->size(), 512);
    for (size_t i = 0; i < 512; ++i)
    {
        UInt64 expected = (i == 100) ? 1 : 0;
        ASSERT_EQ(result_col->getUInt(i), expected) << "Mismatch at row " << i;
    }
}

TEST_F(FunctionDictionaryEncodingTest, ConvertToFullColumnIfDictionary)
{
    auto dict_col = createDictionaryColumn();
    auto full = dict_col->convertToFullColumnIfDictionary();
    ASSERT_TRUE(full != nullptr);
    ASSERT_EQ(full->size(), 512);

    for (size_t i = 0; i < 512; ++i)
    {
        ASSERT_EQ(full->getDataAt(i), dict_col->getDataAt(i));
    }
}

} // namespace DB::tests
