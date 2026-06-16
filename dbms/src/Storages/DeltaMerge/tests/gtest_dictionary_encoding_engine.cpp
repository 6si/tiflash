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
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/DeltaMerge/Encoded/DictionaryEncodingEngine.h>
#include <gtest/gtest.h>

namespace DB::DM::tests
{

class DictionaryEncodingEngineTest : public ::testing::Test
{
};

TEST_F(DictionaryEncodingEngineTest, ComputeCardinalityLow)
{
    auto col = ColumnVector<Int64>::create();
    // 5 distinct values repeated
    for (int i = 0; i < 100; ++i)
        col->insert(Int64(i % 5));

    size_t cardinality = DictionaryEncodingEngine::computeCardinality(*col);
    EXPECT_EQ(cardinality, 5u);
}

TEST_F(DictionaryEncodingEngineTest, ComputeCardinalityHigh)
{
    auto col = ColumnVector<Int64>::create();
    for (int i = 0; i < 5000; ++i)
        col->insert(Int64(i));

    size_t cardinality = DictionaryEncodingEngine::computeCardinality(*col, 4097);
    EXPECT_GT(cardinality, 4096u);
}

TEST_F(DictionaryEncodingEngineTest, IsSuitableEnabled)
{
    auto col = ColumnVector<Int64>::create();
    for (int i = 0; i < 100; ++i)
        col->insert(Int64(i % 10));

    DictionaryEncodingConfig config;
    config.enabled = true;
    config.max_cardinality = 4096;
    config.min_rows_for_encoding = 64;

    EXPECT_TRUE(DictionaryEncodingEngine::isSuitableForDictionary(*col, config));
}

TEST_F(DictionaryEncodingEngineTest, IsSuitableDisabled)
{
    auto col = ColumnVector<Int64>::create();
    for (int i = 0; i < 100; ++i)
        col->insert(Int64(i % 10));

    DictionaryEncodingConfig config;
    config.enabled = false;

    EXPECT_FALSE(DictionaryEncodingEngine::isSuitableForDictionary(*col, config));
}

TEST_F(DictionaryEncodingEngineTest, IsSuitableTooFewRows)
{
    auto col = ColumnVector<Int64>::create();
    for (int i = 0; i < 10; ++i)
        col->insert(Int64(i % 3));

    DictionaryEncodingConfig config;
    config.enabled = true;
    config.min_rows_for_encoding = 64;

    EXPECT_FALSE(DictionaryEncodingEngine::isSuitableForDictionary(*col, config));
}

TEST_F(DictionaryEncodingEngineTest, IsSuitableHighCardinality)
{
    auto col = ColumnVector<Int64>::create();
    for (int i = 0; i < 5000; ++i)
        col->insert(Int64(i)); // 5000 distinct values

    DictionaryEncodingConfig config;
    config.enabled = true;
    config.max_cardinality = 4096;
    config.min_rows_for_encoding = 64;

    EXPECT_FALSE(DictionaryEncodingEngine::isSuitableForDictionary(*col, config));
}

TEST_F(DictionaryEncodingEngineTest, TryEncodeLowCardinality)
{
    auto col = ColumnVector<Int64>::create();
    for (int i = 0; i < 200; ++i)
        col->insert(Int64(i % 5)); // 5 distinct values

    DictionaryEncodingConfig config;
    config.enabled = true;
    config.max_cardinality = 4096;
    config.min_rows_for_encoding = 64;

    auto result = DictionaryEncodingEngine::tryEncode(*col, std::make_shared<DataTypeInt64>(), config);

    ASSERT_TRUE(result.was_encoded);
    EXPECT_EQ(result.cardinality, 5u);
    auto * dict_col = dynamic_cast<const ColumnDictionary *>(result.column.get());
    ASSERT_NE(dict_col, nullptr);
    EXPECT_EQ(dict_col->size(), 200u);
    EXPECT_EQ(dict_col->getDictionarySize(), 5u);
}

TEST_F(DictionaryEncodingEngineTest, TryEncodeHighCardinality)
{
    auto col = ColumnVector<Int64>::create();
    for (int i = 0; i < 5000; ++i)
        col->insert(Int64(i)); // all unique

    DictionaryEncodingConfig config;
    config.enabled = true;
    config.max_cardinality = 4096;
    config.min_rows_for_encoding = 64;

    auto result = DictionaryEncodingEngine::tryEncode(*col, std::make_shared<DataTypeInt64>(), config);

    ASSERT_FALSE(result.was_encoded);
}

TEST_F(DictionaryEncodingEngineTest, TryEncodeDisabled)
{
    auto col = ColumnVector<Int64>::create();
    for (int i = 0; i < 200; ++i)
        col->insert(Int64(i % 5));

    DictionaryEncodingConfig config;
    config.enabled = false;

    auto result = DictionaryEncodingEngine::tryEncode(*col, std::make_shared<DataTypeInt64>(), config);

    ASSERT_FALSE(result.was_encoded);
}

TEST_F(DictionaryEncodingEngineTest, EncodeStringColumn)
{
    auto col = ColumnString::create();
    col->insert(Field(String("apple")));
    col->insert(Field(String("banana")));
    col->insert(Field(String("apple")));
    col->insert(Field(String("cherry")));
    col->insert(Field(String("banana")));

    auto result = DictionaryEncodingEngine::encodeStringColumn(*col, std::make_shared<DataTypeString>());

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 5u);
    EXPECT_EQ(result->getDictionarySize(), 3u); // apple, banana, cherry

    // Verify decode produces correct values
    auto decoded = result->decode();
    for (size_t i = 0; i < 5; ++i)
    {
        Field original, dec;
        col->get(i, original);
        decoded->get(i, dec);
        EXPECT_EQ(original, dec);
    }
}

TEST_F(DictionaryEncodingEngineTest, EncodeIntegerColumn)
{
    auto col = ColumnVector<Int64>::create();
    col->insert(Int64(100));
    col->insert(Int64(200));
    col->insert(Int64(100));
    col->insert(Int64(300));
    col->insert(Int64(200));
    col->insert(Int64(100));

    auto result = DictionaryEncodingEngine::encodeIntegerColumn<Int64>(*col, std::make_shared<DataTypeInt64>());

    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 6u);
    EXPECT_EQ(result->getDictionarySize(), 3u); // 100, 200, 300

    // Verify roundtrip
    auto decoded = result->decode();
    for (size_t i = 0; i < 6; ++i)
    {
        Field original, dec;
        col->get(i, original);
        decoded->get(i, dec);
        EXPECT_EQ(original, dec);
    }
}

TEST_F(DictionaryEncodingEngineTest, EncodePreservesOrder)
{
    auto col = ColumnVector<Int32>::create();
    std::vector<Int32> values = {3, 1, 4, 1, 5, 9, 2, 6, 5, 3};
    for (auto v : values)
        col->insert(Int32(v));

    auto result = DictionaryEncodingEngine::encodeIntegerColumn<Int32>(*col, std::make_shared<DataTypeInt32>());

    // Verify order preserved
    auto decoded = result->decode();
    for (size_t i = 0; i < values.size(); ++i)
    {
        Field f;
        decoded->get(i, f);
        EXPECT_EQ(f.get<Int64>(), values[i]);
    }
}

TEST_F(DictionaryEncodingEngineTest, EncodeExactlyMaxCardinality)
{
    auto col = ColumnVector<Int64>::create();
    // Exactly 4096 distinct values repeated twice each
    for (int i = 0; i < 4096; ++i)
    {
        col->insert(Int64(i));
        col->insert(Int64(i));
    }

    DictionaryEncodingConfig config;
    config.enabled = true;
    config.max_cardinality = 4096;
    config.min_rows_for_encoding = 64;

    auto result = DictionaryEncodingEngine::tryEncode(*col, std::make_shared<DataTypeInt64>(), config);

    ASSERT_TRUE(result.was_encoded);
    EXPECT_EQ(result.cardinality, 4096u);
}

TEST_F(DictionaryEncodingEngineTest, EncodeOneOverMaxCardinality)
{
    auto col = ColumnVector<Int64>::create();
    // 4097 distinct values — should NOT encode
    for (int i = 0; i < 4097; ++i)
        col->insert(Int64(i));

    DictionaryEncodingConfig config;
    config.enabled = true;
    config.max_cardinality = 4096;
    config.min_rows_for_encoding = 64;

    auto result = DictionaryEncodingEngine::tryEncode(*col, std::make_shared<DataTypeInt64>(), config);

    ASSERT_FALSE(result.was_encoded);
}

} // namespace DB::DM::tests
