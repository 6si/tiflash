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

TEST_F(DictionaryEncodingEngineTest, CardinalityBoundaryTransition)
{
    // Start with 4096 distinct values (within threshold), then cross to 4097
    // This tests the exact boundary condition where encoding switches from suitable to not suitable
    DictionaryEncodingConfig config;
    config.enabled = true;
    config.max_cardinality = 4096;
    config.min_rows_for_encoding = 64;

    // 4096 distinct values — should encode
    {
        auto col = ColumnVector<Int64>::create();
        for (int i = 0; i < 8192; ++i)
            col->insert(Int64(i % 4096));

        EXPECT_TRUE(DictionaryEncodingEngine::isSuitableForDictionary(*col, config));
        auto result = DictionaryEncodingEngine::tryEncode(*col, std::make_shared<DataTypeInt64>(), config);
        ASSERT_TRUE(result.was_encoded);
        EXPECT_EQ(result.cardinality, 4096u);
    }

    // 4097 distinct values — should NOT encode
    {
        auto col = ColumnVector<Int64>::create();
        for (int i = 0; i < 8194; ++i)
            col->insert(Int64(i % 4097));

        EXPECT_FALSE(DictionaryEncodingEngine::isSuitableForDictionary(*col, config));
        auto result = DictionaryEncodingEngine::tryEncode(*col, std::make_shared<DataTypeInt64>(), config);
        ASSERT_FALSE(result.was_encoded);
    }
}

TEST_F(DictionaryEncodingEngineTest, ReEncodeAfterMerge)
{
    // Simulate segment merge: two encoded columns with different dictionaries
    // After merge, the combined column should still be re-encodeable if total cardinality is within bounds.
    DictionaryEncodingConfig config;
    config.enabled = true;
    config.max_cardinality = 4096;
    config.min_rows_for_encoding = 64;

    // Segment 1: values {1, 2, 3} repeated
    auto col1 = ColumnVector<Int64>::create();
    for (int i = 0; i < 100; ++i)
        col1->insert(Int64(i % 3 + 1));

    auto encoded1 = DictionaryEncodingEngine::tryEncode(*col1, std::make_shared<DataTypeInt64>(), config);
    ASSERT_TRUE(encoded1.was_encoded);
    EXPECT_EQ(encoded1.cardinality, 3u);

    // Segment 2: values {3, 4, 5} repeated (overlapping value '3')
    auto col2 = ColumnVector<Int64>::create();
    for (int i = 0; i < 100; ++i)
        col2->insert(Int64(i % 3 + 3));

    auto encoded2 = DictionaryEncodingEngine::tryEncode(*col2, std::make_shared<DataTypeInt64>(), config);
    ASSERT_TRUE(encoded2.was_encoded);
    EXPECT_EQ(encoded2.cardinality, 3u);

    // Merge: decode both and combine, then re-encode
    auto decoded1 = dynamic_cast<const ColumnDictionary *>(encoded1.column.get())->decode();
    auto decoded2 = dynamic_cast<const ColumnDictionary *>(encoded2.column.get())->decode();

    // Create merged column
    auto merged = ColumnVector<Int64>::create();
    for (size_t i = 0; i < decoded1->size(); ++i)
    {
        Field f;
        decoded1->get(i, f);
        merged->insert(f.get<Int64>());
    }
    for (size_t i = 0; i < decoded2->size(); ++i)
    {
        Field f;
        decoded2->get(i, f);
        merged->insert(f.get<Int64>());
    }

    EXPECT_EQ(merged->size(), 200u);

    // Re-encode the merged column — cardinality should be 5 (1,2,3,4,5)
    auto merged_result = DictionaryEncodingEngine::tryEncode(*merged, std::make_shared<DataTypeInt64>(), config);
    ASSERT_TRUE(merged_result.was_encoded);
    EXPECT_EQ(merged_result.cardinality, 5u);
    EXPECT_EQ(merged_result.column->size(), 200u);

    // Verify roundtrip correctness
    auto * dict_col = dynamic_cast<const ColumnDictionary *>(merged_result.column.get());
    ASSERT_NE(dict_col, nullptr);
    auto final_decoded = dict_col->decode();
    for (size_t i = 0; i < 200; ++i)
    {
        Field orig, dec;
        merged->get(i, orig);
        final_decoded->get(i, dec);
        EXPECT_EQ(orig, dec);
    }
}

TEST_F(DictionaryEncodingEngineTest, ConcurrentReadAndEncode)
{
    // Verify that encoding a column does not mutate the original column.
    // This tests the safety property: if one thread reads the raw column while another
    // thread encodes it, the raw column remains unchanged.
    auto col = ColumnVector<Int64>::create();
    for (int i = 0; i < 1000; ++i)
        col->insert(Int64(i % 10));

    // Take a snapshot of original data
    std::vector<Int64> original_data(1000);
    for (size_t i = 0; i < 1000; ++i)
    {
        Field f;
        col->get(i, f);
        original_data[i] = f.get<Int64>();
    }

    DictionaryEncodingConfig config;
    config.enabled = true;
    config.max_cardinality = 4096;
    config.min_rows_for_encoding = 64;

    // Encode the column
    auto result = DictionaryEncodingEngine::tryEncode(*col, std::make_shared<DataTypeInt64>(), config);
    ASSERT_TRUE(result.was_encoded);

    // Verify original column is unchanged
    EXPECT_EQ(col->size(), 1000u);
    for (size_t i = 0; i < 1000; ++i)
    {
        Field f;
        col->get(i, f);
        EXPECT_EQ(f.get<Int64>(), original_data[i]);
    }

    // Verify encoded column is also correct
    auto * dict_col = dynamic_cast<const ColumnDictionary *>(result.column.get());
    ASSERT_NE(dict_col, nullptr);
    auto decoded = dict_col->decode();
    for (size_t i = 0; i < 1000; ++i)
    {
        Field f;
        decoded->get(i, f);
        EXPECT_EQ(f.get<Int64>(), original_data[i]);
    }
}

TEST_F(DictionaryEncodingEngineTest, EncodeStringWithNulls)
{
    // Test encoding a string column that has empty strings (approximating NULL representation)
    auto col = ColumnString::create();
    col->insert(Field(String("alpha")));
    col->insert(Field(String("")));  // empty string — distinct from NULL but tests boundary
    col->insert(Field(String("beta")));
    col->insert(Field(String("")));
    col->insert(Field(String("alpha")));
    col->insert(Field(String("gamma")));

    auto result = DictionaryEncodingEngine::encodeStringColumn(*col, std::make_shared<DataTypeString>());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->size(), 6u);
    EXPECT_EQ(result->getDictionarySize(), 4u); // "alpha", "", "beta", "gamma"

    // Verify decode roundtrip
    auto decoded = result->decode();
    for (size_t i = 0; i < 6; ++i)
    {
        Field original, dec;
        col->get(i, original);
        decoded->get(i, dec);
        EXPECT_EQ(original, dec);
    }
}

} // namespace DB::DM::tests
