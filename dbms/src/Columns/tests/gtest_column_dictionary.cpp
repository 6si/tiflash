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
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <gtest/gtest.h>

namespace DB::tests
{

class ColumnDictionaryTest : public ::testing::Test
{
};

TEST_F(ColumnDictionaryTest, BasicConstruction)
{
    std::vector<Field> dict = {Field(Int64(10)), Field(Int64(20)), Field(Int64(30))};
    PaddedPODArray<UInt32> ids = {0, 1, 2, 0, 1, 2, 0};
    auto type = std::make_shared<DataTypeInt64>();

    auto col = ColumnDictionary::createMutable(std::move(dict), std::move(ids), type);
    ASSERT_EQ(col->size(), 7u);
    ASSERT_EQ(col->getDictionarySize(), 3u);
}

TEST_F(ColumnDictionaryTest, ElementAccess)
{
    std::vector<Field> dict = {Field(String("apple")), Field(String("banana")), Field(String("cherry"))};
    PaddedPODArray<UInt32> ids = {0, 1, 2, 1, 0};
    auto type = std::make_shared<DataTypeString>();

    auto col = ColumnDictionary::createMutable(std::move(dict), std::move(ids), type);

    Field f;
    col->get(0, f);
    ASSERT_EQ(f.get<String>(), "apple");
    col->get(1, f);
    ASSERT_EQ(f.get<String>(), "banana");
    col->get(2, f);
    ASSERT_EQ(f.get<String>(), "cherry");
    col->get(3, f);
    ASSERT_EQ(f.get<String>(), "banana");
    col->get(4, f);
    ASSERT_EQ(f.get<String>(), "apple");
}

TEST_F(ColumnDictionaryTest, Filter)
{
    std::vector<Field> dict = {Field(Int64(100)), Field(Int64(200)), Field(Int64(300))};
    PaddedPODArray<UInt32> ids = {0, 1, 2, 0, 1};
    auto type = std::make_shared<DataTypeInt64>();

    auto col = ColumnDictionary::createMutable(std::move(dict), std::move(ids), type);

    // Filter: keep indices 0, 2, 4
    IColumn::Filter filter = {1, 0, 1, 0, 1};
    auto filtered = col->filter(filter, -1);

    ASSERT_EQ(filtered->size(), 3u);
    Field f;
    filtered->get(0, f);
    ASSERT_EQ(f.get<Int64>(), 100);
    filtered->get(1, f);
    ASSERT_EQ(f.get<Int64>(), 300);
    filtered->get(2, f);
    ASSERT_EQ(f.get<Int64>(), 200);
}

TEST_F(ColumnDictionaryTest, Decode)
{
    std::vector<Field> dict = {Field(Int64(10)), Field(Int64(20)), Field(Int64(30))};
    PaddedPODArray<UInt32> ids = {0, 1, 2, 1, 0};
    auto type = std::make_shared<DataTypeInt64>();

    auto col = ColumnDictionary::createMutable(std::move(dict), std::move(ids), type);
    auto decoded = col->decode();

    ASSERT_EQ(decoded->size(), 5u);
    Field f;
    decoded->get(0, f);
    ASSERT_EQ(f.get<Int64>(), 10);
    decoded->get(1, f);
    ASSERT_EQ(f.get<Int64>(), 20);
    decoded->get(2, f);
    ASSERT_EQ(f.get<Int64>(), 30);
    decoded->get(3, f);
    ASSERT_EQ(f.get<Int64>(), 20);
    decoded->get(4, f);
    ASSERT_EQ(f.get<Int64>(), 10);
}

TEST_F(ColumnDictionaryTest, Permute)
{
    std::vector<Field> dict = {Field(String("a")), Field(String("b")), Field(String("c"))};
    PaddedPODArray<UInt32> ids = {0, 1, 2, 0, 1};
    auto type = std::make_shared<DataTypeString>();

    auto col = ColumnDictionary::createMutable(std::move(dict), std::move(ids), type);

    // Reverse permutation
    IColumn::Permutation perm = {4, 3, 2, 1, 0};
    auto permuted = col->permute(perm, 0);

    ASSERT_EQ(permuted->size(), 5u);
    Field f;
    permuted->get(0, f);
    ASSERT_EQ(f.get<String>(), "b"); // was at idx 4 (id=1)
    permuted->get(4, f);
    ASSERT_EQ(f.get<String>(), "a"); // was at idx 0 (id=0)
}

TEST_F(ColumnDictionaryTest, CloneResized)
{
    std::vector<Field> dict = {Field(Int64(1)), Field(Int64(2))};
    PaddedPODArray<UInt32> ids = {0, 1, 0, 1, 0};
    auto type = std::make_shared<DataTypeInt64>();

    auto col = ColumnDictionary::createMutable(std::move(dict), std::move(ids), type);
    auto resized = col->cloneResized(3);

    ASSERT_EQ(resized->size(), 3u);
    Field f;
    resized->get(0, f);
    ASSERT_EQ(f.get<Int64>(), 1);
    resized->get(1, f);
    ASSERT_EQ(f.get<Int64>(), 2);
    resized->get(2, f);
    ASSERT_EQ(f.get<Int64>(), 1);
}

TEST_F(ColumnDictionaryTest, IsDictionaryEncoded)
{
    std::vector<Field> dict = {Field(Int64(1))};
    PaddedPODArray<UInt32> ids = {0};
    auto type = std::make_shared<DataTypeInt64>();

    auto col = ColumnDictionary::createMutable(std::move(dict), std::move(ids), type);
    ASSERT_TRUE(col->isDictionaryEncoded());
}

TEST_F(ColumnDictionaryTest, GetDictionaryIds)
{
    std::vector<Field> dict = {Field(Int64(10)), Field(Int64(20)), Field(Int64(30))};
    PaddedPODArray<UInt32> ids = {0, 2, 1, 2, 0};
    auto type = std::make_shared<DataTypeInt64>();

    auto col = ColumnDictionary::createMutable(std::move(dict), std::move(ids), type);
    const auto & retrieved_ids = col->getDictionaryIds();

    ASSERT_EQ(retrieved_ids.size(), 5u);
    ASSERT_EQ(retrieved_ids[0], 0u);
    ASSERT_EQ(retrieved_ids[1], 2u);
    ASSERT_EQ(retrieved_ids[2], 1u);
    ASSERT_EQ(retrieved_ids[3], 2u);
    ASSERT_EQ(retrieved_ids[4], 0u);
}

} // namespace DB::tests
