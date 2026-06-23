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
#include <gtest/gtest.h>

namespace DB::tests
{

class ColumnDictionaryTest : public ::testing::Test
{
protected:
    static ColumnDictionary::MutablePtr createTestColumn()
    {
        std::vector<Field> dict = {Field(String("apple")), Field(String("banana")), Field(String("cherry"))};
        PaddedPODArray<UInt32> ids;
        // 10 rows: apple, banana, cherry, apple, apple, banana, cherry, cherry, apple, banana
        ids.push_back(0);
        ids.push_back(1);
        ids.push_back(2);
        ids.push_back(0);
        ids.push_back(0);
        ids.push_back(1);
        ids.push_back(2);
        ids.push_back(2);
        ids.push_back(0);
        ids.push_back(1);
        return ColumnDictionary::createMutable(std::move(dict), std::move(ids), std::make_shared<DataTypeString>());
    }
};

TEST_F(ColumnDictionaryTest, BasicProperties)
{
    auto col = createTestColumn();
    ASSERT_EQ(col->size(), 10);
    ASSERT_EQ(col->getDictionarySize(), 3);
    ASSERT_TRUE(col->isDictionaryEncoded());
    ASSERT_STREQ(col->getFamilyName(), "String");
}

TEST_F(ColumnDictionaryTest, GetDataAt)
{
    auto col = createTestColumn();
    ASSERT_EQ(col->getDataAt(0), StringRef("apple", 5));
    ASSERT_EQ(col->getDataAt(1), StringRef("banana", 6));
    ASSERT_EQ(col->getDataAt(2), StringRef("cherry", 6));
    ASSERT_EQ(col->getDataAt(3), StringRef("apple", 5));
}

TEST_F(ColumnDictionaryTest, FieldAccess)
{
    auto col = createTestColumn();
    ASSERT_EQ((*col)[0].get<String>(), "apple");
    ASSERT_EQ((*col)[1].get<String>(), "banana");
    ASSERT_EQ((*col)[9].get<String>(), "banana");
}

TEST_F(ColumnDictionaryTest, Decode)
{
    auto col = createTestColumn();
    auto decoded = col->decode();
    ASSERT_EQ(decoded->size(), 10);
    ASSERT_EQ(decoded->getDataAt(0), StringRef("apple", 5));
    ASSERT_EQ(decoded->getDataAt(1), StringRef("banana", 6));
    ASSERT_EQ(decoded->getDataAt(9), StringRef("banana", 6));
}

TEST_F(ColumnDictionaryTest, ConvertToFullColumn)
{
    auto col = createTestColumn();
    auto full = col->convertToFullColumnIfDictionary();
    ASSERT_TRUE(full != nullptr);
    ASSERT_EQ(full->size(), 10);
    ASSERT_EQ(full->getDataAt(0), StringRef("apple", 5));
}

TEST_F(ColumnDictionaryTest, Filter)
{
    auto col = createTestColumn();
    IColumn::Filter filter = {1, 0, 1, 0, 1, 0, 1, 0, 1, 0}; // keep indices 0,2,4,6,8
    auto filtered = col->filter(filter, -1);
    ASSERT_EQ(filtered->size(), 5);
    ASSERT_EQ(filtered->getDataAt(0), StringRef("apple", 5));
    ASSERT_EQ(filtered->getDataAt(1), StringRef("cherry", 6));
    ASSERT_EQ(filtered->getDataAt(2), StringRef("apple", 5));
    ASSERT_EQ(filtered->getDataAt(3), StringRef("cherry", 6));
    ASSERT_EQ(filtered->getDataAt(4), StringRef("apple", 5));
}

TEST_F(ColumnDictionaryTest, Permute)
{
    auto col = createTestColumn();
    IColumn::Permutation perm = {9, 8, 7, 6, 5, 4, 3, 2, 1, 0}; // reverse
    auto permuted = col->permute(perm, 0);
    ASSERT_EQ(permuted->size(), 10);
    ASSERT_EQ(permuted->getDataAt(0), StringRef("banana", 6)); // was index 9
    ASSERT_EQ(permuted->getDataAt(9), StringRef("apple", 5)); // was index 0
}

TEST_F(ColumnDictionaryTest, CompareAt)
{
    auto col = createTestColumn();
    // apple < banana
    ASSERT_LT(col->compareAt(0, 1, *col, 0), 0);
    // banana > apple
    ASSERT_GT(col->compareAt(1, 0, *col, 0), 0);
    // apple == apple
    ASSERT_EQ(col->compareAt(0, 3, *col, 0), 0);
}

TEST_F(ColumnDictionaryTest, InsertFrom)
{
    auto col = createTestColumn();
    auto col2 = createTestColumn();
    col->insertFrom(*col2, 2); // insert "cherry"
    ASSERT_EQ(col->size(), 11);
    ASSERT_EQ(col->getDataAt(10), StringRef("cherry", 6));
}

TEST_F(ColumnDictionaryTest, InsertRangeFrom)
{
    auto col = createTestColumn();
    auto col2 = createTestColumn();
    col->insertRangeFrom(*col2, 0, 3); // insert apple, banana, cherry
    ASSERT_EQ(col->size(), 13);
    ASSERT_EQ(col->getDataAt(10), StringRef("apple", 5));
    ASSERT_EQ(col->getDataAt(11), StringRef("banana", 6));
    ASSERT_EQ(col->getDataAt(12), StringRef("cherry", 6));
}

TEST_F(ColumnDictionaryTest, CloneResized)
{
    auto col = createTestColumn();
    auto cloned = col->cloneResized(5);
    ASSERT_EQ(cloned->size(), 5);
    ASSERT_EQ(cloned->getDataAt(0), StringRef("apple", 5));
    ASSERT_EQ(cloned->getDataAt(4), StringRef("apple", 5));
}

TEST_F(ColumnDictionaryTest, InsertFromColumnString)
{
    auto col = createTestColumn();

    // Insert from a regular ColumnString
    auto str_col = ColumnString::create();
    str_col->insertData("dragon", 6);
    col->insertFrom(*str_col, 0);
    ASSERT_EQ(col->size(), 11);
    ASSERT_EQ(col->getDataAt(10), StringRef("dragon", 6));
    ASSERT_EQ(col->getDictionarySize(), 4); // new entry added
}

TEST_F(ColumnDictionaryTest, Cut)
{
    auto col = createTestColumn();
    auto cut = col->cut(2, 5); // cherry, apple, apple, banana, cherry
    ASSERT_EQ(cut->size(), 5);
    ASSERT_EQ(cut->getDataAt(0), StringRef("cherry", 6));
    ASSERT_EQ(cut->getDataAt(1), StringRef("apple", 5));
    ASSERT_EQ(cut->getDataAt(2), StringRef("apple", 5));
    ASSERT_EQ(cut->getDataAt(3), StringRef("banana", 6));
    ASSERT_EQ(cut->getDataAt(4), StringRef("cherry", 6));
}

TEST_F(ColumnDictionaryTest, PopBack)
{
    auto col = createTestColumn();
    col->popBack(3);
    ASSERT_EQ(col->size(), 7);
}

} // namespace DB::tests
