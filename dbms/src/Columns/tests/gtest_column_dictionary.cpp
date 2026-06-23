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
#include <Columns/ColumnVector.h>
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

TEST_F(ColumnDictionaryTest, TryAutoEncodeBasic)
{
    // Build a ColumnString with low cardinality (3 distinct values, 300 rows)
    auto str_col = ColumnString::create();
    const std::vector<String> values = {"active", "inactive", "pending"};
    for (size_t i = 0; i < 300; ++i)
        str_col->insert(Field(values[i % 3]));

    ColumnPtr input = std::move(str_col);
    auto result = ColumnDictionary::tryAutoEncode(input, 256, 65536);

    // Should be encoded since 300 >= 256 and 3 <= 65536
    ASSERT_TRUE(result->isDictionaryEncoded());
    ASSERT_EQ(result->size(), 300);

    const auto * dict_col = typeid_cast<const ColumnDictionary *>(result.get());
    ASSERT_NE(dict_col, nullptr);
    ASSERT_EQ(dict_col->getDictionarySize(), 3);

    // Verify values are preserved
    for (size_t i = 0; i < 300; ++i)
    {
        StringRef ref = result->getDataAt(i);
        ASSERT_EQ(ref.toString(), values[i % 3]);
    }
}

TEST_F(ColumnDictionaryTest, TryAutoEncodeTooFewRows)
{
    auto str_col = ColumnString::create();
    for (size_t i = 0; i < 100; ++i)
        str_col->insert(Field(String("val")));

    ColumnPtr input = std::move(str_col);
    auto result = ColumnDictionary::tryAutoEncode(input, 256, 65536);

    // Should NOT be encoded (100 < 256 min_rows)
    ASSERT_FALSE(result->isDictionaryEncoded());
    ASSERT_EQ(result.get(), input.get()); // same pointer
}

TEST_F(ColumnDictionaryTest, TryAutoEncodeHighCardinality)
{
    auto str_col = ColumnString::create();
    // 500 rows with 500 distinct values — exceeds max_dict_size=100
    for (size_t i = 0; i < 500; ++i)
        str_col->insert(Field(String("val_" + std::to_string(i))));

    ColumnPtr input = std::move(str_col);
    auto result = ColumnDictionary::tryAutoEncode(input, 256, 100);

    // Should NOT be encoded (500 distinct > 100 max)
    ASSERT_FALSE(result->isDictionaryEncoded());
    ASSERT_EQ(result.get(), input.get());
}

TEST_F(ColumnDictionaryTest, TryAutoEncodeAlreadyEncoded)
{
    auto col = createTestColumn();
    ColumnPtr input = std::move(col);
    auto result = ColumnDictionary::tryAutoEncode(input, 1, 65536);

    // Already encoded — should return same pointer
    ASSERT_TRUE(result->isDictionaryEncoded());
    ASSERT_EQ(result.get(), input.get());
}

TEST_F(ColumnDictionaryTest, TryAutoEncodeNullable)
{
    auto str_col = ColumnString::create();
    auto null_map = ColumnUInt8::create();
    for (size_t i = 0; i < 300; ++i)
    {
        str_col->insert(Field(String(i % 5 == 0 ? "null_val" : "regular")));
        null_map->insert(Field(static_cast<UInt64>(i % 5 == 0 ? 1 : 0)));
    }
    auto nullable = ColumnNullable::create(std::move(str_col), std::move(null_map));
    ColumnPtr input = std::move(nullable);
    auto result = ColumnDictionary::tryAutoEncode(input, 256, 65536);

    // Nested should be encoded, wrapped in Nullable
    const auto * result_nullable = typeid_cast<const ColumnNullable *>(result.get());
    ASSERT_NE(result_nullable, nullptr);
    ASSERT_TRUE(result_nullable->getNestedColumnPtr()->isDictionaryEncoded());
    ASSERT_EQ(result->size(), 300);
}

TEST_F(ColumnDictionaryTest, MaterializeBeforeInsertRangeFrom)
{
    // Simulates the DeltaMerge interleave scenario:
    // output is ColumnString (from header.cloneEmpty()), source is ColumnDictionary.
    // ColumnString::insertRangeFrom does static_cast<ColumnString&> which crashes
    // on ColumnDictionary. The fix: materialize via convertToFullColumnIfDictionary().
    auto dict_col = createTestColumn();
    ASSERT_TRUE(dict_col->isDictionaryEncoded());

    // Materialize and verify insertRangeFrom works
    auto materialized = dict_col->convertToFullColumnIfDictionary();
    ASSERT_FALSE(materialized->isDictionaryEncoded());

    auto output = ColumnString::create();
    output->insertRangeFrom(*materialized, 0, materialized->size());
    ASSERT_EQ(output->size(), dict_col->size());

    // Verify values match
    for (size_t i = 0; i < output->size(); ++i)
    {
        Field dict_val, out_val;
        dict_col->get(i, dict_val);
        output->get(i, out_val);
        ASSERT_EQ(dict_val, out_val);
    }
}

TEST_F(ColumnDictionaryTest, FilterPreservesDictionary)
{
    // Verifies that filter() on ColumnDictionary returns ColumnDictionary
    // (used by MVCC filter, RowKey filter in the pipeline)
    auto col = createTestColumn();
    size_t n = col->size();

    IColumn::Filter filt(n, 0);
    size_t passed = 0;
    for (size_t i = 0; i < n; ++i)
    {
        if (i % 2 == 0)
        {
            filt[i] = 1;
            ++passed;
        }
    }

    auto filtered = col->filter(filt, passed);
    ASSERT_TRUE(filtered->isDictionaryEncoded());
    ASSERT_EQ(filtered->size(), passed);

    // Verify filtered values are correct (every other row)
    for (size_t i = 0; i < passed; ++i)
    {
        Field orig_val, filt_val;
        col->get(i * 2, orig_val);
        filtered->get(i, filt_val);
        ASSERT_EQ(orig_val, filt_val);
    }
}

TEST_F(ColumnDictionaryTest, ConvertToFullColumnIfDictionaryNonDict)
{
    // Non-dictionary column should return itself
    auto str_col = ColumnString::create();
    str_col->insert(Field(String("hello")));
    ColumnPtr ptr = std::move(str_col);
    auto result = ptr->convertToFullColumnIfDictionary();
    ASSERT_EQ(result.get(), ptr.get());
}

} // namespace DB::tests
