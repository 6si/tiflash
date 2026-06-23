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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Columns/ColumnDictionary.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/OperatorSpillContext.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Debug/TiFlashTestEnv.h>
#include <IO/Encryption/MockKeyManager.h>
#include <IO/FileProvider/FileProvider.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::tests
{

class AggregatorDictionaryVisitCacheTest : public ::testing::Test
{
protected:
    static void SetUpTestCase()
    {
        try
        {
            DB::registerAggregateFunctions();
        }
        catch (DB::Exception &)
        {
            // Already registered
        }
    }

    void SetUp() override
    {
        context = TiFlashTestEnv::getContext();
        auto key_manager = std::make_shared<MockKeyManager>(false);
        auto file_provider = std::make_shared<FileProvider>(key_manager, false);
        spill_dir = TiFlashTestEnv::getTemporaryPath("agg_dict_visit_cache_test");
        spill_config = std::make_shared<SpillConfig>(spill_dir, "test", 1024ULL * 1024 * 1024, 0, 0, file_provider);
    }

    void TearDown() override
    {
        Poco::File spiller_dir(spill_dir);
        if (spiller_dir.exists())
            spiller_dir.remove(true);
    }

    /// Build a block with: col0 = ColumnDictionary key, col1 = UInt64 values
    static Block buildDictBlock(size_t num_rows, size_t num_distinct)
    {
        std::vector<Field> dict;
        dict.reserve(num_distinct);
        for (size_t i = 0; i < num_distinct; ++i)
            dict.push_back(Field(String("key_") + std::to_string(i)));

        PaddedPODArray<UInt32> ids;
        ids.reserve(num_rows);
        for (size_t i = 0; i < num_rows; ++i)
            ids.push_back(static_cast<UInt32>(i % num_distinct));

        auto dict_col = ColumnDictionary::createMutable(std::move(dict), std::move(ids), std::make_shared<DataTypeString>());

        auto val_col = ColumnUInt64::create();
        for (size_t i = 0; i < num_rows; ++i)
            val_col->insert(Field(static_cast<UInt64>(1)));

        Block block;
        block.insert(ColumnWithTypeAndName(std::move(dict_col), std::make_shared<DataTypeString>(), "key"));
        block.insert(ColumnWithTypeAndName(std::move(val_col), std::make_shared<DataTypeUInt64>(), "val"));
        return block;
    }

    /// Build an equivalent block using ColumnString (no dictionary)
    static Block buildStringBlock(size_t num_rows, size_t num_distinct)
    {
        auto str_col = ColumnString::create();
        for (size_t i = 0; i < num_rows; ++i)
            str_col->insert(Field(String("key_") + std::to_string(i % num_distinct)));

        auto val_col = ColumnUInt64::create();
        for (size_t i = 0; i < num_rows; ++i)
            val_col->insert(Field(static_cast<UInt64>(1)));

        Block block;
        block.insert(ColumnWithTypeAndName(std::move(str_col), std::make_shared<DataTypeString>(), "key"));
        block.insert(ColumnWithTypeAndName(std::move(val_col), std::make_shared<DataTypeUInt64>(), "val"));
        return block;
    }

    /// Create Aggregator for: SELECT key, COUNT(val) FROM ... GROUP BY key
    std::unique_ptr<Aggregator> createCountAggregator(const Block & header)
    {
        auto data_type_uint64 = std::make_shared<DataTypeUInt64>();
        AggregateDescriptions agg_descs{
            {.function = AggregateFunctionFactory::instance().get(*context, "count", {data_type_uint64}, {}, 0, false),
             .parameters = {},
             .arguments = {1},
             .argument_names = {"val"},
             .column_name = "count(val)"},
        };

        ColumnNumbers keys = {0};
        KeyRefAggFuncMap key_ref_agg_func;
        AggFuncRefKeyMap agg_func_ref_key;

        Aggregator::Params params(
            header,
            keys,
            key_ref_agg_func,
            agg_func_ref_key,
            agg_descs,
            0,
            0,
            0,
            false,
            *spill_config,
            8192,
            false);

        RegisterOperatorSpillContext no_spill = [](const OperatorSpillContextPtr &) {};
        return std::make_unique<Aggregator>(params, "test", /*concurrency=*/1, no_spill, false, false);
    }

    /// Run aggregation on a block, return {key -> count} map
    std::map<String, UInt64> runAggregation(const Block & block)
    {
        auto aggregator = createCountAggregator(block);
        auto result = std::make_shared<AggregatedDataVariants>();
        Aggregator::AggProcessInfo info(aggregator.get());
        info.resetBlock(block);
        aggregator->executeOnBlock(info, *result, 0);

        ManyAggregatedDataVariants variants;
        variants.push_back(result);
        auto merged = aggregator->mergeAndConvertToBlocks(variants, true, 1);

        std::map<String, UInt64> counts;
        if (!merged)
            return counts;

        for (size_t ci = 0; ci < merged->getConcurrency(); ++ci)
        {
            auto output_block = merged->getData(ci);
            if (!output_block)
                continue;
            size_t rows = output_block.rows();
            if (rows == 0)
                continue;
            const auto & key_col = output_block.getByPosition(0).column;
            const auto & cnt_col = output_block.getByPosition(1).column;
            for (size_t i = 0; i < rows; ++i)
            {
                String key = key_col->getDataAt(i).toString();
                UInt64 count = cnt_col->getUInt(i);
                counts[key] += count;
            }
        }
        return counts;
    }

    std::shared_ptr<Context> context;
    std::shared_ptr<SpillConfig> spill_config;
    String spill_dir;
};

/// Verify visit-cache produces correct GROUP BY results with ColumnDictionary key
TEST_F(AggregatorDictionaryVisitCacheTest, CorrectCountWithDictionaryKey)
try
{
    const size_t num_rows = 1000;
    const size_t num_distinct = 5;

    auto dict_block = buildDictBlock(num_rows, num_distinct);
    auto string_block = buildStringBlock(num_rows, num_distinct);

    auto dict_counts = runAggregation(dict_block);
    auto string_counts = runAggregation(string_block);

    ASSERT_EQ(dict_counts.size(), num_distinct);
    ASSERT_EQ(string_counts.size(), num_distinct);

    for (size_t i = 0; i < num_distinct; ++i)
    {
        String key = "key_" + std::to_string(i);
        ASSERT_EQ(dict_counts[key], num_rows / num_distinct) << "Mismatch for key: " << key;
        ASSERT_EQ(dict_counts[key], string_counts[key]) << "Dict vs String mismatch for key: " << key;
    }
}
CATCH

/// Verify visit-cache activates (dict_ids is populated in AggProcessInfo)
TEST_F(AggregatorDictionaryVisitCacheTest, VisitCacheActivation)
try
{
    auto dict_block = buildDictBlock(512, 3);
    auto aggregator = createCountAggregator(dict_block);
    Aggregator::AggProcessInfo info(aggregator.get());
    info.resetBlock(dict_block);
    info.prepareForAgg();

    ASSERT_NE(info.dict_ids, nullptr) << "dict_ids should be set for ColumnDictionary key";
    ASSERT_EQ(info.dict_size, 3u);
    ASSERT_EQ(info.dict_entries_refs.size(), 3u);
    ASSERT_EQ(info.dict_entries_refs[0].toString(), "key_0");
    ASSERT_EQ(info.dict_entries_refs[1].toString(), "key_1");
    ASSERT_EQ(info.dict_entries_refs[2].toString(), "key_2");
}
CATCH

/// Verify visit-cache does NOT activate for ColumnString (standard path)
TEST_F(AggregatorDictionaryVisitCacheTest, NoVisitCacheForColumnString)
try
{
    auto string_block = buildStringBlock(512, 3);
    auto aggregator = createCountAggregator(string_block);
    Aggregator::AggProcessInfo info(aggregator.get());
    info.resetBlock(string_block);
    info.prepareForAgg();

    ASSERT_EQ(info.dict_ids, nullptr) << "dict_ids should be null for ColumnString key";
    ASSERT_EQ(info.dict_size, 0u);
}
CATCH

/// Verify large dictionary (> 65536) falls back to standard path
TEST_F(AggregatorDictionaryVisitCacheTest, LargeDictionaryFallsBack)
try
{
    auto dict_block = buildDictBlock(100000, 70000);
    auto aggregator = createCountAggregator(dict_block);
    Aggregator::AggProcessInfo info(aggregator.get());
    info.resetBlock(dict_block);
    info.prepareForAgg();

    ASSERT_EQ(info.dict_ids, nullptr) << "dict_ids should be null for large dictionary (> 65536)";
}
CATCH

/// Verify correctness with multiple blocks (cross-block dictionary remapping)
TEST_F(AggregatorDictionaryVisitCacheTest, MultiBlockCorrectness)
try
{
    // Block 1: keys key_0, key_1, key_2 with 300 rows each
    auto block1 = buildDictBlock(900, 3);
    // Block 2: keys key_0, ..., key_4 with 200 rows each
    auto block2 = buildDictBlock(1000, 5);

    auto aggregator = createCountAggregator(block1);
    auto result = std::make_shared<AggregatedDataVariants>();

    {
        Aggregator::AggProcessInfo info(aggregator.get());
        info.resetBlock(block1);
        aggregator->executeOnBlock(info, *result, 0);
    }
    {
        Aggregator::AggProcessInfo info(aggregator.get());
        info.resetBlock(block2);
        aggregator->executeOnBlock(info, *result, 0);
    }

    ManyAggregatedDataVariants variants2;
    variants2.push_back(result);
    auto merged = aggregator->mergeAndConvertToBlocks(variants2, true, 1);

    std::map<String, UInt64> counts;
    if (merged)
    {
        for (size_t ci = 0; ci < merged->getConcurrency(); ++ci)
        {
            auto output_block = merged->getData(ci);
            if (!output_block)
                continue;
            size_t rows = output_block.rows();
            if (rows == 0)
                continue;
            const auto & key_col = output_block.getByPosition(0).column;
            const auto & cnt_col = output_block.getByPosition(1).column;
            for (size_t i = 0; i < rows; ++i)
            {
                String key = key_col->getDataAt(i).toString();
                UInt64 count = cnt_col->getUInt(i);
                counts[key] += count;
            }
        }
    }

    // key_0: 300 (block1) + 200 (block2) = 500
    // key_1: 300 + 200 = 500
    // key_2: 300 + 200 = 500
    // key_3: 0 + 200 = 200
    // key_4: 0 + 200 = 200
    ASSERT_EQ(counts.size(), 5u);
    ASSERT_EQ(counts["key_0"], 500u);
    ASSERT_EQ(counts["key_1"], 500u);
    ASSERT_EQ(counts["key_2"], 500u);
    ASSERT_EQ(counts["key_3"], 200u);
    ASSERT_EQ(counts["key_4"], 200u);
}
CATCH

} // namespace DB::tests
