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
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <Core/SpillConfig.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Debug/TiFlashTestEnv.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::tests
{

class AggregatorDictKeyTest : public ::testing::Test
{
protected:
    void SetUp() override
    {
        static std::once_flag flag;
        std::call_once(flag, [] { registerAggregateFunctions(); });
    }

    Block makeStringKeyBlock(
        const std::vector<String> & keys,
        const std::vector<Int64> & values)
    {
        auto key_col = ColumnString::create();
        for (const auto & k : keys)
            key_col->insertData(k.data(), k.size());

        auto val_col = ColumnVector<Int64>::create();
        for (auto v : values)
            val_col->getData().push_back(v);

        Block block;
        block.insert({std::move(key_col), std::make_shared<DataTypeString>(), "key"});
        block.insert({std::move(val_col), std::make_shared<DataTypeInt64>(), "val"});
        return block;
    }

    Block makeDictKeyBlock(
        const std::vector<String> & dict_values,
        const std::vector<UInt32> & id_vec,
        const std::vector<Int64> & values)
    {
        std::vector<Field> dict;
        for (const auto & v : dict_values)
            dict.emplace_back(v);
        PaddedPODArray<UInt32> id_array;
        for (auto id : id_vec)
            id_array.push_back(id);
        auto dict_col
            = ColumnDictionary::createMutable(std::move(dict), std::move(id_array), std::make_shared<DataTypeString>());

        auto val_col = ColumnVector<Int64>::create();
        for (auto v : values)
            val_col->getData().push_back(v);

        Block block;
        block.insert({std::move(dict_col), std::make_shared<DataTypeString>(), "key"});
        block.insert({std::move(val_col), std::make_shared<DataTypeInt64>(), "val"});
        return block;
    }

    std::unique_ptr<Aggregator> makeSumAggregator(const Block & header)
    {
        auto context = TiFlashTestEnv::getContext();
        ColumnNumbers keys = {0};
        AggregateDescriptions agg_descs;
        AggregateDescription desc;
        desc.column_name = "sum_val";
        desc.arguments = {1};
        DataTypes arg_types = {std::make_shared<DataTypeInt64>()};
        desc.function = AggregateFunctionFactory::instance().get(*context, "sum", arg_types);
        desc.parameters = Array();
        agg_descs.push_back(desc);

        Aggregator::Params params(
            header,
            keys,
            /*key_ref_agg_func=*/{},
            /*agg_func_ref_key=*/{},
            agg_descs,
            /*group_by_two_level_threshold=*/0,
            /*group_by_two_level_threshold_bytes=*/0,
            /*max_bytes_before_external_group_by=*/0,
            /*empty_result_for_aggregation_by_empty_set=*/false,
            SpillConfig("/tmp/tiflash_test_spill", "test", 0, 0, 0, nullptr),
            /*max_block_size=*/65536,
            /*use_magic_hash=*/false);

        return std::make_unique<Aggregator>(
            params,
            "test",
            /*concurrency=*/1,
            /*register_operator_spill_context=*/nullptr,
            /*is_auto_pass_through=*/false,
            /*use_magic_hash=*/false);
    }

    std::unique_ptr<Aggregator> makeCountAggregator(const Block & header)
    {
        auto context = TiFlashTestEnv::getContext();
        ColumnNumbers keys = {0};
        AggregateDescriptions agg_descs;
        AggregateDescription desc;
        desc.column_name = "count";
        desc.arguments = {};
        desc.function = AggregateFunctionFactory::instance().get(*context, "count", {});
        desc.parameters = Array();
        agg_descs.push_back(desc);

        Aggregator::Params params(
            header,
            keys,
            /*key_ref_agg_func=*/{},
            /*agg_func_ref_key=*/{},
            agg_descs,
            /*group_by_two_level_threshold=*/0,
            /*group_by_two_level_threshold_bytes=*/0,
            /*max_bytes_before_external_group_by=*/0,
            /*empty_result_for_aggregation_by_empty_set=*/false,
            SpillConfig("/tmp/tiflash_test_spill", "test", 0, 0, 0, nullptr),
            /*max_block_size=*/65536,
            /*use_magic_hash=*/false);

        return std::make_unique<Aggregator>(
            params,
            "test",
            /*concurrency=*/1,
            /*register_operator_spill_context=*/nullptr,
            /*is_auto_pass_through=*/false,
            /*use_magic_hash=*/false);
    }

    /// Run single-threaded aggregation and return result blocks
    BlocksList runAggregation(Aggregator & aggregator, const std::vector<Block> & blocks)
    {
        auto data = std::make_shared<AggregatedDataVariants>();

        for (auto block : blocks)
        {
            Aggregator::AggProcessInfo info(&aggregator);
            info.resetBlock(block);
            aggregator.executeOnBlock(info, *data, 0);
        }

        ManyAggregatedDataVariants many_data;
        many_data.push_back(std::move(data));
        auto merging = aggregator.mergeAndConvertToBlocks(many_data, /*final=*/true, /*max_threads=*/1);

        BlocksList result;
        if (merging)
        {
            while (true)
            {
                Block out = merging->getData(0);
                if (!out)
                    break;
                if (out.rows() > 0)
                    result.push_back(out);
            }
        }
        return result;
    }

    std::map<String, Int64> collectSumResults(const BlocksList & blocks)
    {
        std::map<String, Int64> results;
        for (const auto & block : blocks)
        {
            const auto & key_col = block.getByPosition(0).column;
            const auto & val_col = block.getByName("sum_val").column;
            for (size_t i = 0; i < block.rows(); ++i)
            {
                String key = key_col->getDataAt(i).toString();
                Int64 val = val_col->getInt(i);
                results[key] += val;
            }
        }
        return results;
    }

    std::map<String, UInt64> collectCountResults(const BlocksList & blocks)
    {
        std::map<String, UInt64> results;
        for (const auto & block : blocks)
        {
            const auto & key_col = block.getByPosition(0).column;
            const auto & val_col = block.getByName("count").column;
            for (size_t i = 0; i < block.rows(); ++i)
            {
                String key = key_col->getDataAt(i).toString();
                UInt64 val = val_col->getUInt(i);
                results[key] += val;
            }
        }
        return results;
    }
};

/// Verify that low-cardinality string key activates dict-key fast path
TEST_F(AggregatorDictKeyTest, StringKeyLowCardinality_ActivatesDictKeyPath)
try
{
    auto block = makeStringKeyBlock(
        {"US", "UK", "DE", "FR", "JP", "US", "UK", "DE", "FR", "JP", "US", "UK"},
        {10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120});

    auto aggregator = makeSumAggregator(block.cloneEmpty());

    auto data = std::make_shared<AggregatedDataVariants>();
    Aggregator::AggProcessInfo info(aggregator.get());
    info.resetBlock(block);
    aggregator->executeOnBlock(info, *data, 0);

    EXPECT_TRUE(aggregator->dict_key_state.isActive());
    EXPECT_EQ(aggregator->dict_key_state.id_to_value.size(), 5u);
}
CATCH

/// SUM correctness with string keys
TEST_F(AggregatorDictKeyTest, StringKey_SumCorrectness)
{
    auto block = makeStringKeyBlock(
        {"US", "UK", "DE", "US", "UK", "DE"},
        {10, 20, 30, 40, 50, 60});

    auto aggregator = makeSumAggregator(block.cloneEmpty());
    auto results_blocks = runAggregation(*aggregator, {block});
    auto results = collectSumResults(results_blocks);

    EXPECT_EQ(results["US"], 50); // 10 + 40
    EXPECT_EQ(results["UK"], 70); // 20 + 50
    EXPECT_EQ(results["DE"], 90); // 30 + 60
    EXPECT_EQ(results.size(), 3u);
}

/// COUNT correctness with string keys
TEST_F(AggregatorDictKeyTest, StringKey_CountCorrectness)
{
    auto block = makeStringKeyBlock(
        {"active", "inactive", "active", "pending", "active", "inactive"},
        {0, 0, 0, 0, 0, 0});

    auto aggregator = makeCountAggregator(block.cloneEmpty());
    auto results_blocks = runAggregation(*aggregator, {block});
    auto results = collectCountResults(results_blocks);

    EXPECT_EQ(results["active"], 3u);
    EXPECT_EQ(results["inactive"], 2u);
    EXPECT_EQ(results["pending"], 1u);
    EXPECT_EQ(results.size(), 3u);
}

/// SUM correctness with ColumnDictionary input
TEST_F(AggregatorDictKeyTest, DictColumnKey_SumCorrectness)
{
    auto block = makeDictKeyBlock(
        {"US", "UK", "DE"},
        {0, 1, 2, 0, 1, 2, 0},
        {10, 20, 30, 40, 50, 60, 70});

    auto aggregator = makeSumAggregator(block.cloneEmpty());
    auto results_blocks = runAggregation(*aggregator, {block});
    auto results = collectSumResults(results_blocks);

    EXPECT_EQ(results["US"], 120); // 10 + 40 + 70
    EXPECT_EQ(results["UK"], 70); // 20 + 50
    EXPECT_EQ(results["DE"], 90); // 30 + 60
    EXPECT_EQ(results.size(), 3u);
}

/// Single group value
TEST_F(AggregatorDictKeyTest, SingleGroup)
{
    auto block = makeStringKeyBlock(
        {"only", "only", "only", "only", "only"},
        {10, 20, 30, 40, 50});

    auto aggregator = makeSumAggregator(block.cloneEmpty());
    auto results_blocks = runAggregation(*aggregator, {block});
    auto results = collectSumResults(results_blocks);

    EXPECT_EQ(results["only"], 150);
    EXPECT_EQ(results.size(), 1u);
}

/// 100 distinct keys — still below threshold (4096)
TEST_F(AggregatorDictKeyTest, ManyGroups_StillActivates)
{
    std::vector<String> keys;
    std::vector<Int64> values;
    for (int i = 0; i < 1000; ++i)
    {
        keys.push_back("group_" + std::to_string(i % 100));
        values.push_back(i);
    }
    auto block = makeStringKeyBlock(keys, values);

    auto aggregator = makeSumAggregator(block.cloneEmpty());

    auto data = std::make_shared<AggregatedDataVariants>();
    Aggregator::AggProcessInfo info(aggregator.get());
    info.resetBlock(block);
    aggregator->executeOnBlock(info, *data, 0);

    EXPECT_TRUE(aggregator->dict_key_state.isActive());
    EXPECT_EQ(aggregator->dict_key_state.id_to_value.size(), 100u);
}

/// 5000 distinct keys — above threshold (4096), should NOT activate
TEST_F(AggregatorDictKeyTest, HighCardinality_DoesNotActivate)
{
    std::vector<String> keys;
    std::vector<Int64> values;
    for (int i = 0; i < 5000; ++i)
    {
        keys.push_back("key_" + std::to_string(i));
        values.push_back(i);
    }
    auto block = makeStringKeyBlock(keys, values);

    auto aggregator = makeSumAggregator(block.cloneEmpty());

    auto data = std::make_shared<AggregatedDataVariants>();
    Aggregator::AggProcessInfo info(aggregator.get());
    info.resetBlock(block);
    aggregator->executeOnBlock(info, *data, 0);

    EXPECT_FALSE(aggregator->dict_key_state.isActive());
}

/// Multiple blocks with the same key space — results merge correctly
TEST_F(AggregatorDictKeyTest, MultiBlock_ConsistentResults)
{
    auto block1 = makeStringKeyBlock({"US", "UK", "US", "UK"}, {10, 20, 30, 40});
    auto block2 = makeStringKeyBlock({"US", "UK", "DE"}, {50, 60, 70});

    auto aggregator = makeSumAggregator(block1.cloneEmpty());
    auto results_blocks = runAggregation(*aggregator, {block1, block2});
    auto results = collectSumResults(results_blocks);

    EXPECT_EQ(results["US"], 90); // 10 + 30 + 50
    EXPECT_EQ(results["UK"], 120); // 20 + 40 + 60
    EXPECT_EQ(results["DE"], 70);
    EXPECT_EQ(results.size(), 3u);
}

/// New keys arriving in later blocks are handled correctly
TEST_F(AggregatorDictKeyTest, MultiBlock_NewKeysInLaterBlock)
{
    auto block1 = makeStringKeyBlock({"A", "B"}, {10, 20});
    auto block2 = makeStringKeyBlock({"C", "D"}, {30, 40});
    auto block3 = makeStringKeyBlock({"A", "C", "E"}, {50, 60, 70});

    auto aggregator = makeSumAggregator(block1.cloneEmpty());
    auto results_blocks = runAggregation(*aggregator, {block1, block2, block3});
    auto results = collectSumResults(results_blocks);

    EXPECT_EQ(results["A"], 60); // 10 + 50
    EXPECT_EQ(results["B"], 20);
    EXPECT_EQ(results["C"], 90); // 30 + 60
    EXPECT_EQ(results["D"], 40);
    EXPECT_EQ(results["E"], 70);
    EXPECT_EQ(results.size(), 5u);
}

/// Direct test of DictKeyState::decodeKeyColumn
TEST_F(AggregatorDictKeyTest, DictKeyState_DecodeKeyColumn)
{
    Aggregator::DictKeyState dks;
    dks.active = true;
    dks.id_to_value = {"apple", "banana", "cherry"};

    auto uint16_col = ColumnUInt16::create();
    uint16_col->getData().push_back(2); // cherry
    uint16_col->getData().push_back(0); // apple
    uint16_col->getData().push_back(1); // banana
    uint16_col->getData().push_back(0); // apple

    auto decoded = dks.decodeKeyColumn(*uint16_col);
    ASSERT_EQ(decoded->size(), 4u);
    EXPECT_EQ(decoded->getDataAt(0).toString(), "cherry");
    EXPECT_EQ(decoded->getDataAt(1).toString(), "apple");
    EXPECT_EQ(decoded->getDataAt(2).toString(), "banana");
    EXPECT_EQ(decoded->getDataAt(3).toString(), "apple");
}

/// Direct test of DictKeyState::getOrInsert
TEST_F(AggregatorDictKeyTest, DictKeyState_GetOrInsert)
{
    Aggregator::DictKeyState dks;
    dks.id_to_value.reserve(100);

    UInt16 id_a = dks.getOrInsert(StringRef("alpha"));
    UInt16 id_b = dks.getOrInsert(StringRef("beta"));
    UInt16 id_a2 = dks.getOrInsert(StringRef("alpha")); // duplicate

    EXPECT_EQ(id_a, 0u);
    EXPECT_EQ(id_b, 1u);
    EXPECT_EQ(id_a2, 0u);
    EXPECT_EQ(dks.id_to_value.size(), 2u);
    EXPECT_FALSE(dks.failed);
}

/// Test that dict-key path activates with binary collation (utf8mb4_bin)
TEST_F(AggregatorDictKeyTest, BinaryCollation_StillActivates)
try
{
    auto block = makeStringKeyBlock(
        {"US", "UK", "DE", "FR", "JP", "US", "UK", "DE", "FR", "JP"},
        {10, 20, 30, 40, 50, 60, 70, 80, 90, 100});

    auto context = TiFlashTestEnv::getContext();
    ColumnNumbers keys = {0};
    AggregateDescriptions agg_descs;
    AggregateDescription desc;
    desc.column_name = "sum_val";
    desc.arguments = {1};
    DataTypes arg_types = {std::make_shared<DataTypeInt64>()};
    desc.function = AggregateFunctionFactory::instance().get(*context, "sum", arg_types);
    desc.parameters = Array();
    agg_descs.push_back(desc);

    // utf8mb4_bin collation (padding binary)
    auto collator = TiDB::ITiDBCollator::getCollator("utf8mb4_bin");
    TiDB::TiDBCollators collators = {collator};

    Aggregator::Params params(
        block.cloneEmpty(),
        keys,
        /*key_ref_agg_func=*/{},
        /*agg_func_ref_key=*/{},
        agg_descs,
        /*group_by_two_level_threshold=*/0,
        /*group_by_two_level_threshold_bytes=*/0,
        /*max_bytes_before_external_group_by=*/0,
        /*empty_result_for_aggregation_by_empty_set=*/false,
        SpillConfig("/tmp/tiflash_test_spill", "test", 0, 0, 0, nullptr),
        /*max_block_size=*/65536,
        /*use_magic_hash=*/false,
        collators);

    auto aggregator = std::make_unique<Aggregator>(
        params,
        "test",
        /*concurrency=*/1,
        /*register_operator_spill_context=*/nullptr,
        /*is_auto_pass_through=*/false,
        /*use_magic_hash=*/false);

    auto data = std::make_shared<AggregatedDataVariants>();
    Aggregator::AggProcessInfo info(aggregator.get());
    info.resetBlock(block);
    aggregator->executeOnBlock(info, *data, 0);

    EXPECT_TRUE(aggregator->dict_key_state.isActive())
        << "Dict-key path should activate with utf8mb4_bin collation";
    EXPECT_EQ(aggregator->dict_key_state.id_to_value.size(), 5u);
}
CATCH

/// Test that dict-key path does NOT activate with case-insensitive collation
TEST_F(AggregatorDictKeyTest, CICollation_DoesNotActivate)
try
{
    auto block = makeStringKeyBlock(
        {"US", "UK", "DE", "FR", "JP"},
        {10, 20, 30, 40, 50});

    auto context = TiFlashTestEnv::getContext();
    ColumnNumbers keys = {0};
    AggregateDescriptions agg_descs;
    AggregateDescription desc;
    desc.column_name = "sum_val";
    desc.arguments = {1};
    DataTypes arg_types = {std::make_shared<DataTypeInt64>()};
    desc.function = AggregateFunctionFactory::instance().get(*context, "sum", arg_types);
    desc.parameters = Array();
    agg_descs.push_back(desc);

    // utf8mb4_general_ci collation (case-insensitive — NOT safe for dict-key)
    auto collator = TiDB::ITiDBCollator::getCollator("utf8mb4_general_ci");
    TiDB::TiDBCollators collators = {collator};

    Aggregator::Params params(
        block.cloneEmpty(),
        keys,
        /*key_ref_agg_func=*/{},
        /*agg_func_ref_key=*/{},
        agg_descs,
        /*group_by_two_level_threshold=*/0,
        /*group_by_two_level_threshold_bytes=*/0,
        /*max_bytes_before_external_group_by=*/0,
        /*empty_result_for_aggregation_by_empty_set=*/false,
        SpillConfig("/tmp/tiflash_test_spill", "test", 0, 0, 0, nullptr),
        /*max_block_size=*/65536,
        /*use_magic_hash=*/false,
        collators);

    auto aggregator = std::make_unique<Aggregator>(
        params,
        "test",
        /*concurrency=*/1,
        /*register_operator_spill_context=*/nullptr,
        /*is_auto_pass_through=*/false,
        /*use_magic_hash=*/false);

    auto data = std::make_shared<AggregatedDataVariants>();
    Aggregator::AggProcessInfo info(aggregator.get());
    info.resetBlock(block);
    aggregator->executeOnBlock(info, *data, 0);

    EXPECT_FALSE(aggregator->dict_key_state.isActive())
        << "Dict-key path should NOT activate with case-insensitive collation";
}
CATCH

TEST_F(AggregatorDictKeyTest, FrozenDictionary_MultipleBlocks)
try
{
    auto block1 = makeStringKeyBlock(
        {"US", "UK", "DE", "FR", "JP"},
        {10, 20, 30, 40, 50});

    auto block2 = makeStringKeyBlock(
        {"US", "JP", "DE", "UK", "FR", "US", "US", "DE", "JP", "FR"},
        {100, 200, 300, 400, 500, 600, 700, 800, 900, 1000});

    auto context = TiFlashTestEnv::getContext();
    ColumnNumbers keys = {0};
    AggregateDescriptions agg_descs;
    AggregateDescription desc;
    desc.column_name = "sum_val";
    desc.arguments = {1};
    DataTypes arg_types = {std::make_shared<DataTypeInt64>()};
    desc.function = AggregateFunctionFactory::instance().get(*context, "sum", arg_types);
    desc.parameters = Array();
    agg_descs.push_back(desc);

    Aggregator::Params params(
        block1.cloneEmpty(),
        keys,
        /*key_ref_agg_func=*/{},
        /*agg_func_ref_key=*/{},
        agg_descs,
        /*group_by_two_level_threshold=*/0,
        /*group_by_two_level_threshold_bytes=*/0,
        /*max_bytes_before_external_group_by=*/0,
        /*empty_result_for_aggregation_by_empty_set=*/false,
        SpillConfig("/tmp/tiflash_test_spill", "test", 0, 0, 0, nullptr),
        /*max_block_size=*/65536,
        /*use_magic_hash=*/false);

    auto aggregator = std::make_unique<Aggregator>(
        params,
        "test",
        /*concurrency=*/1,
        /*register_operator_spill_context=*/nullptr,
        /*is_auto_pass_through=*/false,
        /*use_magic_hash=*/false);

    auto data = std::make_shared<AggregatedDataVariants>();
    Aggregator::AggProcessInfo info(aggregator.get());

    // Block 1: populates dictionary, should NOT freeze (new entries added)
    info.resetBlock(block1);
    aggregator->executeOnBlock(info, *data, 0);
    EXPECT_TRUE(aggregator->dict_key_state.isActive());
    EXPECT_EQ(aggregator->dict_key_state.id_to_value.size(), 5u);

    // Block 2: all values already in dictionary, should trigger freeze
    info.resetBlock(block2);
    aggregator->executeOnBlock(info, *data, 0);
    EXPECT_TRUE(aggregator->dict_key_state.isActive());
    EXPECT_TRUE(aggregator->dict_key_state.dictionary_frozen.load());

    // Verify result via merge
    ManyAggregatedDataVariants many_data;
    many_data.push_back(std::move(data));
    auto merging = aggregator->mergeAndConvertToBlocks(many_data, /*final=*/true, /*max_threads=*/1);
    size_t total_rows = 0;
    if (merging)
    {
        while (true)
        {
            Block out = merging->getData(0);
            if (!out)
                break;
            total_rows += out.rows();
        }
    }
    EXPECT_EQ(total_rows, 5u);
}
CATCH

/// Test that dict-key activates and produces correct results with multi-thread concurrency
TEST_F(AggregatorDictKeyTest, MultiThread_Concurrent)
try
{
    auto block1 = makeStringKeyBlock(
        {"US", "UK", "DE", "FR", "JP"},
        {10, 20, 30, 40, 50});

    auto block2 = makeStringKeyBlock(
        {"US", "JP", "DE"},
        {100, 200, 300});

    auto block3 = makeStringKeyBlock(
        {"UK", "FR", "US", "DE"},
        {400, 500, 600, 700});

    auto context = TiFlashTestEnv::getContext();
    ColumnNumbers keys = {0};
    AggregateDescriptions agg_descs;
    AggregateDescription desc;
    desc.column_name = "sum_val";
    desc.arguments = {1};
    DataTypes arg_types = {std::make_shared<DataTypeInt64>()};
    desc.function = AggregateFunctionFactory::instance().get(*context, "sum", arg_types);
    desc.parameters = Array();
    agg_descs.push_back(desc);

    size_t concurrency = 3;
    Aggregator::Params params(
        block1.cloneEmpty(),
        keys,
        /*key_ref_agg_func=*/{},
        /*agg_func_ref_key=*/{},
        agg_descs,
        /*group_by_two_level_threshold=*/0,
        /*group_by_two_level_threshold_bytes=*/0,
        /*max_bytes_before_external_group_by=*/0,
        /*empty_result_for_aggregation_by_empty_set=*/false,
        SpillConfig("/tmp/tiflash_test_spill", "test", 0, 0, 0, nullptr),
        /*max_block_size=*/65536,
        /*use_magic_hash=*/false);

    auto aggregator = std::make_unique<Aggregator>(
        params,
        "test",
        /*concurrency=*/concurrency,
        /*register_operator_spill_context=*/nullptr,
        /*is_auto_pass_through=*/false,
        /*use_magic_hash=*/false);

    // Each thread gets its own AggregatedDataVariants
    std::vector<std::shared_ptr<AggregatedDataVariants>> thread_data(concurrency);
    for (size_t i = 0; i < concurrency; ++i)
        thread_data[i] = std::make_shared<AggregatedDataVariants>();

    // Process blocks on separate threads
    std::vector<Block> blocks = {block1, block2, block3};
    std::vector<std::thread> threads;
    for (size_t i = 0; i < concurrency; ++i)
    {
        threads.emplace_back([&, i]() {
            Aggregator::AggProcessInfo info(aggregator.get());
            info.resetBlock(blocks[i]);
            aggregator->executeOnBlock(info, *thread_data[i], i);
        });
    }
    for (auto & t : threads)
        t.join();

    EXPECT_TRUE(aggregator->dict_key_state.isActive());
    EXPECT_EQ(aggregator->dict_key_state.id_to_value.size(), 5u);

    // Merge all thread results
    ManyAggregatedDataVariants many_data;
    for (auto & d : thread_data)
        many_data.push_back(d);
    auto merging = aggregator->mergeAndConvertToBlocks(many_data, /*final=*/true, /*max_threads=*/1);
    std::map<String, Int64> results;
    if (merging)
    {
        while (true)
        {
            Block out = merging->getData(0);
            if (!out)
                break;
            auto & key_col = typeid_cast<const ColumnString &>(*out.getByName("key").column);
            auto & sum_col = typeid_cast<const ColumnVector<Int64> &>(*out.getByName("sum_val").column);
            for (size_t i = 0; i < out.rows(); ++i)
                results[key_col.getDataAt(i).toString()] += sum_col.getData()[i];
        }
    }
    // Thread 1: US=10, UK=20, DE=30, FR=40, JP=50
    // Thread 2: US=100, JP=200, DE=300
    // Thread 3: UK=400, FR=500, US=600, DE=700
    EXPECT_EQ(results.size(), 5u);
    EXPECT_EQ(results["US"], 710);  // 10+100+600
    EXPECT_EQ(results["UK"], 420);  // 20+400
    EXPECT_EQ(results["DE"], 1030); // 30+300+700
    EXPECT_EQ(results["FR"], 540);  // 40+500
    EXPECT_EQ(results["JP"], 250);  // 50+200
}
CATCH

/// Test that padding-binary collation (utf8mb4_bin) correctly normalizes trailing spaces
TEST_F(AggregatorDictKeyTest, PaddingBinary_TrailingSpacesNormalized)
try
{
    // "US", "US " (1 space), "US  " (2 spaces) should all map to same dict ID
    auto block = makeStringKeyBlock(
        {"US", "US ", "US  ", "UK", "UK "},
        {10, 20, 30, 40, 50});

    auto context = TiFlashTestEnv::getContext();
    ColumnNumbers keys = {0};
    AggregateDescriptions agg_descs;
    AggregateDescription desc;
    desc.column_name = "sum_val";
    desc.arguments = {1};
    DataTypes arg_types = {std::make_shared<DataTypeInt64>()};
    desc.function = AggregateFunctionFactory::instance().get(*context, "sum", arg_types);
    desc.parameters = Array();
    agg_descs.push_back(desc);

    // utf8mb4_bin collation (padding binary — strips trailing spaces)
    auto collator = TiDB::ITiDBCollator::getCollator("utf8mb4_bin");
    TiDB::TiDBCollators collators = {collator};

    Aggregator::Params params(
        block.cloneEmpty(),
        keys,
        /*key_ref_agg_func=*/{},
        /*agg_func_ref_key=*/{},
        agg_descs,
        /*group_by_two_level_threshold=*/0,
        /*group_by_two_level_threshold_bytes=*/0,
        /*max_bytes_before_external_group_by=*/0,
        /*empty_result_for_aggregation_by_empty_set=*/false,
        SpillConfig("/tmp/tiflash_test_spill", "test", 0, 0, 0, nullptr),
        /*max_block_size=*/65536,
        /*use_magic_hash=*/false,
        collators);

    auto aggregator = std::make_unique<Aggregator>(
        params,
        "test",
        /*concurrency=*/1,
        /*register_operator_spill_context=*/nullptr,
        /*is_auto_pass_through=*/false,
        /*use_magic_hash=*/false);

    auto data = std::make_shared<AggregatedDataVariants>();
    Aggregator::AggProcessInfo info(aggregator.get());
    info.resetBlock(block);
    aggregator->executeOnBlock(info, *data, 0);

    EXPECT_TRUE(aggregator->dict_key_state.isActive());
    EXPECT_TRUE(aggregator->dict_key_state.padding_binary);
    // "US", "US ", "US  " should all normalize to "US" → 2 distinct groups, not 4
    EXPECT_EQ(aggregator->dict_key_state.id_to_value.size(), 2u)
        << "Trailing spaces should be normalized for padding-binary collation";

    // Verify sums: US group = 10+20+30=60, UK group = 40+50=90
    ManyAggregatedDataVariants many_data;
    many_data.push_back(std::move(data));
    auto merging = aggregator->mergeAndConvertToBlocks(many_data, /*final=*/true, /*max_threads=*/1);
    Int64 total_sum = 0;
    size_t total_rows = 0;
    if (merging)
    {
        while (true)
        {
            Block out = merging->getData(0);
            if (!out)
                break;
            total_rows += out.rows();
            auto & sum_col = typeid_cast<const ColumnVector<Int64> &>(*out.getByName("sum_val").column);
            for (size_t i = 0; i < out.rows(); ++i)
                total_sum += sum_col.getData()[i];
        }
    }
    EXPECT_EQ(total_rows, 2u);
    EXPECT_EQ(total_sum, 150); // 60 + 90
}
CATCH

/// Test that pure BINARY collation does NOT normalize trailing spaces
TEST_F(AggregatorDictKeyTest, PureBinary_TrailingSpacesPreserved)
try
{
    // With pure BINARY collation, "US" and "US " are DIFFERENT keys
    auto block = makeStringKeyBlock(
        {"US", "US ", "UK"},
        {10, 20, 30});

    auto context = TiFlashTestEnv::getContext();
    ColumnNumbers keys = {0};
    AggregateDescriptions agg_descs;
    AggregateDescription desc;
    desc.column_name = "sum_val";
    desc.arguments = {1};
    DataTypes arg_types = {std::make_shared<DataTypeInt64>()};
    desc.function = AggregateFunctionFactory::instance().get(*context, "sum", arg_types);
    desc.parameters = Array();
    agg_descs.push_back(desc);

    // Pure BINARY collation (no padding — exact byte comparison)
    auto collator = TiDB::ITiDBCollator::getCollator("binary");
    TiDB::TiDBCollators collators = {collator};

    Aggregator::Params params(
        block.cloneEmpty(),
        keys,
        /*key_ref_agg_func=*/{},
        /*agg_func_ref_key=*/{},
        agg_descs,
        /*group_by_two_level_threshold=*/0,
        /*group_by_two_level_threshold_bytes=*/0,
        /*max_bytes_before_external_group_by=*/0,
        /*empty_result_for_aggregation_by_empty_set=*/false,
        SpillConfig("/tmp/tiflash_test_spill", "test", 0, 0, 0, nullptr),
        /*max_block_size=*/65536,
        /*use_magic_hash=*/false,
        collators);

    auto aggregator = std::make_unique<Aggregator>(
        params,
        "test",
        /*concurrency=*/1,
        /*register_operator_spill_context=*/nullptr,
        /*is_auto_pass_through=*/false,
        /*use_magic_hash=*/false);

    auto data = std::make_shared<AggregatedDataVariants>();
    Aggregator::AggProcessInfo info(aggregator.get());
    info.resetBlock(block);
    aggregator->executeOnBlock(info, *data, 0);

    EXPECT_TRUE(aggregator->dict_key_state.isActive());
    EXPECT_FALSE(aggregator->dict_key_state.padding_binary);
    // "US" and "US " are different keys under pure BINARY → 3 distinct groups
    EXPECT_EQ(aggregator->dict_key_state.id_to_value.size(), 3u)
        << "Trailing spaces should be preserved for pure BINARY collation";
}
CATCH

} // namespace DB::tests
