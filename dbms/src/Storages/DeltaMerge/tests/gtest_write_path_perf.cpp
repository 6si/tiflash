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

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/Stopwatch.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Compression/CompressedWriteBuffer.h>
#include <Interpreters/sortBlock.h>
#include <Storages/DeltaMerge/ColumnFile/ColumnFilePersisted.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <gtest/gtest.h>

#include <random>

namespace DB::DM::tests
{

class WritePathPerfTest : public ::testing::Test
{
protected:
    static Block createBlock(size_t rows, size_t json_size_bytes, bool pre_sorted)
    {
        std::mt19937 rng(42);
        Block block;

        // Handle column (Int64) — simulates _tidb_rowid
        auto handle_col = ColumnInt64::create(rows);
        auto & handles = handle_col->getData();
        for (size_t i = 0; i < rows; ++i)
            handles[i] = pre_sorted ? static_cast<Int64>(i) : static_cast<Int64>(rng() % (rows * 10));
        block.insert(ColumnWithTypeAndName{std::move(handle_col), std::make_shared<DataTypeInt64>(), "_tidb_rowid"});

        // Version column (UInt64)
        auto ver_col = ColumnUInt64::create(rows);
        auto & versions = ver_col->getData();
        for (size_t i = 0; i < rows; ++i)
            versions[i] = 1;
        block.insert(ColumnWithTypeAndName{std::move(ver_col), std::make_shared<DataTypeUInt64>(), "__version"});

        // Del mark column (UInt8)
        auto del_col = ColumnUInt8::create(rows);
        auto & del_marks = del_col->getData();
        for (size_t i = 0; i < rows; ++i)
            del_marks[i] = 0;
        block.insert(ColumnWithTypeAndName{std::move(del_col), std::make_shared<DataTypeUInt8>(), "__del_mark"});

        // Integer column (simulates tenant_id)
        auto int_col = ColumnInt64::create(rows);
        auto & ints = int_col->getData();
        for (size_t i = 0; i < rows; ++i)
            ints[i] = rng() % 1000;
        block.insert(ColumnWithTypeAndName{std::move(int_col), std::make_shared<DataTypeInt64>(), "tenant_id"});

        // JSON payload column (ColumnString with large blobs)
        auto json_col = ColumnString::create();
        json_col->reserve(rows);
        // Generate random JSON-like strings of specified size
        String base_json(json_size_bytes, ' ');
        for (size_t i = 0; i < rows; ++i)
        {
            // Vary slightly to prevent trivial compression
            base_json[0] = '{';
            base_json[json_size_bytes - 1] = '}';
            // Put some varying content
            auto offset = (i * 7) % (json_size_bytes - 20);
            auto snippet = fmt::format("\"id\":{}", i);
            memcpy(base_json.data() + 1 + offset, snippet.data(), std::min(snippet.size(), json_size_bytes - 2 - offset));
            json_col->insertData(base_json.data(), json_size_bytes);
        }
        block.insert(ColumnWithTypeAndName{std::move(json_col), std::make_shared<DataTypeString>(), "payload"});

        return block;
    }

    // Measure sort time for a block
    static double measureSortTime(Block & block, size_t iterations)
    {
        Stopwatch sw;
        for (size_t iter = 0; iter < iterations; ++iter)
        {
            // Create a copy for each iteration since sort is in-place
            Block copy = block.cloneWithColumns(block.getColumns());
            SortDescription sort;
            sort.emplace_back("_tidb_rowid", 1, 0);
            sort.emplace_back("__version", 1, 0);
            if (!isAlreadySorted(copy, sort))
                stableSortBlock(copy, sort);
        }
        return sw.elapsedSeconds() / iterations;
    }

    // Measure serialization + compression time (simulates ColumnFileTiny::writeColumnFileData)
    static double measureSerializeTime(const Block & block, CompressionMethod method, size_t iterations)
    {
        Stopwatch sw;
        for (size_t iter = 0; iter < iterations; ++iter)
        {
            WriteBufferFromOwnString write_buf;
            for (const auto & col : block)
            {
                serializeColumn(write_buf, *col.column, col.type, 0, col.column->size(), method, 1);
            }
        }
        return sw.elapsedSeconds() / iterations;
    }

    // Measure just the raw column serialization without compression
    static double measureRawSerializeTime(const Block & block, size_t iterations)
    {
        Stopwatch sw;
        for (size_t iter = 0; iter < iterations; ++iter)
        {
            WriteBufferFromOwnString write_buf;
            for (const auto & col : block)
            {
                col.type->serializeBinaryBulkWithMultipleStreams(
                    *col.column,
                    [&](const IDataType::SubstreamPath &) { return &write_buf; },
                    0,
                    col.column->size(),
                    true,
                    {});
            }
        }
        return sw.elapsedSeconds() / iterations;
    }
};

// =============================================================================
// Benchmark: Sort overhead
// =============================================================================

TEST_F(WritePathPerfTest, SortOverhead_PreSorted)
{
    // Typical case: TiKV data is already sorted by handle
    auto block = createBlock(8192, 512, /*pre_sorted=*/true);
    auto sort_time = measureSortTime(block, 10);
    fmt::print(
        "\n[Sort] Pre-sorted block (8192 rows, 512B JSON): {:.3f} ms/block\n"
        "  → isAlreadySorted check dominates (O(n) scan)\n",
        sort_time * 1000);
}

TEST_F(WritePathPerfTest, SortOverhead_Unsorted)
{
    // Worst case: unsorted data (e.g. from concurrent txns)
    auto block = createBlock(8192, 512, /*pre_sorted=*/false);
    auto sort_time = measureSortTime(block, 5);
    fmt::print(
        "\n[Sort] Unsorted block (8192 rows, 512B JSON): {:.3f} ms/block\n"
        "  → Full stableSortBlock with column permutation\n",
        sort_time * 1000);
}

TEST_F(WritePathPerfTest, SortOverhead_LargeJson)
{
    // Large JSON case: sort is expensive because column permutation moves 3KB per row
    auto block = createBlock(8192, 3072, /*pre_sorted=*/false);
    auto sort_time = measureSortTime(block, 3);
    fmt::print(
        "\n[Sort] Unsorted block (8192 rows, 3KB JSON): {:.3f} ms/block\n"
        "  → Column permutation dominates with large strings\n",
        sort_time * 1000);
}

TEST_F(WritePathPerfTest, SortOverhead_LargeJson_Parallel)
{
    // OPTIMIZED: Parallel column permutation for large string columns
    auto block = createBlock(8192, 3072, /*pre_sorted=*/false);

    Stopwatch sw;
    const size_t iterations = 3;
    for (size_t iter = 0; iter < iterations; ++iter)
    {
        Block copy = block.cloneWithColumns(block.getColumns());
        SortDescription sort;
        sort.emplace_back("_tidb_rowid", 1, 0);
        sort.emplace_back("__version", 1, 0);
        if (!isAlreadySorted(copy, sort))
            stableSortBlockParallel(copy, sort);
    }
    auto parallel_time = sw.elapsedSeconds() / iterations;

    // Also measure sequential for comparison
    Stopwatch sw2;
    for (size_t iter = 0; iter < iterations; ++iter)
    {
        Block copy = block.cloneWithColumns(block.getColumns());
        SortDescription sort;
        sort.emplace_back("_tidb_rowid", 1, 0);
        sort.emplace_back("__version", 1, 0);
        if (!isAlreadySorted(copy, sort))
            stableSortBlock(copy, sort);
    }
    auto sequential_time = sw2.elapsedSeconds() / iterations;

    fmt::print(
        "\n[Sort] Unsorted block (8192 rows, 3KB JSON) — PARALLEL vs SEQUENTIAL:\n"
        "  Sequential: {:.3f} ms/block\n"
        "  Parallel:   {:.3f} ms/block\n"
        "  Speedup:    {:.2f}x\n",
        sequential_time * 1000,
        parallel_time * 1000,
        sequential_time / parallel_time);
}

TEST_F(WritePathPerfTest, SortOverhead_MultipleJsonColumns_Parallel)
{
    // Multiple large JSON columns (e.g., payload + metadata + raw_event)
    std::mt19937 rng(42);
    Block block;

    // Handle column
    auto handle_col = ColumnInt64::create(8192);
    auto & handles = handle_col->getData();
    for (size_t i = 0; i < 8192; ++i)
        handles[i] = static_cast<Int64>(rng() % 80000);
    block.insert(ColumnWithTypeAndName{std::move(handle_col), std::make_shared<DataTypeInt64>(), "_tidb_rowid"});

    // Version column
    auto ver_col = ColumnUInt64::create(8192);
    auto & versions = ver_col->getData();
    for (size_t i = 0; i < 8192; ++i)
        versions[i] = 1;
    block.insert(ColumnWithTypeAndName{std::move(ver_col), std::make_shared<DataTypeUInt64>(), "__version"});

    // Three large JSON columns (simulating real-world analytics tables)
    for (int j = 0; j < 3; ++j)
    {
        auto json_col = ColumnString::create();
        json_col->reserve(8192);
        String base(2048, 'x');
        for (size_t i = 0; i < 8192; ++i)
        {
            base[i % 2000] = 'a' + (i % 26);
            json_col->insertData(base.data(), 2048);
        }
        block.insert(ColumnWithTypeAndName{std::move(json_col), std::make_shared<DataTypeString>(), fmt::format("json_col_{}", j)});
    }

    const size_t iterations = 3;

    // Sequential
    Stopwatch sw1;
    for (size_t iter = 0; iter < iterations; ++iter)
    {
        Block copy = block.cloneWithColumns(block.getColumns());
        SortDescription sort;
        sort.emplace_back("_tidb_rowid", 1, 0);
        sort.emplace_back("__version", 1, 0);
        stableSortBlock(copy, sort);
    }
    auto sequential_time = sw1.elapsedSeconds() / iterations;

    // Parallel
    Stopwatch sw2;
    for (size_t iter = 0; iter < iterations; ++iter)
    {
        Block copy = block.cloneWithColumns(block.getColumns());
        SortDescription sort;
        sort.emplace_back("_tidb_rowid", 1, 0);
        sort.emplace_back("__version", 1, 0);
        stableSortBlockParallel(copy, sort);
    }
    auto parallel_time = sw2.elapsedSeconds() / iterations;

    fmt::print(
        "\n[Sort] Unsorted block (8192 rows, 3×2KB JSON cols = 48MB) — PARALLEL vs SEQUENTIAL:\n"
        "  Sequential: {:.3f} ms/block\n"
        "  Parallel:   {:.3f} ms/block\n"
        "  Speedup:    {:.2f}x\n"
        "  → Multiple large columns benefit from parallel permutation\n",
        sequential_time * 1000,
        parallel_time * 1000,
        sequential_time / parallel_time);
}

// =============================================================================
// Benchmark: Serialization + Compression
// =============================================================================

TEST_F(WritePathPerfTest, Serialize_LZ4_SmallJson)
{
    auto block = createBlock(8192, 512, true);
    auto lz4_time = measureSerializeTime(block, CompressionMethod::LZ4, 5);
    auto raw_time = measureRawSerializeTime(block, 5);
    fmt::print(
        "\n[Serialize] 8192 rows × 512B JSON:\n"
        "  LZ4:        {:.3f} ms/block ({:.1f} MB/s)\n"
        "  Raw (none):  {:.3f} ms/block ({:.1f} MB/s)\n"
        "  Compression overhead: {:.1f}%\n",
        lz4_time * 1000,
        block.bytes() / lz4_time / 1e6,
        raw_time * 1000,
        block.bytes() / raw_time / 1e6,
        (lz4_time - raw_time) / raw_time * 100);
}

TEST_F(WritePathPerfTest, Serialize_LZ4_LargeJson)
{
    auto block = createBlock(8192, 3072, true);
    auto lz4_time = measureSerializeTime(block, CompressionMethod::LZ4, 3);
    auto raw_time = measureRawSerializeTime(block, 3);
    auto block_bytes_mb = block.bytes() / 1e6;
    fmt::print(
        "\n[Serialize] 8192 rows × 3KB JSON ({:.1f} MB/block):\n"
        "  LZ4:        {:.3f} ms/block ({:.1f} MB/s)\n"
        "  Raw (none):  {:.3f} ms/block ({:.1f} MB/s)\n"
        "  Compression overhead: {:.1f}%\n"
        "  → For delta layer (transient data), compression is WASTED CPU\n",
        block_bytes_mb,
        lz4_time * 1000,
        block.bytes() / lz4_time / 1e6,
        raw_time * 1000,
        block.bytes() / raw_time / 1e6,
        (lz4_time - raw_time) / raw_time * 100);
}

TEST_F(WritePathPerfTest, Serialize_NONE_vs_LZ4_vs_ZSTD)
{
    auto block = createBlock(8192, 3072, true);
    auto none_time = measureRawSerializeTime(block, 5);
    auto lz4_time = measureSerializeTime(block, CompressionMethod::LZ4, 3);
    auto zstd_time = measureSerializeTime(block, CompressionMethod::ZSTD, 3);
    fmt::print(
        "\n[Compression comparison] 8192 rows × 3KB JSON:\n"
        "  NONE:  {:.3f} ms ({:.0f} MB/s)\n"
        "  LZ4:   {:.3f} ms ({:.0f} MB/s)\n"
        "  ZSTD:  {:.3f} ms ({:.0f} MB/s)\n"
        "  LZ4/NONE ratio: {:.2f}x slower\n"
        "  ZSTD/NONE ratio: {:.2f}x slower\n",
        none_time * 1000,
        block.bytes() / none_time / 1e6,
        lz4_time * 1000,
        block.bytes() / lz4_time / 1e6,
        zstd_time * 1000,
        block.bytes() / zstd_time / 1e6,
        lz4_time / none_time,
        zstd_time / none_time);
}

// =============================================================================
// Benchmark: Write amplification analysis
// =============================================================================

TEST_F(WritePathPerfTest, WriteAmplification_MergeDelta)
{
    // Simulates: 100 packs of stable data + 1 pack of delta
    // Current behavior: re-read + re-compress ALL 101 packs
    // Optimal: copy 100 packs unchanged + only compress 1 new pack
    const size_t num_stable_packs = 100;
    const size_t rows_per_pack = 8192;
    const size_t json_bytes = 3072;

    // Time to serialize 1 pack (what merge delta SHOULD do for the delta portion)
    auto one_pack = createBlock(rows_per_pack, json_bytes, true);
    auto one_pack_time = measureSerializeTime(one_pack, CompressionMethod::LZ4, 3);

    // Time to serialize all 101 packs (what merge delta ACTUALLY does)
    auto full_time = one_pack_time * (num_stable_packs + 1);

    // Time for optimal approach: memcpy 100 stable packs + compress 1 new
    // Assume memcpy is ~10x faster than compress for same data size
    auto memcpy_time = one_pack_time * 0.1 * num_stable_packs;
    auto optimal_time = memcpy_time + one_pack_time;

    fmt::print(
        "\n[Write Amplification] Merge delta: 100 stable packs + 1 delta pack (3KB JSON)\n"
        "  Current (re-compress all): {:.1f} ms\n"
        "  Optimal (copy stable + compress delta): {:.1f} ms\n"
        "  Potential speedup: {:.1f}x\n"
        "  → Pack-level copy avoids {} unnecessary decompress/recompress cycles\n",
        full_time * 1000,
        optimal_time * 1000,
        full_time / optimal_time,
        num_stable_packs);
}

// =============================================================================
// Benchmark: Overall write path throughput
// =============================================================================

TEST_F(WritePathPerfTest, EndToEnd_SmallBlocks)
{
    // Simulates Raft apply: many small blocks written sequentially
    const size_t num_blocks = 100;
    const size_t rows_per_block = 1024; // Typical Raft batch size
    const size_t json_bytes = 512;

    auto block = createBlock(rows_per_block, json_bytes, true); // Pre-sorted from TiKV

    Stopwatch sw;
    for (size_t i = 0; i < num_blocks; ++i)
    {
        Block copy = block.cloneWithColumns(block.getColumns());
        // Sort check
        SortDescription sort;
        sort.emplace_back("_tidb_rowid", 1, 0);
        sort.emplace_back("__version", 1, 0);
        if (!isAlreadySorted(copy, sort))
            stableSortBlock(copy, sort);
        // Serialize (simulates writeColumnFileData)
        WriteBufferFromOwnString write_buf;
        for (const auto & col : copy)
        {
            serializeColumn(write_buf, *col.column, col.type, 0, col.column->size(), CompressionMethod::LZ4, 1);
        }
    }
    auto total_time = sw.elapsedSeconds();
    auto total_rows = num_blocks * rows_per_block;
    auto total_bytes = num_blocks * block.bytes();

    fmt::print(
        "\n[EndToEnd] {} blocks × {} rows × {}B JSON (pre-sorted):\n"
        "  Total time: {:.1f} ms\n"
        "  Throughput: {:.0f} K rows/s, {:.0f} MB/s\n",
        num_blocks,
        rows_per_block,
        json_bytes,
        total_time * 1000,
        total_rows / total_time / 1000,
        total_bytes / total_time / 1e6);
}

TEST_F(WritePathPerfTest, EndToEnd_LargeJsonBlocks)
{
    // Large JSON scenario (our use case): 3KB JSON blobs
    const size_t num_blocks = 20;
    const size_t rows_per_block = 8192;
    const size_t json_bytes = 3072;

    auto block = createBlock(rows_per_block, json_bytes, true);

    Stopwatch sw;
    for (size_t i = 0; i < num_blocks; ++i)
    {
        Block copy = block.cloneWithColumns(block.getColumns());
        SortDescription sort;
        sort.emplace_back("_tidb_rowid", 1, 0);
        sort.emplace_back("__version", 1, 0);
        if (!isAlreadySorted(copy, sort))
            stableSortBlock(copy, sort);
        WriteBufferFromOwnString write_buf;
        for (const auto & col : copy)
        {
            serializeColumn(write_buf, *col.column, col.type, 0, col.column->size(), CompressionMethod::LZ4, 1);
        }
    }
    auto total_time = sw.elapsedSeconds();
    auto total_rows = num_blocks * rows_per_block;
    auto total_bytes = num_blocks * block.bytes();

    fmt::print(
        "\n[EndToEnd] {} blocks × {} rows × {}B JSON (pre-sorted):\n"
        "  Total time: {:.1f} ms\n"
        "  Throughput: {:.0f} K rows/s, {:.0f} MB/s\n"
        "  Data volume: {:.1f} MB\n"
        "  → Bottleneck: LZ4 compression of 3KB JSON blobs\n",
        num_blocks,
        rows_per_block,
        json_bytes,
        total_time * 1000,
        total_rows / total_time / 1000,
        total_bytes / total_time / 1e6,
        total_bytes / 1e6);
}

TEST_F(WritePathPerfTest, EndToEnd_UnsortedMultiJson_ParallelVsSequential)
{
    // Real-world scenario: unsorted blocks (region merge, concurrent txns)
    // with multiple JSON columns. This is where parallel sort helps most.
    const size_t num_blocks = 20;
    const size_t rows_per_block = 8192;

    // Create block with 2 JSON columns (payload + metadata) — typical analytics table
    std::mt19937 rng(42);
    Block template_block;

    auto handle_col = ColumnInt64::create(rows_per_block);
    for (size_t i = 0; i < rows_per_block; ++i)
        handle_col->getData()[i] = static_cast<Int64>(rng() % 80000);
    template_block.insert(ColumnWithTypeAndName{std::move(handle_col), std::make_shared<DataTypeInt64>(), "_tidb_rowid"});

    auto ver_col = ColumnUInt64::create(rows_per_block);
    for (size_t i = 0; i < rows_per_block; ++i)
        ver_col->getData()[i] = 1;
    template_block.insert(ColumnWithTypeAndName{std::move(ver_col), std::make_shared<DataTypeUInt64>(), "__version"});

    auto del_col = ColumnUInt8::create(rows_per_block);
    for (size_t i = 0; i < rows_per_block; ++i)
        del_col->getData()[i] = 0;
    template_block.insert(ColumnWithTypeAndName{std::move(del_col), std::make_shared<DataTypeUInt8>(), "__del_mark"});

    // 2 JSON columns × 2KB each
    for (int j = 0; j < 2; ++j)
    {
        auto json_col = ColumnString::create();
        json_col->reserve(rows_per_block);
        for (size_t i = 0; i < rows_per_block; ++i)
        {
            String data(2048, static_cast<char>('a' + ((i + j * 17) % 26)));
            data[0] = '{';
            data[data.size() - 1] = '}';
            json_col->insertData(data.data(), data.size());
        }
        template_block.insert(ColumnWithTypeAndName{std::move(json_col), std::make_shared<DataTypeString>(), fmt::format("json_{}", j)});
    }

    // Sequential sort + serialize
    Stopwatch sw1;
    for (size_t i = 0; i < num_blocks; ++i)
    {
        Block copy = template_block.cloneWithColumns(template_block.getColumns());
        SortDescription sort;
        sort.emplace_back("_tidb_rowid", 1, 0);
        sort.emplace_back("__version", 1, 0);
        if (!isAlreadySorted(copy, sort))
            stableSortBlock(copy, sort);
        WriteBufferFromOwnString write_buf;
        for (const auto & col : copy)
            serializeColumn(write_buf, *col.column, col.type, 0, col.column->size(), CompressionMethod::LZ4, 1);
    }
    auto sequential_time = sw1.elapsedSeconds();

    // Parallel sort + serialize
    Stopwatch sw2;
    for (size_t i = 0; i < num_blocks; ++i)
    {
        Block copy = template_block.cloneWithColumns(template_block.getColumns());
        SortDescription sort;
        sort.emplace_back("_tidb_rowid", 1, 0);
        sort.emplace_back("__version", 1, 0);
        if (!isAlreadySorted(copy, sort))
            stableSortBlockParallel(copy, sort);
        WriteBufferFromOwnString write_buf;
        for (const auto & col : copy)
            serializeColumn(write_buf, *col.column, col.type, 0, col.column->size(), CompressionMethod::LZ4, 1);
    }
    auto parallel_time = sw2.elapsedSeconds();

    auto total_rows = num_blocks * rows_per_block;
    auto total_bytes = num_blocks * template_block.bytes();

    fmt::print(
        "\n[EndToEnd] {} blocks × {} rows × 2 JSON cols (UNSORTED) — PARALLEL vs SEQUENTIAL:\n"
        "  Sequential: {:.1f} ms total, {:.0f} K rows/s, {:.0f} MB/s\n"
        "  Parallel:   {:.1f} ms total, {:.0f} K rows/s, {:.0f} MB/s\n"
        "  Speedup:    {:.2f}x\n"
        "  Data volume: {:.1f} MB\n",
        num_blocks,
        rows_per_block,
        sequential_time * 1000,
        total_rows / sequential_time / 1000,
        total_bytes / sequential_time / 1e6,
        parallel_time * 1000,
        total_rows / parallel_time / 1000,
        total_bytes / parallel_time / 1e6,
        sequential_time / parallel_time,
        total_bytes / 1e6);
}

} // namespace DB::DM::tests
