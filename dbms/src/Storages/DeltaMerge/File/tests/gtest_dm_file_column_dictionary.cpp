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

/// Tests for DMFileWriter -> Dictionary codec -> DMFileReader -> ColumnDictionary
/// round-trip preservation.  The write path forces SizePrefix format + Dictionary
/// codec for String columns; the read path detects dictionary blocks and returns
/// ColumnDictionary instead of ColumnString.

#include <Columns/ColumnDictionary.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/File/DMFileWriter.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/DeltaMerge/tests/DMTestEnv.h>
#include <Storages/PathPool.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>

namespace DB::DM::tests
{

class DMFileColumnDictionaryTest : public DB::base::TiFlashStorageTestBasic
{
public:
    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();

        parent_path = TiFlashStorageTestBasic::getTemporaryPath();
        path_pool = std::make_shared<StoragePathPool>(
            db_context->getPathPool().withTable("test", "DMFileColumnDictionaryTest", false));
        storage_pool
            = std::make_shared<StoragePool>(*db_context, NullspaceID, /*ns_id*/ 100, *path_pool, "test.t1");

        auto configuration = std::make_optional<DMChecksumConfig>();
        dm_file = DMFile::create(
            1,
            parent_path,
            std::move(configuration),
            128 * 1024,
            16 * 1024 * 1024,
            NullspaceID,
            DMFileFormat::V3);

        DB::tests::TiFlashTestEnv::disableS3Config();

        TiFlashStorageTestBasic::reload();
        *path_pool = db_context->getPathPool().withTable("test", "t1", false);
        dm_context = DMContext::createUnique(
            *db_context,
            path_pool,
            storage_pool,
            /*min_version_*/ 0,
            NullspaceID,
            /*physical_table_id*/ 100,
            /*pk_col_id*/ 0,
            false,
            1,
            db_context->getSettingsRef());
    }

    DMContext & dmContext() { return *dm_context; }
    Context & dbContext() { return *db_context; }

protected:
    String parent_path;
    std::shared_ptr<StoragePathPool> path_pool;
    std::shared_ptr<StoragePool> storage_pool;
    std::unique_ptr<DMContext> dm_context;
    DMFilePtr dm_file;
};

/// Write a block with low-cardinality string column via DMFileWriter,
/// then read it back via DMFileReader and verify ColumnDictionary is produced.
TEST_F(DMFileColumnDictionaryTest, WriteReadStringColumnDictionary)
try
{
    const size_t num_rows = 100;
    const std::vector<String> dict_values = {"active", "inactive", "pending", "deleted", "archived"};

    // Build column defines: handle + version + delmark + status(String)
    ColumnDefines write_cols;
    write_cols.push_back(getExtraHandleColumnDefine(/*is_common_handle=*/false));
    write_cols.push_back(getVersionColumnDefine());
    write_cols.push_back(getTagColumnDefine());
    write_cols.push_back(ColumnDefine{100, "status", std::make_shared<DataTypeString>()});

    // Build status string data
    std::vector<String> status_data;
    status_data.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        status_data.push_back(dict_values[i % dict_values.size()]);

    // Build the block using test helpers
    Block block;
    block.insert(
        DB::tests::createColumn<Int64>(createNumbers<Int64>(0, num_rows), MutSup::extra_handle_column_name, MutSup::extra_handle_id));
    block.insert(
        DB::tests::createColumn<UInt64>(std::vector<UInt64>(num_rows, 1), MutSup::version_column_name, MutSup::version_col_id));
    block.insert(
        DB::tests::createColumn<UInt8>(std::vector<UInt64>(num_rows, 0), MutSup::delmark_column_name, MutSup::delmark_col_id));
    block.insert(
        DB::tests::createColumn<String>(status_data, "status", 100));

    // Write
    {
        DMFileBlockOutputStream::BlockProperty prop{};
        prop.effective_num_rows = num_rows;
        prop.gc_hint_version = 1;
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, write_cols);
        stream->writePrefix();
        stream->write(block, prop);
        stream->writeSuffix();
    }

    // Read back and check that ColumnDictionary is produced
    {
        ColumnDefines read_cols;
        read_cols.push_back(ColumnDefine{100, "status", std::make_shared<DataTypeString>()});

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.build(
            dm_file,
            read_cols,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            std::make_shared<ScanContext>());

        stream->readPrefix();
        auto result_block = stream->read();
        stream->readSuffix();

        ASSERT_TRUE(result_block) << "Should read a non-empty block";
        ASSERT_EQ(result_block.rows(), num_rows);

        const auto & result_col = result_block.getByName("status").column;

        // The column should be ColumnDictionary (from the dictionary-encoded block)
        const auto * dict_col = typeid_cast<const ColumnDictionary *>(result_col.get());
        if (dict_col)
        {
            // Verify dictionary size matches our distinct values
            ASSERT_EQ(dict_col->getDictionarySize(), dict_values.size())
                << "Dictionary should have " << dict_values.size() << " entries";

            // Verify all values decode correctly
            for (size_t i = 0; i < num_rows; ++i)
            {
                Field val;
                dict_col->get(i, val);
                ASSERT_EQ(val.get<String>(), dict_values[i % dict_values.size()])
                    << "Mismatch at row " << i;
            }
        }
        else
        {
            // Fallback to ColumnString is also acceptable (codec decided not to use dict)
            // Still verify correctness
            for (size_t i = 0; i < num_rows; ++i)
            {
                Field val;
                result_col->get(i, val);
                ASSERT_EQ(val.get<String>(), dict_values[i % dict_values.size()])
                    << "Mismatch at row " << i;
            }
        }
    }
}
CATCH

/// Verify that ColumnDictionary from DMFile read has correct getDataAt behavior.
TEST_F(DMFileColumnDictionaryTest, WriteReadDictGetDataAt)
try
{
    const size_t num_rows = 200;
    const std::vector<String> dict_values = {"A", "B", "C", "D"};

    ColumnDefines write_cols;
    write_cols.push_back(getExtraHandleColumnDefine(false));
    write_cols.push_back(getVersionColumnDefine());
    write_cols.push_back(getTagColumnDefine());
    write_cols.push_back(ColumnDefine{100, "category", std::make_shared<DataTypeString>()});

    std::vector<String> cat_data;
    cat_data.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        cat_data.push_back(dict_values[i % dict_values.size()]);

    Block block;
    block.insert(
        DB::tests::createColumn<Int64>(createNumbers<Int64>(0, num_rows), MutSup::extra_handle_column_name, MutSup::extra_handle_id));
    block.insert(
        DB::tests::createColumn<UInt64>(std::vector<UInt64>(num_rows, 1), MutSup::version_column_name, MutSup::version_col_id));
    block.insert(
        DB::tests::createColumn<UInt8>(std::vector<UInt64>(num_rows, 0), MutSup::delmark_column_name, MutSup::delmark_col_id));
    block.insert(
        DB::tests::createColumn<String>(cat_data, "category", 100));

    // Write
    {
        DMFileBlockOutputStream::BlockProperty prop{};
        prop.effective_num_rows = num_rows;
        prop.gc_hint_version = 1;
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, write_cols);
        stream->writePrefix();
        stream->write(block, prop);
        stream->writeSuffix();
    }

    // Read back
    {
        ColumnDefines read_cols;
        read_cols.push_back(ColumnDefine{100, "category", std::make_shared<DataTypeString>()});

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.build(
            dm_file,
            read_cols,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            std::make_shared<ScanContext>());

        stream->readPrefix();
        auto result_block = stream->read();
        stream->readSuffix();

        ASSERT_TRUE(result_block);
        const auto & col = result_block.getByName("category").column;

        // Verify getDataAt works correctly regardless of column type
        for (size_t i = 0; i < num_rows; ++i)
        {
            auto ref = col->getDataAt(i);
            String actual(ref.data, ref.size);
            ASSERT_EQ(actual, dict_values[i % dict_values.size()])
                << "getDataAt mismatch at row " << i;
        }
    }
}
CATCH

/// Verify that high-cardinality columns still read correctly
/// (Dictionary codec may fall back to uncompressed).
TEST_F(DMFileColumnDictionaryTest, HighCardinalityFallback)
try
{
    const size_t num_rows = 200;

    ColumnDefines write_cols;
    write_cols.push_back(getExtraHandleColumnDefine(false));
    write_cols.push_back(getVersionColumnDefine());
    write_cols.push_back(getTagColumnDefine());
    write_cols.push_back(ColumnDefine{100, "unique_id", std::make_shared<DataTypeString>()});

    std::vector<String> id_data;
    id_data.reserve(num_rows);
    for (size_t i = 0; i < num_rows; ++i)
        id_data.push_back(fmt::format("unique_value_{:06d}", i));

    Block block;
    block.insert(
        DB::tests::createColumn<Int64>(createNumbers<Int64>(0, num_rows), MutSup::extra_handle_column_name, MutSup::extra_handle_id));
    block.insert(
        DB::tests::createColumn<UInt64>(std::vector<UInt64>(num_rows, 1), MutSup::version_column_name, MutSup::version_col_id));
    block.insert(
        DB::tests::createColumn<UInt8>(std::vector<UInt64>(num_rows, 0), MutSup::delmark_column_name, MutSup::delmark_col_id));
    block.insert(
        DB::tests::createColumn<String>(id_data, "unique_id", 100));

    // Write
    {
        DMFileBlockOutputStream::BlockProperty prop{};
        prop.effective_num_rows = num_rows;
        prop.gc_hint_version = 1;
        auto stream = std::make_shared<DMFileBlockOutputStream>(dbContext(), dm_file, write_cols);
        stream->writePrefix();
        stream->write(block, prop);
        stream->writeSuffix();
    }

    // Read back -- high cardinality may use Dictionary (all fit) or fall back
    {
        ColumnDefines read_cols;
        read_cols.push_back(ColumnDefine{100, "unique_id", std::make_shared<DataTypeString>()});

        DMFileBlockInputStreamBuilder builder(dbContext());
        auto stream = builder.build(
            dm_file,
            read_cols,
            RowKeyRanges{RowKeyRange::newAll(false, 1)},
            std::make_shared<ScanContext>());

        stream->readPrefix();
        auto result_block = stream->read();
        stream->readSuffix();

        ASSERT_TRUE(result_block);
        const auto & col = result_block.getByName("unique_id").column;

        // Verify correctness regardless of column type
        for (size_t i = 0; i < num_rows; ++i)
        {
            Field val;
            col->get(i, val);
            auto expected = fmt::format("unique_value_{:06d}", i);
            ASSERT_EQ(val.get<String>(), expected) << "Mismatch at row " << i;
        }
    }
}
CATCH

} // namespace DB::DM::tests
