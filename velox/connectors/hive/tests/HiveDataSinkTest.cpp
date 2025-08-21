/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"

#include <folly/init/Init.h>
#include <re2/re2.h>
#include "velox/common/base/Fs.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/dwio/dwrf/writer/FlushPolicy.h"
#include "velox/dwio/dwrf/writer/Writer.h"

#ifdef VELOX_ENABLE_PARQUET
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"
#endif

#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/PrefixSortUtils.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::connector::hive {
namespace {

using namespace facebook::velox::common;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::common::testutil;

class HiveDataSinkTest : public exec::test::HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    Type::registerSerDe();
    HiveSortingColumn::registerSerDe();
    HiveBucketProperty::registerSerDe();

    rowType_ =
        ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"},
            {BIGINT(),
             INTEGER(),
             SMALLINT(),
             REAL(),
             DOUBLE(),
             VARBINARY(),
             BOOLEAN(),
             VARBINARY(), // c7 - additional VARBINARY for compact sort key
             VARBINARY(), // c8 - additional VARBINARY for compact sort key
             VARBINARY()}); // c9 - additional VARBINARY for compact sort key

    // Create a compact-specific row type with VARBINARY sort keys
    compactRowType_ =
        ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"},
            {BIGINT(),
             INTEGER(),
             SMALLINT(),
             REAL(),
             DOUBLE(),
             VARBINARY(),
             BOOLEAN(),
             VARBINARY(), // c7 - sort key 1
             VARBINARY(), // c8 - sort key 2
             VARBINARY()}); // c9 - sort key 3

    setupMemoryPools();

    spillExecutor_ = std::make_unique<folly::IOThreadPoolExecutor>(
        std::thread::hardware_concurrency());
  }

  void TearDown() override {
    connectorQueryCtx_.reset();
    connectorPool_.reset();
    opPool_.reset();
    root_.reset();
    HiveConnectorTestBase::TearDown();
  }

  std::vector<RowVectorPtr> createVectors(int vectorSize, int numVectors) {
    VectorFuzzer::Options options;
    options.vectorSize = vectorSize;
    VectorFuzzer fuzzer(options, pool());
    std::vector<RowVectorPtr> vectors;
    for (int i = 0; i < numVectors; ++i) {
      vectors.push_back(fuzzer.fuzzInputRow(rowType_));
    }
    return vectors;
  }

  std::unique_ptr<SpillConfig> getSpillConfig(
      const std::string& spillPath,
      uint64_t writerFlushThreshold) {
    return std::make_unique<SpillConfig>(
        [spillPath]() -> const std::string& { return spillPath; },
        [&](uint64_t) {},
        "",
        0,
        0,
        /*readBufferSize=*/1 << 20,
        spillExecutor_.get(),
        10,
        20,
        0,
        0,
        0,
        0,
        writerFlushThreshold,
        "none");
  }

  void setupMemoryPools() {
    connectorQueryCtx_.reset();
    connectorPool_.reset();
    opPool_.reset();
    root_.reset();

    root_ = memory::memoryManager()->addRootPool(
        "HiveDataSinkTest", 1L << 30, exec::MemoryReclaimer::create());
    opPool_ = root_->addLeafChild("operator");
    connectorPool_ =
        root_->addAggregateChild("connector", exec::MemoryReclaimer::create());

    connectorQueryCtx_ = std::make_unique<connector::ConnectorQueryCtx>(
        opPool_.get(),
        connectorPool_.get(),
        connectorSessionProperties_.get(),
        nullptr,
        exec::test::defaultPrefixSortConfig(),
        nullptr,
        nullptr,
        "query.HiveDataSinkTest",
        "task.HiveDataSinkTest",
        "planNodeId.HiveDataSinkTest",
        0,
        "");
  }

  std::shared_ptr<connector::hive::HiveInsertTableHandle>
  createHiveInsertTableHandle(
      const RowTypePtr& outputRowType,
      const std::string& outputDirectoryPath,
      dwio::common::FileFormat fileFormat = dwio::common::FileFormat::DWRF,
      const std::vector<std::string>& partitionedBy = {},
      const std::shared_ptr<connector::hive::HiveBucketProperty>&
          bucketProperty = nullptr,
      const std::shared_ptr<dwio::common::WriterOptions>& writerOptions =
          nullptr) {
    return makeHiveInsertTableHandle(
        outputRowType->names(),
        outputRowType->children(),
        partitionedBy,
        bucketProperty,
        makeLocationHandle(
            outputDirectoryPath,
            std::nullopt,
            connector::hive::LocationHandle::TableType::kNew),
        fileFormat,
        CompressionKind::CompressionKind_ZSTD,
        {},
        writerOptions);
  }

  std::shared_ptr<HiveDataSink> createDataSink(
      const RowTypePtr& rowType,
      const std::string& outputDirectoryPath,
      dwio::common::FileFormat fileFormat = dwio::common::FileFormat::DWRF,
      const std::vector<std::string>& partitionedBy = {},
      const std::shared_ptr<connector::hive::HiveBucketProperty>&
          bucketProperty = nullptr,
      const std::shared_ptr<dwio::common::WriterOptions>& writerOptions =
          nullptr) {
    return std::make_shared<HiveDataSink>(
        rowType,
        createHiveInsertTableHandle(
            rowType,
            outputDirectoryPath,
            fileFormat,
            partitionedBy,
            bucketProperty,
            writerOptions),
        connectorQueryCtx_.get(),
        CommitStrategy::kNoCommit,
        connectorConfig_);
  }

  // Helper function to create HiveInsertTableHandle with CompactProperty
  std::shared_ptr<connector::hive::HiveInsertTableHandle>
  createHiveInsertTableHandleWithCompact(
      const RowTypePtr& outputRowType,
      const std::string& outputDirectoryPath,
      const std::shared_ptr<connector::hive::CompactProperty>& compactProperty,
      dwio::common::FileFormat fileFormat = dwio::common::FileFormat::DWRF,
      const std::vector<std::string>& partitionedBy = {},
      const std::shared_ptr<connector::hive::HiveBucketProperty>&
          bucketProperty = nullptr,
      const std::shared_ptr<dwio::common::WriterOptions>& writerOptions =
          nullptr) {
    std::vector<std::shared_ptr<const HiveColumnHandle>> inputColumns;
    inputColumns.reserve(outputRowType->size());
    for (int i = 0; i < outputRowType->size(); ++i) {
      bool isPartitionKey =
          std::find(
              partitionedBy.begin(),
              partitionedBy.end(),
              outputRowType->nameOf(i)) != partitionedBy.end();
      inputColumns.emplace_back(std::make_shared<HiveColumnHandle>(
          outputRowType->nameOf(i),
          isPartitionKey ? HiveColumnHandle::ColumnType::kPartitionKey
                         : HiveColumnHandle::ColumnType::kRegular,
          outputRowType->childAt(i),
          outputRowType->childAt(i)));
    }

    auto locationHandle = makeLocationHandle(
        outputDirectoryPath,
        std::nullopt,
        connector::hive::LocationHandle::TableType::kNew);

    return std::make_shared<HiveInsertTableHandle>(
        inputColumns,
        locationHandle,
        fileFormat,
        bucketProperty,
        CompressionKind::CompressionKind_ZSTD,
        std::unordered_map<std::string, std::string>{},
        writerOptions,
        compactProperty);
  }

  std::shared_ptr<HiveDataSink> createDataSinkWithCompact(
      const RowTypePtr& rowType,
      const std::string& outputDirectoryPath,
      const std::shared_ptr<connector::hive::CompactProperty>& compactProperty,
      dwio::common::FileFormat fileFormat = dwio::common::FileFormat::DWRF,
      const std::vector<std::string>& partitionedBy = {},
      const std::shared_ptr<connector::hive::HiveBucketProperty>&
          bucketProperty = nullptr,
      const std::shared_ptr<dwio::common::WriterOptions>& writerOptions =
          nullptr) {
    return std::make_shared<HiveDataSink>(
        rowType,
        createHiveInsertTableHandleWithCompact(
            rowType,
            outputDirectoryPath,
            compactProperty,
            fileFormat,
            partitionedBy,
            bucketProperty,
            writerOptions),
        connectorQueryCtx_.get(),
        CommitStrategy::kNoCommit,
        connectorConfig_);
  }

  std::vector<std::string> listFiles(const std::string& dirPath) {
    std::vector<std::string> files;
    for (auto& dirEntry : fs::recursive_directory_iterator(dirPath)) {
      if (dirEntry.is_regular_file()) {
        files.push_back(dirEntry.path().string());
      }
    }
    return files;
  }

  void verifyWrittenData(const std::string& dirPath, int32_t numFiles = 1) {
    const std::vector<std::string> filePaths = listFiles(dirPath);
    ASSERT_EQ(filePaths.size(), numFiles);
    std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
    std::for_each(filePaths.begin(), filePaths.end(), [&](auto filePath) {
      splits.push_back(makeHiveConnectorSplit(filePath));
    });
    HiveConnectorTestBase::assertQuery(
        PlanBuilder().tableScan(rowType_).planNode(),
        splits,
        fmt::format("SELECT * FROM tmp"));
  }

  void setConnectorQueryContext(
      std::unique_ptr<ConnectorQueryCtx> connectorQueryCtx) {
    connectorQueryCtx_ = std::move(connectorQueryCtx);
  }

  const std::shared_ptr<memory::MemoryPool> pool_ =
      memory::memoryManager()->addLeafPool();

  std::shared_ptr<memory::MemoryPool> root_;
  std::shared_ptr<memory::MemoryPool> opPool_;
  std::shared_ptr<memory::MemoryPool> connectorPool_;
  RowTypePtr rowType_;
  RowTypePtr compactRowType_;
  std::shared_ptr<config::ConfigBase> connectorSessionProperties_ =
      std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>(),
          /*mutable=*/true);
  std::unique_ptr<ConnectorQueryCtx> connectorQueryCtx_;
  std::shared_ptr<HiveConfig> connectorConfig_ =
      std::make_shared<HiveConfig>(std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>()));
  std::unique_ptr<folly::IOThreadPoolExecutor> spillExecutor_;

  // Helper function to create CompactProperty with fileNameGenerator for tests
  std::shared_ptr<connector::hive::CompactProperty>
  createCompactPropertyWithGenerator(
      bool enabled,
      uint64_t fileSizeThreshold,
      const std::vector<std::string>& sortKeyColumns,
      const std::string& timeColumn) {
    auto generator = [](int index) -> std::string {
      return fmt::format("compact_file_{:04d}", index);
    };

    return std::make_shared<connector::hive::CompactProperty>(
        enabled, fileSizeThreshold, sortKeyColumns, timeColumn, generator);
  }
};

TEST_F(HiveDataSinkTest, hiveSortingColumn) {
  struct {
    std::string sortColumn;
    core::SortOrder sortOrder;
    bool badColumn;
    std::string exceptionString;
    std::string expectedToString;

    std::string debugString() const {
      return fmt::format(
          "sortColumn {} sortOrder {} badColumn {} exceptionString {} expectedToString {}",
          sortColumn,
          sortOrder.toString(),
          badColumn,
          exceptionString,
          expectedToString);
    }
  } testSettings[] = {
      {"a",
       core::SortOrder{true, true},
       false,
       "",
       "[COLUMN[a] ORDER[ASC NULLS FIRST]]"},
      {"a",
       core::SortOrder{false, false},
       false,
       "",
       "[COLUMN[a] ORDER[DESC NULLS LAST]]"},
      {"",
       core::SortOrder{true, true},
       true,
       "hive sort column must be set",
       ""},
      {"a",
       core::SortOrder{true, false},
       true,
       "Bad hive sort order: [COLUMN[a] ORDER[ASC NULLS LAST]]",
       ""},
      {"a",
       core::SortOrder{false, true},
       true,
       "Bad hive sort order: [COLUMN[a] ORDER[DESC NULLS FIRST]]",
       ""}};
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    if (testData.badColumn) {
      VELOX_ASSERT_THROW(
          HiveSortingColumn(testData.sortColumn, testData.sortOrder),
          testData.exceptionString);
      continue;
    }
    const HiveSortingColumn column(testData.sortColumn, testData.sortOrder);
    ASSERT_EQ(column.sortOrder(), testData.sortOrder);
    ASSERT_EQ(column.sortColumn(), testData.sortColumn);
    ASSERT_EQ(column.toString(), testData.expectedToString);
    auto obj = column.serialize();
    const auto deserializedColumn = HiveSortingColumn::deserialize(obj, pool());
    ASSERT_EQ(obj, deserializedColumn->serialize());
  }
}

TEST_F(HiveDataSinkTest, hiveBucketProperty) {
  const std::vector<std::string> columns = {"a", "b", "c"};
  const std::vector<TypePtr> types = {INTEGER(), VARBINARY(), BIGINT()};
  const std::vector<std::shared_ptr<const HiveSortingColumn>> sortedColumns = {
      std::make_shared<HiveSortingColumn>("d", core::SortOrder{false, false}),
      std::make_shared<HiveSortingColumn>("e", core::SortOrder{false, false}),
      std::make_shared<HiveSortingColumn>("f", core::SortOrder{true, true})};
  struct {
    HiveBucketProperty::Kind kind;
    std::vector<std::string> bucketedBy;
    std::vector<TypePtr> bucketedTypes;
    uint32_t bucketCount;
    std::vector<std::shared_ptr<const HiveSortingColumn>> sortedBy;
    bool badProperty;
    std::string exceptionString;
    std::string expectedToString;
  } testSettings[] = {
      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0]},
       {types[0], types[1]},
       4,
       {},
       true,
       "The number of hive bucket columns and types do not match",
       ""},
      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0], columns[1]},
       {types[0]},
       4,
       {},
       true,
       "The number of hive bucket columns and types do not match",
       ""},
      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0], columns[1]},
       {},
       4,
       {},
       true,
       "The number of hive bucket columns and types do not match",
       ""},
      {HiveBucketProperty::Kind::kPrestoNative,
       {},
       {types[0]},
       4,
       {},
       true,
       "Hive bucket columns must be set",
       ""},
      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0], columns[1]},
       {types[0], types[1]},
       0,
       {},
       true,
       "Hive bucket count can't be zero",
       ""},
      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0], columns[1]},
       {types[0], types[1]},
       4,
       {},
       false,
       "",
       "\nHiveBucketProperty[<PRESTO_NATIVE 4>\n"
       "\tBucket Columns:\n"
       "\t\ta\n"
       "\t\tb\n"
       "\tBucket Types:\n"
       "\t\tINTEGER\n"
       "\t\tVARBINARY\n"
       "]\n"},
      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0]},
       {types[0]},
       4,
       {},
       false,
       "",
       "\nHiveBucketProperty[<PRESTO_NATIVE 4>\n\tBucket Columns:\n\t\ta\n\tBucket Types:\n\t\tINTEGER\n]\n"},
      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0], columns[1]},
       {types[0], types[1]},
       4,
       {{sortedColumns[0]}},
       false,
       "",
       "\nHiveBucketProperty[<PRESTO_NATIVE 4>\n\tBucket Columns:\n\t\ta\n\t\tb\n\tBucket Types:\n\t\tINTEGER\n\t\tVARBINARY\n\tSortedBy Columns:\n\t\t[COLUMN[d] ORDER[DESC NULLS LAST]]\n]\n"},
      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0]},
       {types[0]},
       4,
       {{sortedColumns[0], sortedColumns[2]}},
       false,
       "",
       "\nHiveBucketProperty[<PRESTO_NATIVE 4>\n\tBucket Columns:\n\t\ta\n\tBucket Types:\n\t\tINTEGER\n\tSortedBy Columns:\n\t\t[COLUMN[d] ORDER[DESC NULLS LAST]]\n\t\t[COLUMN[f] ORDER[ASC NULLS FIRST]]\n]\n"},

      {HiveBucketProperty::Kind::kPrestoNative,
       {columns[0]},
       {types[0], types[1]},
       4,
       {},
       true,
       "The number of hive bucket columns and types do not match",
       ""},
      {HiveBucketProperty::Kind::kHiveCompatible,
       {columns[0], columns[1]},
       {types[0]},
       4,
       {},
       true,
       "The number of hive bucket columns and types do not match",
       ""},
      {HiveBucketProperty::Kind::kHiveCompatible,
       {columns[0], columns[1]},
       {},
       4,
       {},
       true,
       "The number of hive bucket columns and types do not match",
       ""},
      {HiveBucketProperty::Kind::kHiveCompatible,
       {},
       {types[0]},
       4,
       {},
       true,
       "Hive bucket columns must be set",
       ""},
      {HiveBucketProperty::Kind::kHiveCompatible,
       {columns[0], columns[1]},
       {types[0], types[1]},
       0,
       {},
       true,
       "Hive bucket count can't be zero",
       ""},
      {HiveBucketProperty::Kind::kHiveCompatible,
       {columns[0], columns[1]},
       {types[0], types[1]},
       4,
       {},
       false,
       "",
       "\nHiveBucketProperty[<HIVE_COMPATIBLE 4>\n"
       "\tBucket Columns:\n"
       "\t\ta\n"
       "\t\tb\n"
       "\tBucket Types:\n"
       "\t\tINTEGER\n"
       "\t\tVARBINARY\n"
       "]\n"},
      {HiveBucketProperty::Kind::kHiveCompatible,
       {columns[0]},
       {types[0]},
       4,
       {},
       false,
       "",
       "\nHiveBucketProperty[<HIVE_COMPATIBLE 4>\n\tBucket Columns:\n\t\ta\n\tBucket Types:\n\t\tINTEGER\n]\n"},
      {HiveBucketProperty::Kind::kHiveCompatible,
       {columns[0], columns[1]},
       {types[0], types[1]},
       4,
       {{sortedColumns[0]}},
       false,
       "",
       "\nHiveBucketProperty[<HIVE_COMPATIBLE 4>\n\tBucket Columns:\n\t\ta\n\t\tb\n\tBucket Types:\n\t\tINTEGER\n\t\tVARBINARY\n\tSortedBy Columns:\n\t\t[COLUMN[d] ORDER[DESC NULLS LAST]]\n]\n"},
      {HiveBucketProperty::Kind::kHiveCompatible,
       {columns[0]},
       {types[0]},
       4,
       {{sortedColumns[0], sortedColumns[2]}},
       false,
       "",
       "\nHiveBucketProperty[<HIVE_COMPATIBLE 4>\n\tBucket Columns:\n\t\ta\n\tBucket Types:\n\t\tINTEGER\n\tSortedBy Columns:\n\t\t[COLUMN[d] ORDER[DESC NULLS LAST]]\n\t\t[COLUMN[f] ORDER[ASC NULLS FIRST]]\n]\n"},
  };
  for (const auto& testData : testSettings) {
    if (testData.badProperty) {
      VELOX_ASSERT_THROW(
          HiveBucketProperty(
              testData.kind,
              testData.bucketCount,
              testData.bucketedBy,
              testData.bucketedTypes,
              testData.sortedBy),
          testData.exceptionString);
      continue;
    }
    HiveBucketProperty hiveProperty(
        testData.kind,
        testData.bucketCount,
        testData.bucketedBy,
        testData.bucketedTypes,
        testData.sortedBy);
    ASSERT_EQ(hiveProperty.kind(), testData.kind);
    ASSERT_EQ(hiveProperty.sortedBy(), testData.sortedBy);
    ASSERT_EQ(hiveProperty.bucketedBy(), testData.bucketedBy);
    ASSERT_EQ(hiveProperty.bucketedTypes(), testData.bucketedTypes);
    ASSERT_EQ(hiveProperty.toString(), testData.expectedToString);

    auto obj = hiveProperty.serialize();
    const auto deserializedProperty =
        HiveBucketProperty::deserialize(obj, pool());
    ASSERT_EQ(obj, deserializedProperty->serialize());
  }
}

TEST_F(HiveDataSinkTest, basic) {
  const auto outputDirectory = TempDirectoryPath::create();
  auto dataSink = createDataSink(rowType_, outputDirectory->getPath());
  auto stats = dataSink->stats();
  ASSERT_TRUE(stats.empty()) << stats.toString();
  ASSERT_EQ(
      stats.toString(),
      "numWrittenBytes 0B numWrittenFiles 0 spillRuns[0] spilledInputBytes[0B] "
      "spilledBytes[0B] spilledRows[0] spilledPartitions[0] spilledFiles[0] "
      "spillFillTimeNanos[0ns] spillSortTimeNanos[0ns] spillExtractVectorTime[0ns] spillSerializationTimeNanos[0ns] "
      "spillWrites[0] spillFlushTimeNanos[0ns] spillWriteTimeNanos[0ns] "
      "maxSpillExceededLimitCount[0] spillReadBytes[0B] spillReads[0] "
      "spillReadTimeNanos[0ns] spillReadDeserializationTimeNanos[0ns]");

  const int numBatches = 10;
  const auto vectors = createVectors(500, numBatches);
  for (const auto& vector : vectors) {
    dataSink->appendData(vector);
  }
  stats = dataSink->stats();
  ASSERT_FALSE(stats.empty());
  ASSERT_GT(stats.numWrittenBytes, 0);
  ASSERT_EQ(stats.numWrittenFiles, 0);
  ASSERT_TRUE(dataSink->finish());
  ASSERT_TRUE(dataSink->finish());
  const auto partitions = dataSink->close();
  stats = dataSink->stats();
  ASSERT_FALSE(stats.empty());
  ASSERT_EQ(partitions.size(), 1);

  createDuckDbTable(vectors);
  verifyWrittenData(outputDirectory->getPath());
}

TEST_F(HiveDataSinkTest, basicBucket) {
  const auto outputDirectory = TempDirectoryPath::create();

  const int32_t numBuckets = 4;
  auto bucketProperty = std::make_shared<HiveBucketProperty>(
      HiveBucketProperty::Kind::kHiveCompatible,
      numBuckets,
      std::vector<std::string>{"c0"},
      std::vector<TypePtr>{BIGINT()},
      std::vector<std::shared_ptr<const HiveSortingColumn>>{
          std::make_shared<HiveSortingColumn>(
              "c1", core::SortOrder{false, false})});
  connectorSessionProperties_->set(
      HiveConfig::kSortWriterFinishTimeSliceLimitMsSession, "1");
  auto dataSink = createDataSink(
      rowType_,
      outputDirectory->getPath(),
      dwio::common::FileFormat::DWRF,
      {},
      bucketProperty);
  auto stats = dataSink->stats();
  ASSERT_TRUE(stats.empty()) << stats.toString();
  ASSERT_EQ(
      stats.toString(),
      "numWrittenBytes 0B numWrittenFiles 0 spillRuns[0] spilledInputBytes[0B] "
      "spilledBytes[0B] spilledRows[0] spilledPartitions[0] spilledFiles[0] "
      "spillFillTimeNanos[0ns] spillSortTimeNanos[0ns] spillExtractVectorTime[0ns] spillSerializationTimeNanos[0ns] "
      "spillWrites[0] spillFlushTimeNanos[0ns] spillWriteTimeNanos[0ns] "
      "maxSpillExceededLimitCount[0] spillReadBytes[0B] spillReads[0] "
      "spillReadTimeNanos[0ns] spillReadDeserializationTimeNanos[0ns]");

  const int numBatches = 10;
  const auto vectors = createVectors(500, numBatches);
  for (const auto& vector : vectors) {
    dataSink->appendData(vector);
  }
  stats = dataSink->stats();
  ASSERT_FALSE(stats.empty());
  ASSERT_GT(stats.numWrittenBytes, 0);
  ASSERT_EQ(stats.numWrittenFiles, 0);
  VELOX_ASSERT_THROW(
      dataSink->close(), "Unexpected state transition from RUNNING to CLOSED");
  while (!dataSink->finish()) {
  }
  const auto partitions = dataSink->close();
  stats = dataSink->stats();
  ASSERT_FALSE(stats.empty());
  ASSERT_EQ(partitions.size(), numBuckets);

  createDuckDbTable(vectors);
  verifyWrittenData(outputDirectory->getPath(), numBuckets);
}

TEST_F(HiveDataSinkTest, close) {
  for (bool empty : {true, false}) {
    SCOPED_TRACE(fmt::format("Data sink is empty: {}", empty));
    const auto outputDirectory = TempDirectoryPath::create();
    auto dataSink = createDataSink(rowType_, outputDirectory->getPath());

    auto vectors = createVectors(500, 1);

    if (!empty) {
      dataSink->appendData(vectors[0]);
      ASSERT_GT(dataSink->stats().numWrittenBytes, 0);
    } else {
      ASSERT_EQ(dataSink->stats().numWrittenBytes, 0);
    }
    ASSERT_TRUE(dataSink->finish());
    const auto partitions = dataSink->close();
    // Can't append after close.
    VELOX_ASSERT_THROW(
        dataSink->appendData(vectors.back()), "Hive data sink is not running");
    VELOX_ASSERT_THROW(
        dataSink->close(), "Unexpected state transition from CLOSED to CLOSED");
    VELOX_ASSERT_THROW(
        dataSink->abort(),
        "Unexpected state transition from CLOSED to ABORTED");

    const auto stats = dataSink->stats();
    if (!empty) {
      ASSERT_EQ(partitions.size(), 1);
      ASSERT_GT(stats.numWrittenBytes, 0);
      createDuckDbTable(vectors);
      verifyWrittenData(outputDirectory->getPath());
    } else {
      ASSERT_TRUE(partitions.empty());
      ASSERT_EQ(stats.numWrittenBytes, 0);
    }
  }
}

TEST_F(HiveDataSinkTest, abort) {
  for (bool empty : {true, false}) {
    SCOPED_TRACE(fmt::format("Data sink is empty: {}", empty));
    const auto outputDirectory = TempDirectoryPath::create();
    auto dataSink = createDataSink(rowType_, outputDirectory->getPath());

    auto vectors = createVectors(1, 1);
    int initialBytes = 0;
    if (!empty) {
      dataSink->appendData(vectors.back());
      initialBytes = dataSink->stats().numWrittenBytes;
      ASSERT_GT(initialBytes, 0);
    } else {
      initialBytes = dataSink->stats().numWrittenBytes;
      ASSERT_EQ(initialBytes, 0);
    }
    dataSink->abort();
    const auto stats = dataSink->stats();
    ASSERT_TRUE(stats.empty());
    // Can't close after abort.
    VELOX_ASSERT_THROW(
        dataSink->close(),
        "Unexpected state transition from ABORTED to CLOSED");
    VELOX_ASSERT_THROW(
        dataSink->abort(),
        "Unexpected state transition from ABORTED to ABORTED");
    // Can't append after abort.
    VELOX_ASSERT_THROW(
        dataSink->appendData(vectors.back()), "Hive data sink is not running");
  }
}

DEBUG_ONLY_TEST_F(HiveDataSinkTest, memoryReclaim) {
  const int numBatches = 200;
  auto vectors = createVectors(500, 200);

  struct {
    dwio::common::FileFormat format;
    bool sortWriter;
    bool writerSpillEnabled;
    uint64_t writerFlushThreshold;
    bool expectedWriterReclaimEnabled;
    bool expectedWriterReclaimed;

    std::string debugString() const {
      return fmt::format(
          "format: {}, sortWriter: {}, writerSpillEnabled: {}, writerFlushThreshold: {}, expectedWriterReclaimEnabled: {}, expectedWriterReclaimed: {}",
          dwio::common::toString(format),
          sortWriter,
          writerSpillEnabled,
          succinctBytes(writerFlushThreshold),
          expectedWriterReclaimEnabled,
          expectedWriterReclaimed);
    }
  } testSettings[] = {
      {dwio::common::FileFormat::DWRF, true, true, 1 << 30, true, true},
      {dwio::common::FileFormat::DWRF, true, true, 1, true, true},
      {dwio::common::FileFormat::DWRF, true, false, 1 << 30, false, false},
      {dwio::common::FileFormat::DWRF, true, false, 1, false, false},
      {dwio::common::FileFormat::DWRF, false, true, 1 << 30, true, false},
      {dwio::common::FileFormat::DWRF, false, true, 1, true, true},
      {dwio::common::FileFormat::DWRF, false, false, 1 << 30, false, false},
      {dwio::common::FileFormat::DWRF, false, false, 1, false, false},
  // Add Parquet with https://github.com/facebookincubator/velox/issues/5560
#if 0
      {dwio::common::FileFormat::PARQUET, true, true, 1 << 30, false, false},
      {dwio::common::FileFormat::PARQUET, true, true, 1, false, false},
      {dwio::common::FileFormat::PARQUET, true, false, 1 << 30, false, false},
      {dwio::common::FileFormat::PARQUET, true, false, 1, false, false},
      {dwio::common::FileFormat::PARQUET, false, true, 1 << 30, false, false},
      {dwio::common::FileFormat::PARQUET, false, true, 1, false, false},
      {dwio::common::FileFormat::PARQUET, false, false, 1 << 30, false, false},
      {dwio::common::FileFormat::PARQUET, false, false, 1, false, false}
#endif
  };
  SCOPED_TESTVALUE_SET(
      "facebook::velox::dwrf::Writer::MemoryReclaimer::reclaimableBytes",
      std::function<void(dwrf::Writer*)>([&](dwrf::Writer* writer) {
        // Release before reclaim to make it not able to reclaim from reserved
        // memory.
        writer->getContext().releaseMemoryReservation();
      }));
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());
    setupMemoryPools();

    const auto outputDirectory = TempDirectoryPath::create();
    std::shared_ptr<HiveBucketProperty> bucketProperty;
    std::vector<std::string> partitionBy;
    if (testData.sortWriter) {
      partitionBy = {"c6"};
      bucketProperty = std::make_shared<HiveBucketProperty>(
          HiveBucketProperty::Kind::kHiveCompatible,
          4,
          std::vector<std::string>{"c0"},
          std::vector<TypePtr>{BIGINT()},
          std::vector<std::shared_ptr<const HiveSortingColumn>>{
              std::make_shared<HiveSortingColumn>(
                  "c1", core::SortOrder{false, false})});
    }
    std::shared_ptr<TempDirectoryPath> spillDirectory;
    std::unique_ptr<SpillConfig> spillConfig;
    if (testData.writerSpillEnabled) {
      spillDirectory = exec::test::TempDirectoryPath::create();
      spillConfig = getSpillConfig(
          spillDirectory->getPath(), testData.writerFlushThreshold);
      auto connectorQueryCtx = std::make_unique<connector::ConnectorQueryCtx>(
          opPool_.get(),
          connectorPool_.get(),
          connectorSessionProperties_.get(),
          spillConfig.get(),
          exec::test::defaultPrefixSortConfig(),
          nullptr,
          nullptr,
          "query.HiveDataSinkTest",
          "task.HiveDataSinkTest",
          "planNodeId.HiveDataSinkTest",
          0,
          "");
      setConnectorQueryContext(std::move(connectorQueryCtx));
    } else {
      auto connectorQueryCtx = std::make_unique<connector::ConnectorQueryCtx>(
          opPool_.get(),
          connectorPool_.get(),
          connectorSessionProperties_.get(),
          nullptr,
          exec::test::defaultPrefixSortConfig(),
          nullptr,
          nullptr,
          "query.HiveDataSinkTest",
          "task.HiveDataSinkTest",
          "planNodeId.HiveDataSinkTest",
          0,
          "");
      setConnectorQueryContext(std::move(connectorQueryCtx));
    }

    auto dataSink = createDataSink(
        rowType_,
        outputDirectory->getPath(),
        testData.format,
        partitionBy,
        bucketProperty);
    auto* hiveDataSink = static_cast<HiveDataSink*>(dataSink.get());
    ASSERT_EQ(
        hiveDataSink->canReclaim(), testData.expectedWriterReclaimEnabled);
    for (int i = 0; i < numBatches; ++i) {
      dataSink->appendData(vectors[i]);
    }
    memory::MemoryArbitrator::Stats oldStats =
        memory::memoryManager()->arbitrator()->stats();
    uint64_t reclaimableBytes{0};
    if (testData.expectedWriterReclaimed) {
      reclaimableBytes = root_->reclaimableBytes().value();
      ASSERT_GT(reclaimableBytes, 0);
      memory::testingRunArbitration();
      memory::MemoryArbitrator::Stats curStats =
          memory::memoryManager()->arbitrator()->stats();
      ASSERT_GT(curStats.reclaimedUsedBytes - oldStats.reclaimedUsedBytes, 0);
      // We expect dwrf writer set numNonReclaimableAttempts counter.
      ASSERT_LE(
          curStats.numNonReclaimableAttempts -
              oldStats.numNonReclaimableAttempts,
          1);
    } else {
      ASSERT_FALSE(root_->reclaimableBytes().has_value());
      memory::testingRunArbitration();
      memory::MemoryArbitrator::Stats curStats =
          memory::memoryManager()->arbitrator()->stats();
      ASSERT_EQ(curStats.reclaimedUsedBytes - oldStats.reclaimedUsedBytes, 0);
    }
    while (!dataSink->finish()) {
    }
    const auto partitions = dataSink->close();
    if (testData.sortWriter && testData.expectedWriterReclaimed) {
      ASSERT_FALSE(dataSink->stats().spillStats.empty());
    } else {
      ASSERT_TRUE(dataSink->stats().spillStats.empty());
    }
    ASSERT_GE(partitions.size(), 1);
  }
}

TEST_F(HiveDataSinkTest, memoryReclaimAfterClose) {
  const int numBatches = 10;
  const auto vectors = createVectors(500, 10);

  struct {
    dwio::common::FileFormat format;
    bool sortWriter;
    bool writerSpillEnabled;
    bool close;
    bool expectedWriterReclaimEnabled;

    std::string debugString() const {
      return fmt::format(
          "format: {}, sortWriter: {}, writerSpillEnabled: {}, close: {}, expectedWriterReclaimEnabled: {}",
          dwio::common::toString(format),
          sortWriter,
          writerSpillEnabled,
          close,
          expectedWriterReclaimEnabled);
    }
  } testSettings[] = {
      {dwio::common::FileFormat::DWRF, true, true, true, true},
      {dwio::common::FileFormat::DWRF, true, false, true, false},
      {dwio::common::FileFormat::DWRF, true, true, false, true},
      {dwio::common::FileFormat::DWRF, true, false, false, false},
      {dwio::common::FileFormat::DWRF, false, true, true, true},
      {dwio::common::FileFormat::DWRF, false, false, true, false},
      {dwio::common::FileFormat::DWRF, false, true, false, true},
      {dwio::common::FileFormat::DWRF, false, false, false, false}
      // Add parquet file format after fix
      // https://github.com/facebookincubator/velox/issues/5560
  };
  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    std::unordered_map<std::string, std::string> connectorConfig;
    // Always allow memory reclaim from the file writer/
    connectorConfig.emplace(
        "file_writer_flush_threshold_bytes", folly::to<std::string>(0));
    // Avoid internal the stripe flush while data write.
    connectorConfig.emplace("hive.orc.writer.stripe-max-size", "1GB");
    connectorConfig.emplace("hive.orc.writer.dictionary-max-memory", "1GB");

    connectorConfig_ = std::make_shared<HiveConfig>(
        std::make_shared<config::ConfigBase>(std::move(connectorConfig)));
    const auto outputDirectory = TempDirectoryPath::create();
    std::shared_ptr<HiveBucketProperty> bucketProperty;
    std::vector<std::string> partitionBy;
    if (testData.sortWriter) {
      partitionBy = {"c6"};
      bucketProperty = std::make_shared<HiveBucketProperty>(
          HiveBucketProperty::Kind::kHiveCompatible,
          4,
          std::vector<std::string>{"c0"},
          std::vector<TypePtr>{BIGINT()},
          std::vector<std::shared_ptr<const HiveSortingColumn>>{
              std::make_shared<HiveSortingColumn>(
                  "c1", core::SortOrder{false, false})});
    }
    std::shared_ptr<TempDirectoryPath> spillDirectory;
    std::unique_ptr<SpillConfig> spillConfig;
    if (testData.writerSpillEnabled) {
      spillDirectory = exec::test::TempDirectoryPath::create();
      spillConfig = getSpillConfig(spillDirectory->getPath(), 0);
      auto connectorQueryCtx = std::make_unique<connector::ConnectorQueryCtx>(
          opPool_.get(),
          connectorPool_.get(),
          connectorSessionProperties_.get(),
          spillConfig.get(),
          exec::test::defaultPrefixSortConfig(),
          nullptr,
          nullptr,
          "query.HiveDataSinkTest",
          "task.HiveDataSinkTest",
          "planNodeId.HiveDataSinkTest",
          0,
          "");
      setConnectorQueryContext(std::move(connectorQueryCtx));
    } else {
      auto connectorQueryCtx = std::make_unique<connector::ConnectorQueryCtx>(
          opPool_.get(),
          connectorPool_.get(),
          connectorSessionProperties_.get(),
          nullptr,
          exec::test::defaultPrefixSortConfig(),
          nullptr,
          nullptr,
          "query.HiveDataSinkTest",
          "task.HiveDataSinkTest",
          "planNodeId.HiveDataSinkTest",
          0,
          "");
      setConnectorQueryContext(std::move(connectorQueryCtx));
    }

    auto dataSink = createDataSink(
        rowType_,
        outputDirectory->getPath(),
        testData.format,
        partitionBy,
        bucketProperty);
    auto* hiveDataSink = static_cast<HiveDataSink*>(dataSink.get());
    ASSERT_EQ(
        hiveDataSink->canReclaim(), testData.expectedWriterReclaimEnabled);

    for (int i = 0; i < numBatches; ++i) {
      dataSink->appendData(vectors[i]);
    }
    if (testData.close) {
      ASSERT_TRUE(dataSink->finish());
      const auto partitions = dataSink->close();
      ASSERT_GE(partitions.size(), 1);
    } else {
      dataSink->abort();
      ASSERT_TRUE(dataSink->stats().empty());
    }

    memory::MemoryReclaimer::Stats stats;
    uint64_t reclaimableBytes{0};
    if (testData.expectedWriterReclaimEnabled) {
      reclaimableBytes = root_->reclaimableBytes().value();
      if (testData.close) {
        // NOTE: file writer might not release all the memory on close
        // immediately.
        ASSERT_GE(reclaimableBytes, 0);
      } else {
        ASSERT_EQ(reclaimableBytes, 0);
      }
    } else {
      ASSERT_FALSE(root_->reclaimableBytes().has_value());
    }
    ASSERT_EQ(root_->reclaim(1L << 30, 0, stats), 0);
    ASSERT_EQ(stats.reclaimExecTimeUs, 0);
    ASSERT_EQ(stats.reclaimedBytes, 0);
    if (testData.expectedWriterReclaimEnabled) {
      ASSERT_GE(stats.numNonReclaimableAttempts, 0);
    } else {
      ASSERT_EQ(stats.numNonReclaimableAttempts, 0);
    }
  }
}

DEBUG_ONLY_TEST_F(HiveDataSinkTest, sortWriterAbortDuringFinish) {
  const auto outputDirectory = TempDirectoryPath::create();
  const int32_t numBuckets = 4;
  auto bucketProperty = std::make_shared<HiveBucketProperty>(
      HiveBucketProperty::Kind::kHiveCompatible,
      numBuckets,
      std::vector<std::string>{"c0"},
      std::vector<TypePtr>{BIGINT()},
      std::vector<std::shared_ptr<const HiveSortingColumn>>{
          std::make_shared<HiveSortingColumn>(
              "c1", core::SortOrder{false, false})});
  connectorSessionProperties_->set(
      HiveConfig::kSortWriterFinishTimeSliceLimitMsSession, "1");
  connectorSessionProperties_->set(
      HiveConfig::kSortWriterMaxOutputRowsSession, "100");
  auto dataSink = createDataSink(
      rowType_,
      outputDirectory->getPath(),
      dwio::common::FileFormat::DWRF,
      {},
      bucketProperty);
  const int numBatches{10};
  const auto vectors = createVectors(500, numBatches);
  for (const auto& vector : vectors) {
    dataSink->appendData(vector);
  }

  std::atomic_int injectCount{0};
  SCOPED_TESTVALUE_SET(
      "facebook::velox::dwrf::Writer::write",
      std::function<void(dwrf::Writer*)>([&](dwrf::Writer* /*unused*/) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }));

  for (int i = 0;; ++i) {
    ASSERT_FALSE(dataSink->finish());
    if (i == 2) {
      dataSink->abort();
      break;
    }
  }
  const auto stats = dataSink->stats();
  ASSERT_TRUE(stats.empty());
}

TEST_F(HiveDataSinkTest, sortWriterMemoryReclaimDuringFinish) {
  const auto outputDirectory = TempDirectoryPath::create();
  const int32_t numBuckets = 4;
  auto bucketProperty = std::make_shared<HiveBucketProperty>(
      HiveBucketProperty::Kind::kHiveCompatible,
      numBuckets,
      std::vector<std::string>{"c0"},
      std::vector<TypePtr>{BIGINT()},
      std::vector<std::shared_ptr<const HiveSortingColumn>>{
          std::make_shared<HiveSortingColumn>(
              "c1", core::SortOrder{false, false})});
  std::shared_ptr<TempDirectoryPath> spillDirectory =
      exec::test::TempDirectoryPath::create();
  std::unique_ptr<SpillConfig> spillConfig =
      getSpillConfig(spillDirectory->getPath(), 1);
  connectorSessionProperties_->set(
      HiveConfig::kSortWriterFinishTimeSliceLimitMsSession, "1");
  connectorSessionProperties_->set(
      HiveConfig::kSortWriterMaxOutputRowsSession, "100");
  auto connectorQueryCtx = std::make_unique<connector::ConnectorQueryCtx>(
      opPool_.get(),
      connectorPool_.get(),
      connectorSessionProperties_.get(),
      spillConfig.get(),
      exec::test::defaultPrefixSortConfig(),
      nullptr,
      nullptr,
      "query.HiveDataSinkTest",
      "task.HiveDataSinkTest",
      "planNodeId.HiveDataSinkTest",
      0,
      "");
  setConnectorQueryContext(std::move(connectorQueryCtx));
  auto dataSink = createDataSink(
      rowType_,
      outputDirectory->getPath(),
      dwio::common::FileFormat::DWRF,
      {},
      bucketProperty);
  const int numBatches{10};
  const auto vectors = createVectors(500, numBatches);
  for (const auto& vector : vectors) {
    dataSink->appendData(vector);
  }

  for (int i = 0; !dataSink->finish(); ++i) {
    if (i == 2) {
      ASSERT_GT(root_->reclaimableBytes().value(), 0);
      const memory::MemoryArbitrator::Stats prevStats =
          memory::memoryManager()->arbitrator()->stats();
      memory::testingRunArbitration();
      memory::MemoryArbitrator::Stats curStats =
          memory::memoryManager()->arbitrator()->stats();
      ASSERT_GT(curStats.reclaimedUsedBytes - prevStats.reclaimedUsedBytes, 0);
    }
  }
  const auto partitions = dataSink->close();
  const auto stats = dataSink->stats();
  ASSERT_FALSE(stats.empty());
  ASSERT_EQ(partitions.size(), numBuckets);

  createDuckDbTable(vectors);
  verifyWrittenData(outputDirectory->getPath(), numBuckets);
}

DEBUG_ONLY_TEST_F(HiveDataSinkTest, sortWriterFailureTest) {
  auto vectors = createVectors(500, 10);

  const auto outputDirectory = TempDirectoryPath::create();
  const std::vector<std::string> partitionBy{"c6"};
  const auto bucketProperty = std::make_shared<HiveBucketProperty>(
      HiveBucketProperty::Kind::kHiveCompatible,
      4,
      std::vector<std::string>{"c0"},
      std::vector<TypePtr>{BIGINT()},
      std::vector<std::shared_ptr<const HiveSortingColumn>>{
          std::make_shared<HiveSortingColumn>(
              "c1", core::SortOrder{false, false})});
  const std::shared_ptr<TempDirectoryPath> spillDirectory =
      exec::test::TempDirectoryPath::create();
  std::unique_ptr<SpillConfig> spillConfig =
      getSpillConfig(spillDirectory->getPath(), 0);
  // Triggers the memory reservation in sort buffer.
  spillConfig->minSpillableReservationPct = 1'000;
  auto connectorQueryCtx = std::make_unique<connector::ConnectorQueryCtx>(
      opPool_.get(),
      connectorPool_.get(),
      connectorSessionProperties_.get(),
      spillConfig.get(),
      exec::test::defaultPrefixSortConfig(),
      nullptr,
      nullptr,
      "query.HiveDataSinkTest",
      "task.HiveDataSinkTest",
      "planNodeId.HiveDataSinkTest",
      0,
      "");
  setConnectorQueryContext(std::move(connectorQueryCtx));

  auto dataSink = createDataSink(
      rowType_,
      outputDirectory->getPath(),
      dwio::common::FileFormat::DWRF,
      partitionBy,
      bucketProperty);
  for (auto& vector : vectors) {
    dataSink->appendData(vector);
  }

  SCOPED_TESTVALUE_SET(
      "facebook::velox::dwrf::Writer::write",
      std::function<void(memory::MemoryPool*)>(
          [&](memory::MemoryPool* pool) { VELOX_FAIL("inject failure"); }));

  VELOX_ASSERT_THROW(dataSink->finish(), "inject failure");
}

TEST_F(HiveDataSinkTest, insertTableHandleToString) {
  const int32_t numBuckets = 4;
  auto bucketProperty = std::make_shared<HiveBucketProperty>(
      HiveBucketProperty::Kind::kHiveCompatible,
      numBuckets,
      std::vector<std::string>{"c5"},
      std::vector<TypePtr>{VARBINARY()},
      std::vector<std::shared_ptr<const HiveSortingColumn>>{
          std::make_shared<HiveSortingColumn>(
              "c5", core::SortOrder{false, false})});
  auto insertTableHandle = createHiveInsertTableHandle(
      rowType_,
      "/path/to/test",
      dwio::common::FileFormat::DWRF,
      {"c5", "c6"},
      bucketProperty);
  ASSERT_EQ(
      insertTableHandle->toString(),
      "HiveInsertTableHandle [dwrf zstd], [inputColumns: [ HiveColumnHandle [name: c0, columnType: Regular, dataType: BIGINT, requiredSubfields: [ ]] HiveColumnHandle [name: c1, columnType: Regular, dataType: INTEGER, requiredSubfields: [ ]] HiveColumnHandle [name: c2, columnType: Regular, dataType: SMALLINT, requiredSubfields: [ ]] HiveColumnHandle [name: c3, columnType: Regular, dataType: REAL, requiredSubfields: [ ]] HiveColumnHandle [name: c4, columnType: Regular, dataType: DOUBLE, requiredSubfields: [ ]] HiveColumnHandle [name: c5, columnType: PartitionKey, dataType: VARBINARY, requiredSubfields: [ ]] HiveColumnHandle [name: c6, columnType: PartitionKey, dataType: BOOLEAN, requiredSubfields: [ ]] HiveColumnHandle [name: c7, columnType: Regular, dataType: VARBINARY, requiredSubfields: [ ]] HiveColumnHandle [name: c8, columnType: Regular, dataType: VARBINARY, requiredSubfields: [ ]] HiveColumnHandle [name: c9, columnType: Regular, dataType: VARBINARY, requiredSubfields: [ ]] ], locationHandle: LocationHandle [targetPath: /path/to/test, writePath: /path/to/test, tableType: kNew,, bucketProperty: \nHiveBucketProperty[<HIVE_COMPATIBLE 4>\n\tBucket Columns:\n\t\tc5\n\tBucket Types:\n\t\tVARBINARY\n\tSortedBy Columns:\n\t\t[COLUMN[c5] ORDER[DESC NULLS LAST]]\n]\n]");
}

#ifdef VELOX_ENABLE_PARQUET
TEST_F(HiveDataSinkTest, flushPolicyWithParquet) {
  const auto outputDirectory = TempDirectoryPath::create();
  auto flushPolicyFactory = []() {
    return std::make_unique<parquet::DefaultFlushPolicy>(1234, 0);
  };
  auto writeOptions = std::make_shared<parquet::WriterOptions>();
  writeOptions->flushPolicyFactory = flushPolicyFactory;
  auto dataSink = createDataSink(
      rowType_,
      outputDirectory->getPath(),
      dwio::common::FileFormat::PARQUET,
      {},
      nullptr,
      writeOptions);

  const int numBatches = 10;
  const auto vectors = createVectors(500, numBatches);
  for (const auto& vector : vectors) {
    dataSink->appendData(vector);
  }
  ASSERT_TRUE(dataSink->finish());
  dataSink->close();

  dwio::common::ReaderOptions readerOpts{pool_.get()};
  const std::vector<std::string> filePaths =
      listFiles(outputDirectory->getPath());
  auto bufferedInput = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<LocalReadFile>(filePaths[0]), readerOpts.memoryPool());

  auto reader = std::make_unique<facebook::velox::parquet::ParquetReader>(
      std::move(bufferedInput), readerOpts);
  auto fileMeta = reader->fileMetaData();
  EXPECT_EQ(fileMeta.numRowGroups(), 10);
  EXPECT_EQ(fileMeta.rowGroup(0).numRows(), 500);
}
#endif

TEST_F(HiveDataSinkTest, flushPolicyWithDWRF) {
  const auto outputDirectory = TempDirectoryPath::create();
  auto flushPolicyFactory = []() {
    return std::make_unique<dwrf::DefaultFlushPolicy>(1234, 0);
  };

  auto writeOptions = std::make_shared<dwrf::WriterOptions>();
  writeOptions->flushPolicyFactory = flushPolicyFactory;
  auto dataSink = createDataSink(
      rowType_,
      outputDirectory->getPath(),
      dwio::common::FileFormat::DWRF,
      {},
      nullptr,
      writeOptions);

  const int numBatches = 10;
  const auto vectors = createVectors(500, numBatches);
  for (const auto& vector : vectors) {
    dataSink->appendData(vector);
  }
  ASSERT_TRUE(dataSink->finish());
  dataSink->close();

  dwio::common::ReaderOptions readerOpts{pool_.get()};
  const std::vector<std::string> filePaths =
      listFiles(outputDirectory->getPath());
  auto bufferedInput = std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<LocalReadFile>(filePaths[0]), readerOpts.memoryPool());

  auto reader = std::make_unique<facebook::velox::dwrf::DwrfReader>(
      readerOpts, std::move(bufferedInput));
  ASSERT_EQ(reader->getNumberOfStripes(), 10);
  ASSERT_EQ(reader->getRowsPerStripe()[0], 500);
}

// Compact Mode Tests
TEST_F(HiveDataSinkTest, compactProperty) {
  struct {
    bool enabled;
    uint64_t fileSizeThreshold;
    std::vector<std::string> sortKeyColumns;
    std::string timeColumn;
    bool hasCustomGenerator;
    std::string expectedToString;

    std::string debugString() const {
      return fmt::format(
          "enabled {} fileSizeThreshold {} sortKeyColumns [{}] timeColumn {} hasCustomGenerator {} expectedToString {}",
          enabled,
          fileSizeThreshold,
          folly::join(", ", sortKeyColumns),
          timeColumn,
          hasCustomGenerator,
          expectedToString);
    }
  } testSettings[] = {
      {true,
       1024,
       {"col1", "col2"},
       "time_col",
       false,
       "CompactProperty[enabled=true, fileSizeThreshold=1024, sortKeyColumns=[col1, col2], timeColumn=time_col]"},
      {false,
       2048,
       {"col1"},
       "time_col",
       false,
       "CompactProperty[enabled=false, fileSizeThreshold=2048, sortKeyColumns=[col1], timeColumn=time_col]"},
      {true,
       4096,
       {},
       "",
       false,
       "CompactProperty[enabled=true, fileSizeThreshold=4096, sortKeyColumns=[], timeColumn=]"},
      {true,
       8192,
       {"col1", "col2", "col3"},
       "timestamp",
       false,
       "CompactProperty[enabled=true, fileSizeThreshold=8192, sortKeyColumns=[col1, col2, col3], timeColumn=timestamp]"},
  };

  for (const auto& testData : testSettings) {
    SCOPED_TRACE(testData.debugString());

    std::shared_ptr<connector::hive::CompactProperty> property;
    if (testData.hasCustomGenerator) {
      auto generator = [](int index) {
        return fmt::format("custom_{}.parquet", index);
      };
      property = std::make_shared<connector::hive::CompactProperty>(
          testData.enabled,
          testData.fileSizeThreshold,
          testData.sortKeyColumns,
          testData.timeColumn,
          generator);
    } else {
      property = std::make_shared<connector::hive::CompactProperty>(
          testData.enabled,
          testData.fileSizeThreshold,
          testData.sortKeyColumns,
          testData.timeColumn);
    }

    EXPECT_EQ(property->enabled(), testData.enabled);
    EXPECT_EQ(property->fileSizeThreshold(), testData.fileSizeThreshold);
    EXPECT_EQ(property->sortKeyColumns(), testData.sortKeyColumns);
    EXPECT_EQ(property->timeColumn(), testData.timeColumn);
    EXPECT_EQ(
        property->hasCustomFileNameGenerator(), testData.hasCustomGenerator);
    EXPECT_EQ(property->toString(), testData.expectedToString);
  }
}

TEST_F(HiveDataSinkTest, compactPropertySerialization) {
  // Test serialization and deserialization of CompactProperty
  auto original = std::make_shared<connector::hive::CompactProperty>(
      true, // enabled
      1024 * 1024, // 1MB threshold
      std::vector<std::string>{"col1", "col2", "col3"}, // sort key columns
      "timestamp" // time column
  );

  // Serialize
  auto serialized = original->serialize();

  // Verify serialized structure
  EXPECT_EQ(serialized["name"].asString(), "CompactProperty");
  EXPECT_EQ(serialized["enabled"].asBool(), true);
  EXPECT_EQ(serialized["fileSizeThreshold"].asInt(), 1024 * 1024);
  EXPECT_EQ(serialized["timeColumn"].asString(), "timestamp");
  EXPECT_FALSE(serialized["hasCustomFileNameGenerator"].asBool());

  std::cout << "serialized: " << folly::toJson(serialized) << std::endl;

  // Deserialize
  auto deserialized =
      connector::hive::CompactProperty::deserialize(serialized, nullptr);

  // Verify all properties match
  EXPECT_EQ(original->enabled(), deserialized->enabled());
  EXPECT_EQ(original->fileSizeThreshold(), deserialized->fileSizeThreshold());
  EXPECT_EQ(original->sortKeyColumns(), deserialized->sortKeyColumns());
  EXPECT_EQ(original->timeColumn(), deserialized->timeColumn());

  // Custom generators should not be serialized
  EXPECT_FALSE(deserialized->hasCustomFileNameGenerator());
}

TEST_F(HiveDataSinkTest, compactPropertyWithCustomGenerator) {
  auto generator = [](int index) -> std::string {
    return fmt::format("custom_file_{:04d}.dwrf", index);
  };

  auto property = std::make_shared<connector::hive::CompactProperty>(
      true, // enabled
      512, // threshold
      std::vector<std::string>{"col1"}, // sort key columns
      "time_col", // time column
      generator);

  EXPECT_TRUE(property->hasCustomFileNameGenerator());
  EXPECT_EQ(property->fileNameGenerator()(5), "custom_file_0005.dwrf");

  // Serialization should not include the custom generator
  auto serialized = property->serialize();
  auto deserialized =
      connector::hive::CompactProperty::deserialize(serialized, nullptr);
  EXPECT_FALSE(deserialized->hasCustomFileNameGenerator());
}

TEST_F(HiveDataSinkTest, compactModeBasic) {
  setupMemoryPools();

  auto compactProperty = createCompactPropertyWithGenerator(
      true, // enabled
      5000, // fileSizeThreshold: Set to 5KB to work with our small flush policy
      std::vector<std::string>{
          "c5", "c7", "c8", "c9"}, // sort key columns (VARBINARY)
      "c0" // time column (BIGINT)
  );

  // Create custom Parquet writer options with small flush policy for testing
  auto parquetOptions = std::make_shared<parquet::WriterOptions>();

  // Create a custom flush policy with small thresholds for testing
  // This ensures that RowGroups are flushed frequently, updating ioStats
  auto flushPolicyFactory = []() {
    // Use small thresholds: 100 rows or 1KB to trigger frequent flushes
    return std::make_unique<parquet::DefaultFlushPolicy>(
        100, // rowsInRowGroup: very small for testing
        1024 // bytesInRowGroup: 1KB, very small for testing
    );
  };
  parquetOptions->flushPolicyFactory = flushPolicyFactory;

  auto dataSink = createDataSinkWithCompact(
      compactRowType_, // Use compact-specific row type
      "/tmp/test",
      compactProperty,
      dwio::common::FileFormat::PARQUET, // Use Parquet format
      {}, // partitionedBy
      nullptr, // bucketProperty
      parquetOptions); // Custom writer options with small flush policy
  EXPECT_TRUE(dataSink != nullptr);

  // Create test vectors - use moderate size to trigger multiple flushes
  auto vectors = createVectors(150, 5); // 5 vectors of 150 rows each

  // Append data - should trigger multiple file switches due to small threshold
  for (size_t i = 0; i < vectors.size(); ++i) {
    std::cout << "=== Appending vector " << (i + 1) << " with "
              << vectors[i]->size() << " rows ===" << std::endl;
    dataSink->appendData(vectors[i]);

    // Check stats after each append
    auto stats = dataSink->stats();
    std::cout << "After vector " << (i + 1)
              << ": numWrittenBytes=" << stats.numWrittenBytes
              << ", numWrittenFiles=" << stats.numWrittenFiles << std::endl;
  }

  EXPECT_TRUE(dataSink->finish());
  auto result = dataSink->close();

  std::cout << "=== Final Results ===" << std::endl;
  std::cout << "Number of output files: " << result.size() << std::endl;

  for (size_t i = 0; i < result.size(); ++i) {
    std::cout << "File " << i << " partition update:" << std::endl;
    std::cout << result[i] << std::endl;
  }

  // Should have created multiple partition updates due to file switches
  EXPECT_GT(result.size(), 1)
      << "Should create multiple files with small threshold, got "
      << result.size() << " files";

  // Verify partition updates contain compact stats
  for (size_t i = 0; i < result.size(); ++i) {
    auto updateJson = folly::parseJson(result[i]);
    EXPECT_TRUE(updateJson.count("compactStats") > 0)
        << "Partition update should contain compact stats";

    auto fileWriteInfo = updateJson["fileWriteInfos"];
    EXPECT_TRUE(fileWriteInfo.count("fileSize") > 0);
    EXPECT_TRUE(fileWriteInfo.count("targetFileName") > 0);
    EXPECT_TRUE(fileWriteInfo.count("writeFileName") > 0);
    auto targetFileName = fileWriteInfo["targetFileName"].asString();
    auto writeFileName = fileWriteInfo["writeFileName"].asString();
    EXPECT_EQ(targetFileName, writeFileName);
    EXPECT_EQ(targetFileName, fmt::format("compact_file_{:04d}.parquet", i));
    EXPECT_EQ(writeFileName, fmt::format("compact_file_{:04d}.parquet", i));

    EXPECT_TRUE(updateJson.count("rowCount") > 0)
        << "Partition update should contain rowCount";
    auto rowCount = updateJson["rowCount"];
    EXPECT_TRUE(rowCount.isInt());
    EXPECT_GE(rowCount.asInt(), 150); // 150 rows per vector
    std::cout << "rowCount: " << rowCount.asInt() << std::endl;

    auto partIndex = updateJson["part_index"];
    EXPECT_TRUE(partIndex.isInt());
    EXPECT_EQ(partIndex.asInt(), i);

    auto compactStats = updateJson["compactStats"];
    EXPECT_TRUE(compactStats.count("sortKeyStats") > 0);
    EXPECT_TRUE(compactStats.count("timeColumnStats") > 0);
    EXPECT_TRUE(compactStats.count("sortKeyColumns") > 0);
    EXPECT_TRUE(compactStats.count("timeColumn") > 0);

    // Verify new sortKeyStats structure (min/max arrays)
    auto sortKeyStats = compactStats["sortKeyStats"];
    if (!sortKeyStats.empty()) {
      EXPECT_TRUE(sortKeyStats.count("min") > 0)
          << "Should have min array in sortKeyStats";
      EXPECT_TRUE(sortKeyStats.count("max") > 0)
          << "Should have max array in sortKeyStats";

      auto minArray = sortKeyStats["min"];
      auto maxArray = sortKeyStats["max"];
      EXPECT_TRUE(minArray.isArray()) << "min should be an array";
      EXPECT_TRUE(maxArray.isArray()) << "max should be an array";
      EXPECT_EQ(minArray.size(), maxArray.size())
          << "min and max arrays should have same size";
      EXPECT_EQ(minArray.size(), 4);
      EXPECT_EQ(maxArray.size(), 4);
    }
  }
}

TEST_F(HiveDataSinkTest, compactModeDisabled) {
  setupMemoryPools();

  auto compactProperty = createCompactPropertyWithGenerator(
      false, // disabled
      1024, // threshold (should be ignored)
      std::vector<std::string>{
          "c7", "c8", "c9"}, // sort key columns (should be ignored)
      "c0" // time column (should be ignored)
  );

  auto dataSink = createDataSinkWithCompact(
      compactRowType_, // Use compact-specific row type
      "/tmp/test",
      compactProperty,
      dwio::common::FileFormat::PARQUET); // Use Parquet format

  auto vectors = createVectors(100, 3);
  for (const auto& vector : vectors) {
    dataSink->appendData(vector);
  }

  EXPECT_TRUE(dataSink->finish());
  auto result = dataSink->close();

  // Should create only one file since compact mode is disabled
  EXPECT_EQ(result.size(), 1)
      << "Should create single file when compact mode is disabled";

  // Should not contain compact stats when disabled
  auto updateJson = folly::parseJson(result[0]);
  EXPECT_FALSE(updateJson.count("compactStats") > 0)
      << "Should not contain compact stats when compact mode is disabled";
}

TEST_F(HiveDataSinkTest, compactModeWithLargeThreshold) {
  setupMemoryPools();

  auto compactProperty = createCompactPropertyWithGenerator(
      true, // enabled
      10 * 1024 * 1024, // 10MB - large threshold
      std::vector<std::string>{"c7", "c8", "c9"}, // sort key columns
      "c0" // time column
  );

  auto dataSink = createDataSinkWithCompact(
      compactRowType_, // Use compact-specific row type
      "/tmp/test",
      compactProperty,
      dwio::common::FileFormat::PARQUET); // Use Parquet format

  auto vectors = createVectors(100, 3);
  for (const auto& vector : vectors) {
    dataSink->appendData(vector);
  }

  EXPECT_TRUE(dataSink->finish());
  auto result = dataSink->close();

  // Should create only one file due to large threshold
  EXPECT_EQ(result.size(), 1)
      << "Should create single file with large threshold";

  // Should contain compact stats
  auto updateJson = folly::parseJson(result[0]);
  EXPECT_TRUE(updateJson.count("compactStats") > 0)
      << "Should contain compact stats even with single file";
}

TEST_F(HiveDataSinkTest, compactModeWithEmptySortKeys) {
  setupMemoryPools();

  auto compactProperty = createCompactPropertyWithGenerator(
      true, // enabled
      1024, // small threshold
      std::vector<std::string>{}, // empty sort key columns
      "c0" // time column
  );


  EXPECT_THROW(
      {
        auto dataSink = createDataSinkWithCompact(
            compactRowType_, // Use compact-specific row type
            "/tmp/test",
            compactProperty,
            dwio::common::FileFormat::PARQUET); // Use Parquet format
      },
      VeloxUserError);
}

TEST_F(HiveDataSinkTest, compactModeWithEmptyTimeColumn) {
  setupMemoryPools();

  auto compactProperty = createCompactPropertyWithGenerator(
      true, // enabled
      1024, // small threshold
      std::vector<std::string>{"c7", "c8", "c9"}, // sort key columns
      "" // empty time column
  );

  EXPECT_THROW(
      {
        auto dataSink = createDataSinkWithCompact(
            compactRowType_, // Use compact-specific row type
            "/tmp/test",
            compactProperty,
            dwio::common::FileFormat::PARQUET); // Use Parquet format
      },
      VeloxUserError);

}

TEST_F(HiveDataSinkTest, compactModeInvalidSortKeyColumn) {
  setupMemoryPools();

  auto compactProperty = createCompactPropertyWithGenerator(
      true, // enabled
      1024, // threshold
      std::vector<std::string>{"invalid_column"}, // invalid sort key column
      "c0" // time column
  );

  // Should throw when creating data sink due to invalid column
  EXPECT_THROW(
      {
        auto dataSink = createDataSinkWithCompact(
            compactRowType_, // Use compact-specific row type
            "/tmp/test",
            compactProperty,
            dwio::common::FileFormat::PARQUET); // Use Parquet format
      },
      std::exception)
      << "Should throw for invalid sort key column";
}

TEST_F(HiveDataSinkTest, compactModeInvalidTimeColumn) {
  setupMemoryPools();

  auto compactProperty = createCompactPropertyWithGenerator(
      true, // enabled
      1024, // threshold
      std::vector<std::string>{"c7", "c8", "c9"}, // sort key columns
      "invalid_time_column" // invalid time column
  );

  // Should throw when creating data sink due to invalid column
  EXPECT_THROW(
      {
        auto dataSink = createDataSinkWithCompact(
            compactRowType_, // Use compact-specific row type
            "/tmp/test",
            compactProperty,
            dwio::common::FileFormat::PARQUET); // Use Parquet format
      },
      std::exception)
      << "Should throw for invalid time column";
}

TEST_F(HiveDataSinkTest, compactModeWithPartitions) {
  setupMemoryPools();

  // Create partitioned table setup
  auto partitionedRowType =
      ROW({"c0", "c1", "partition_key"}, {BIGINT(), INTEGER(), VARBINARY()});

  std::vector<std::string> partitionedBy = {"partition_key"};

  auto compactProperty = std::make_shared<connector::hive::CompactProperty>(
      true, // enabled
      1024, // threshold
      std::vector<std::string>{"c0"}, // sort key columns
      "c1" // time column
  );

  // Should throw when trying to use compact mode with partitioned table
  EXPECT_THROW(
      {
        auto dataSink = createDataSinkWithCompact(
            partitionedRowType,
            "/tmp/test",
            compactProperty,
            dwio::common::FileFormat::DWRF,
            partitionedBy); // partitioned table
      },
      std::exception)
      << "Should throw when using compact mode with partitioned tables";
}

TEST_F(HiveDataSinkTest, compactModeWithBuckets) {
  setupMemoryPools();

  // Create bucketed table setup
  auto bucketProperty = std::make_shared<HiveBucketProperty>(
      HiveBucketProperty::Kind::kHiveCompatible,
      4, // bucket count
      std::vector<std::string>{"c0"}, // bucketed by
      std::vector<TypePtr>{BIGINT()}, // bucket types
      std::vector<std::shared_ptr<const HiveSortingColumn>>{});

  auto compactProperty = std::make_shared<connector::hive::CompactProperty>(
      true, // enabled
      1024, // threshold
      std::vector<std::string>{"c0"}, // sort key columns
      "c1" // time column
  );

  // Should throw when trying to use compact mode with bucketed table
  EXPECT_THROW(
      {
        auto dataSink = createDataSinkWithCompact(
            rowType_,
            "/tmp/test",
            compactProperty,
            dwio::common::FileFormat::DWRF,
            {}, // partitionedBy
            bucketProperty); // bucketed table
      },
      std::exception)
      << "Should throw when using compact mode with bucketed tables";
}

TEST_F(HiveDataSinkTest, compactModeFileStatistics) {
  setupMemoryPools();

  auto compactProperty = createCompactPropertyWithGenerator(
      true, // enabled
      512, // small threshold to trigger multiple files - reduced for better
           // testing
      std::vector<std::string>{"c7", "c8", "c9"}, // multiple sort key columns
      "c0" // time column
  );

  // Create custom Parquet writer options with small flush policy for testing
  auto parquetOptions = std::make_shared<parquet::WriterOptions>();
  auto flushPolicyFactory = []() {
    return std::make_unique<parquet::DefaultFlushPolicy>(100, 1024);
  };
  parquetOptions->flushPolicyFactory = flushPolicyFactory;

  auto dataSink = createDataSinkWithCompact(
      compactRowType_, // Use compact-specific row type
      "/tmp/test",
      compactProperty,
      dwio::common::FileFormat::PARQUET, // Use Parquet format
      {}, // partitionedBy
      nullptr, // bucketProperty
      parquetOptions); // Custom writer options with small flush policy

  auto vectors = createVectors(150, 4); // Increased data size
  for (const auto& vector : vectors) {
    dataSink->appendData(vector);
  }

  EXPECT_TRUE(dataSink->finish());
  auto result = dataSink->close();

  // Verify each partition update contains proper statistics
  for (const auto& partitionUpdate : result) {
    auto updateJson = folly::parseJson(partitionUpdate);
    EXPECT_TRUE(updateJson.count("compactStats") > 0);

    auto compactStats = updateJson["compactStats"];

    // Verify sort key statistics
    auto sortKeyStats = compactStats["sortKeyStats"];
    for (const auto& sortKeyColumn : compactProperty->sortKeyColumns()) {
      if (sortKeyStats.count(sortKeyColumn) > 0) {
        auto columnStats = sortKeyStats[sortKeyColumn];
        EXPECT_TRUE(columnStats.count("min") > 0)
            << "Should have min value for sort key column: " << sortKeyColumn;
        EXPECT_TRUE(columnStats.count("max") > 0)
            << "Should have max value for sort key column: " << sortKeyColumn;
      }
    }

    // Verify time column statistics
    if (!compactProperty->timeColumn().empty()) {
      auto timeColumnStats = compactStats["timeColumnStats"];
      if (!timeColumnStats.empty()) {
        EXPECT_TRUE(timeColumnStats.count("min") > 0)
            << "Should have min value for time column";
        EXPECT_TRUE(timeColumnStats.count("max") > 0)
            << "Should have max value for time column";
      }
    }

    // Verify metadata
    EXPECT_EQ(
        compactStats["sortKeyColumns"],
        ISerializable::serialize(compactProperty->sortKeyColumns()));
    EXPECT_EQ(
        compactStats["timeColumn"].asString(), compactProperty->timeColumn());
  }
}

} // namespace
} // namespace facebook::velox::connector::hive

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  // Signal handler required for ThreadDebugInfoTest
  facebook::velox::process::addDefaultFatalSignalHandler();

  folly::Init init{&argc, &argv, false};

  // Register SerDe for CompactProperty
  facebook::velox::connector::hive::CompactProperty::registerSerDe();

  return RUN_ALL_TESTS();
}
