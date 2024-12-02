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
#include <filesystem>
#include <folly/Singleton.h>

#include "velox/parse/QueryPlanner.h"
#include "velox/common/base/tests/GTestUtils.h"

#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/vector/tests/utils/VectorTestBase.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/common/compression/Compression.h"
#include "velox/parse/TypeResolver.h"

using namespace facebook::velox;
using namespace facebook::velox::common;
using namespace facebook::velox::test;
using namespace facebook::velox::parquet;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::dwio::common;
namespace fs = std::filesystem;

namespace facebook::velox::core::test {

class QueryPlannerTest : public testing::Test,
                         public VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::initializeMemoryManager({});
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
    parquet::registerParquetReaderFactory();
    parquet::registerParquetWriterFactory();
    parse::registerTypeResolver();
    LocalFileSink::registerFactory();
    folly::SingletonVault::singleton()->registrationComplete();
    connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());
  }

  void assertPlan(
      const std::string& sql,
      const std::unordered_map<std::string, std::vector<RowVectorPtr>>&
          inMemoryTables,
      const std::string& expectedPlan) {
    SCOPED_TRACE(sql);
    auto plan = parseQuery(sql, pool_.get(), inMemoryTables);
    ASSERT_EQ(expectedPlan, plan->toString(false, true));
  }

  void assertPlanError(const std::string& sql, const char* expectedMessage) {
    SCOPED_TRACE(sql);
    SCOPED_TRACE(expectedMessage);
    try {
      parseQuery(sql, pool_.get());
      FAIL() << "Expected an exception: " << expectedMessage;
    } catch (std::exception& e) {
      ASSERT_TRUE(strstr(e.what(), expectedMessage) != nullptr) << e.what();
    }
  }

  void assertPlan(const std::string& sql, const std::string& expectedPlan) {
    assertPlan(sql, {}, expectedPlan);
  }

  RowVectorPtr makeEmptyRowVector(const RowTypePtr& rowType) {
    return std::make_shared<RowVector>(
        pool_.get(),
        rowType,
        nullptr, // nulls
        0,
        std::vector<VectorPtr>{});
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
};

TEST_F(QueryPlannerTest, values) {
  assertPlan(
      "SELECT x, x + 5 FROM UNNEST([1, 2, 3]) as t(x)",
      "-- Project[3]\n"
      "  -- Unnest[2]\n"
      "    -- Project[1]\n"
      "      -- Values[0]\n");

  assertPlan(
      "SELECT sum(x) FROM UNNEST([1, 2, 3]) as t(x)",
      "-- Project[4]\n"
      "  -- Aggregation[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");

  assertPlan(
      "SELECT x % 5, sum(x) FROM UNNEST([1, 2, 3]) as t(x) GROUP BY 1",
      "-- Project[5]\n"
      "  -- Aggregation[4]\n"
      "    -- Project[3]\n"
      "      -- Unnest[2]\n"
      "        -- Project[1]\n"
      "          -- Values[0]\n");

  assertPlan(
      "SELECT sum(x * 4) FROM UNNEST([1, 2, 3]) as t(x)",
      "-- Project[5]\n"
      "  -- Aggregation[4]\n"
      "    -- Project[3]\n"
      "      -- Unnest[2]\n"
      "        -- Project[1]\n"
      "          -- Values[0]\n");
}

TEST_F(QueryPlannerTest, inMemoryTable) {
  std::unordered_map<std::string, std::vector<RowVectorPtr>> inMemoryTables = {
      {"t",
       {makeEmptyRowVector(
           ROW({"a", "b", "c"}, {BIGINT(), INTEGER(), SMALLINT()}))}},
      {"u", {makeEmptyRowVector(ROW({"a", "b"}, {BIGINT(), DOUBLE()}))}},
  };

  assertPlan(
      "SELECT a, sum(b) FROM t WHERE c > 5 GROUP BY 1",
      inMemoryTables,
      "-- Project[3]\n"
      "  -- Aggregation[2]\n"
      "    -- Filter[1]\n"
      "      -- Values[0]\n");

  assertPlan(
      "SELECT t.a, t.b, t.c, u.b FROM t, u WHERE t.a = u.a",
      inMemoryTables,
      "-- Project[4]\n"
      "  -- Filter[3]\n"
      "    -- NestedLoopJoin[2]\n"
      "      -- Values[0]\n"
      "      -- Values[1]\n");
}

TEST_F(QueryPlannerTest, inStorageTable) {
  std::shared_ptr<memory::MemoryPool> data_column_pool{memory::memoryManager()->addLeafPool()};
  const std::string kHiveConnectorId = "test-hive";
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(
              kHiveConnectorId,
              std::make_shared<config::ConfigBase>(
                  std::unordered_map<std::string, std::string>()));
  connector::registerConnector(hiveConnector);
  filesystems::registerLocalFileSystem();

  // step1: 构造数据
  std::vector<CompressionKind> params = {
    CompressionKind::CompressionKind_NONE,
    CompressionKind::CompressionKind_SNAPPY,
    CompressionKind::CompressionKind_ZSTD,
  };

  auto schema = ROW({"a", "b", "c"}, {BIGINT(), INTEGER(), DOUBLE()});
  const int64_t kRows = 100;
  const auto data = makeRowVector({
      makeFlatVector<int64_t>(kRows, [](auto row) { return row - 15; }),
      makeFlatVector<int32_t>(kRows, [](auto row) { return row + 30; }),
      makeFlatVector<double>(kRows, [](auto row) { return row - 25; }),
  });

  auto root = TempDirectoryPath::create();
  auto filePath = fs::path(root->getPath()) / "lzl.parquet";
  ASSERT_FALSE(fs::exists(filePath.string()));

  std::cerr << "filePath: " << filePath.string() << std::endl;
  auto localFileSink = FileSink::create(
      fmt::format("/{}", filePath.string()), {.pool = data_column_pool.get()});
  ASSERT_TRUE(localFileSink->isBuffered());

  std::cerr << "nihao\n";

  facebook::velox::parquet::WriterOptions writerOptions;
  writerOptions.memoryPool = data_column_pool.get();
  writerOptions.compressionKind = CompressionKind::CompressionKind_SNAPPY;

  std::cerr << "nihao11\n";

  auto root_pool = memory::memoryManager()->addRootPool("InStorageTableTests");
  auto writer = std::make_unique<facebook::velox::parquet::Writer>(
      std::move(localFileSink), writerOptions, root_pool, schema);
  writer->write(data);
  writer->close();
  std::cerr << "nihao33\n";
  EXPECT_TRUE(fs::exists(filePath.string()));

  // step2: 构造执行计划
  std::unordered_map<std::string, TableScanInfo> inStorageTables = {
    {"t",
      {
      kHiveConnectorId,
      ROW({"a", "b", "c", "d"}, {BIGINT(), INTEGER(), DOUBLE(), DOUBLE()})
      }
    },
    {"u",
      {
      kHiveConnectorId,
      ROW({"a", "b", "c"}, {BIGINT(), INTEGER(), DOUBLE()})
      }
    },
  };

  auto sql = "SELECT a, min(b), sum(b), sum(d), max(d) FROM t WHERE c > 5 GROUP BY 1";

  auto plan = parseQuery(sql, pool_.get(), inStorageTables);
  std::cerr << plan->toString(true, true) << std::endl;
  
  // step3: 构造split
  auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
      kHiveConnectorId,
      filePath,
      dwio::common::FileFormat::PARQUET);
  auto planIDs = plan->leafPlanNodeIds();
  std::cerr << "TableScan planID: " << *planIDs.begin() << std::endl;

  // step4: Task run
  auto results = AssertQueryBuilder(plan)
                       .split(*planIDs.begin(), connectorSplit)
                       .copyResults(pool_.get());

  std::cout << std::endl
            << "> number of results in storage table test "
            << results->toString() << std::endl;
  std::cout << results->toString(0, results->size()) << std::endl;
}

TEST_F(QueryPlannerTest, customScalarFunctions) {
  DuckDbQueryPlanner planner(pool_.get());
  planner.registerScalarFunction("foo", {BIGINT()}, BOOLEAN());
  planner.registerScalarFunction("bar", {ARRAY(BIGINT())}, BIGINT());

  auto plan =
      planner.plan("SELECT foo(x), bar([x]) FROM UNNEST([1, 2, 3]) as t(x)");
  ASSERT_EQ(
      plan->toString(false, true),
      "-- Project[3]\n"
      "  -- Unnest[2]\n"
      "    -- Project[1]\n"
      "      -- Values[0]\n");
}

TEST_F(QueryPlannerTest, customAggregateFunctions) {
  DuckDbQueryPlanner planner(pool_.get());

  planner.registerAggregateFunction("foo_agg", {BIGINT(), BIGINT()}, DOUBLE());
  planner.registerAggregateFunction(
      "bar_agg", {ARRAY(BIGINT()), BIGINT()}, ARRAY(BIGINT()));

  auto plan = planner.plan(
      "SELECT foo_agg(x, x + 5), bar_agg([x], 1) FROM UNNEST([1, 2, 3]) as t(x)");
  ASSERT_EQ(
      plan->toString(false, true),
      "-- Project[5]\n"
      "  -- Aggregation[4]\n"
      "    -- Project[3]\n"
      "      -- Unnest[2]\n"
      "        -- Project[1]\n"
      "          -- Values[0]\n");
}

TEST_F(QueryPlannerTest, error) {
  assertPlanError(
      "SELECT * FROM my_table", "Table with name my_table does not exist");
  assertPlanError(
      "SELECT my_function(1)",
      "Scalar Function with name my_function does not exist");
  // DuckDB gives the same error regardless of whether scalar or aggregate
  // function is missing.
  assertPlanError(
      "SELECT x % 5, my_agg(x) FROM UNNEST([1, 2, 3]) as t(x) GROUP BY 1",
      "Scalar Function with name my_agg does not exist");
  assertPlanError("SELECT 'test ", "unterminated quoted string");
}

} // namespace facebook::velox::core::test
