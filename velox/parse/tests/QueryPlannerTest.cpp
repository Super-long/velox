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
    std::cerr << plan->toString(true, true) << std::endl;
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
    {"903_20620_prometheus_prod_default_d74d",
      {
      kHiveConnectorId,
      ROW({"t.alloc_affinity", "t.ap_name", "yottadb_partition_witness_replica_abnormal_offset"}, {BIGINT(), INTEGER(), DOUBLE()})
      }
    },
    {"u",
      {
      kHiveConnectorId,
      ROW({"a", "b", "c"}, {BIGINT(), INTEGER(), DOUBLE()})
      }
    },
  };

  auto sql = "SELECT a, min(b) as min_b , sum(b) as sum_b, sum(d) as sum_d, sum(d) as sum_d, sum(d) as sum_d FROM t WHERE c > 5 GROUP BY 1";
  // auto plan = parseQuery(sql, pool_.get(), inStorageTables);

  DuckDbQueryPlanner planner(pool_.get());

  auto table_names = planner.extractTableNames(sql);
  for (auto name : table_names) {
    std::cerr << "table: " << name << std::endl;
  }

  for (auto& [name, tableScanInfo] : inStorageTables) {
    planner.registerTableScan(name, tableScanInfo);
  }

  auto duckPlan = planner.duckPlan(sql);
  std::cerr << duckPlan->ToString() << std::endl;

  PlanNodeIdGenerator planNodeIdGenerator;
  auto plan = planner.duckPlanConvertVeloxPlan(duckPlan, &planNodeIdGenerator);

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

TEST_F(QueryPlannerTest, extractTableNames) {
  DuckDbQueryPlanner planner(pool_.get());

  struct testCase {
    std::string origin_sql;
    std::vector<std::string> table_names;
  };

  std::vector<testCase> test_cases {
    {
      "SELECT sum(yottadb_pod_mem_bytes_rss) AS d0 FROM \"903_20620_prometheus_prod_default_d74d\" WHERE ((\"t.cluster_name\" = 'yottadb-dev-cqth-dp0' AND \"t.node_ip\" = '11.63.17.227' AND \"t.pod_name\" = 'db-scg0-1-5' AND \"t.region_name\" = 'cqth') AND \"t.zyx_instance_mark\" = '11.159.246.204') ",
      {"903_20620_prometheus_prod_default_d74d"}
    },
    {
      "select count(speed), count(temp), max(type), id from car where city='city_0' group by id ",
      {"car"}
    },
    {
      "select count(speed), count(temp), max(type), id from \"car\"",
      {"car"}
    },
  };

  for (auto test_case: test_cases) {
    auto table_names = planner.extractTableNames(test_case.origin_sql);
    std::cerr << test_case.origin_sql << std::endl;
    for (auto name : table_names) {
      std::cerr << "table: " << name << std::endl;
    }
  }
  // ASSERT_EQ(true, false);
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
  
  // 验证time_bucket、from_nanoseconds
  plan = planner.plan("SELECT time_bucket('1 s', from_nanoseconds(x)) FROM UNNEST([1, 2, 3]) as t(x)");
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

  auto duckdbPlan = planner.duckPlan(
      "SELECT foo_agg(x, x + 5), bar_agg([x], 1) FROM UNNEST([1, 2, 3]) as t(x)");

  std::cerr << duckdbPlan->ToString() << std::endl;

  PlanNodeIdGenerator planNodeIdGenerator;
  auto plan = planner.duckPlanConvertVeloxPlan(duckdbPlan, &planNodeIdGenerator);

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


TEST_F(QueryPlannerTest, logicalOperators) {
  // 测试AND和OR逻辑运算符
  assertPlan(
      "SELECT x FROM UNNEST([1, 2, 3, 4, 5]) as t(x) WHERE x > 2 AND x < 5",
      "-- Project[4]\n"
      "  -- Filter[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");

  assertPlan(
      "SELECT x FROM UNNEST([1, 2, 3, 4, 5]) as t(x) WHERE x < 2 OR x > 4",
      "-- Project[4]\n"
      "  -- Filter[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");

  // 测试复杂的逻辑表达式
  assertPlan(
      "SELECT x FROM UNNEST([1, 2, 3, 4, 5]) as t(x) WHERE (x > 1 AND x < 4) OR x = 5",
      "-- Project[4]\n"
      "  -- Filter[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");
}

TEST_F(QueryPlannerTest, comparisonOperators) {
  // 测试各种比较运算符
  std::vector<std::string> operators = {"=", "<>", ">", "<", ">=", "<="};
  
  for (const auto& op : operators) {
    std::string sql = fmt::format(
        "SELECT x FROM UNNEST([1, 2, 3, 4, 5]) as t(x) WHERE x {} 3", op);
    
    assertPlan(
        sql,
        "-- Project[4]\n"
        "  -- Filter[3]\n"
        "    -- Unnest[2]\n"
        "      -- Project[1]\n"
        "        -- Values[0]\n");
  }
}

TEST_F(QueryPlannerTest, likeOperator) {
  // 测试LIKE运算符
  assertPlan(
      "SELECT s FROM UNNEST(['apple', 'banana', 'cherry']) as t(s) WHERE s LIKE 'a%'",
      "-- Project[4]\n"
      "  -- Filter[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");
  
  // 测试ILIKE运算符（不区分大小写）
  assertPlan(
      "SELECT s FROM UNNEST(['apple', 'banana', 'cherry']) as t(s) WHERE s ILIKE 'a%'",
      "-- Project[4]\n"
      "  -- Filter[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");
}

TEST_F(QueryPlannerTest, regexOperator) {
  // 测试正则表达式匹配
  assertPlan(
      "SELECT s FROM UNNEST(['apple', 'banana', 'cherry']) as t(s) WHERE s ~ '^a.*'",
      "-- Project[4]\n"
      "  -- Filter[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");
  
  // 测试regexp_full_match函数
  assertPlan(
      "SELECT s FROM UNNEST(['apple', 'banana', 'cherry']) as t(s) WHERE regexp_full_match(s, '^a.*')",
      "-- Project[4]\n"
      "  -- Filter[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");
}

TEST_F(QueryPlannerTest, inOperator) {
  // 测试IN运算符
  assertPlan(
      "SELECT x FROM UNNEST([1, 2, 3, 4, 5]) as t(x) WHERE x IN (1, 3, 5)",
      "-- Project[4]\n"
      "  -- Filter[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");
  
  // 测试NOT IN
  assertPlan(
      "SELECT x FROM UNNEST([1, 2, 3, 4, 5]) as t(x) WHERE x NOT IN(2, 4)",
      "-- Project[4]\n"
      "  -- Filter[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");
}

TEST_F(QueryPlannerTest, nullOperators) {
  // 测试IS NULL
  assertPlan(
      "SELECT x FROM UNNEST([1, 2, NULL, 4, 5]) as t(x) WHERE x IS NULL",
      "-- Project[4]\n"
      "  -- Filter[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");
  
  // 测试IS NOT NULL
  assertPlan(
      "SELECT x FROM UNNEST([1, 2, NULL, 4, 5]) as t(x) WHERE x IS NOT NULL",
      "-- Project[4]\n"
      "  -- Filter[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");
}

TEST_F(QueryPlannerTest, complexFilters) {
  // 测试复杂的过滤条件组合
  std::unordered_map<std::string, std::vector<RowVectorPtr>> inMemoryTables = {
      {"t",
       {makeEmptyRowVector(
           ROW({"a", "b", "c"}, {BIGINT(), VARCHAR(), DOUBLE()}))}},
  };

  // 组合多种过滤条件
  assertPlan(
      "SELECT a, b FROM t WHERE a > 10 AND b LIKE 'test%' AND c IS NOT NULL",
      inMemoryTables,
      "-- Project[2]\n"
      "  -- Filter[1]\n"
      "    -- Values[0]\n");
  
  // 测试OR和AND的组合
  assertPlan(
      "SELECT a, b FROM t WHERE (a > 10 AND b LIKE 'test%') OR (c > 5.0 AND b IN ('x', 'y', 'z'))",
      inMemoryTables,
      "-- Project[2]\n"
      "  -- Filter[1]\n"
      "    -- Values[0]\n");
}

TEST_F(QueryPlannerTest, orderBy) {
  // 测试ORDER BY
  assertPlan(
      "SELECT x FROM UNNEST([3, 1, 5, 2, 4]) as t(x) ORDER BY x",
      "-- OrderBy[4]\n"
      "  -- Project[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");
  
  // 测试多列排序
  assertPlan(
      "SELECT x, x % 3 as y FROM UNNEST([3, 1, 5, 2, 4]) as t(x) ORDER BY y, x DESC",
      "-- OrderBy[4]\n"
      "  -- Project[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");
  
  // 测试带表达式的排序 （x%2) project
  assertPlan(
      "SELECT x FROM UNNEST([3, 1, 5, 2, 4]) as t(x) ORDER BY x % 2, ABS(x - 3)",
      "-- Project[5]\n"
      "  -- OrderBy[4]\n"
      "    -- Project[3]\n"
      "      -- Unnest[2]\n"
      "        -- Project[1]\n"
      "          -- Values[0]\n");
}

TEST_F(QueryPlannerTest, distinct) {
  // 测试DISTINCT
  assertPlan(
      "SELECT DISTINCT x % 2 FROM UNNEST([1, 2, 3, 4, 5]) as t(x)",
      "-- Aggregation[4]\n"
      "  -- Project[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");
  
  // 测试多列DISTINCT
  assertPlan(
      "SELECT DISTINCT x % 2, x > 3 FROM UNNEST([1, 2, 3, 4, 5]) as t(x)",
      "-- Aggregation[4]\n"
      "  -- Project[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");
}

TEST_F(QueryPlannerTest, limit) {
  // 测试LIMIT
  assertPlan(
      "SELECT x FROM UNNEST([1, 2, 3, 4, 5]) as t(x) LIMIT 3",
      "-- Limit[4]\n"
      "  -- Project[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");
  
  // 测试LIMIT与OFFSET
  assertPlan(
      "SELECT x FROM UNNEST([1, 2, 3, 4, 5]) as t(x) LIMIT 3 OFFSET 2",
      "-- Limit[4]\n"
      "  -- Project[3]\n"
      "    -- Unnest[2]\n"
      "      -- Project[1]\n"
      "        -- Values[0]\n");
  
  // 测试LIMIT与ORDER BY组合
  assertPlan(
      "SELECT x FROM UNNEST([3, 1, 5, 2, 4]) as t(x) ORDER BY x LIMIT 3",
      "-- Limit[5]\n"
      "  -- OrderBy[4]\n"
      "    -- Project[3]\n"
      "      -- Unnest[2]\n"
      "        -- Project[1]\n"
      "          -- Values[0]\n");
}

TEST_F(QueryPlannerTest, complexQueries) {
  // 测试组合多种操作的复杂查询
  assertPlan(
      "SELECT DISTINCT x % 3 as mod FROM UNNEST([1, 2, 3, 4, 5, 6, 7, 8, 9]) as t(x) WHERE x > 2 ORDER BY mod LIMIT 2",
      "-- Limit[7]\n"
      "  -- OrderBy[6]\n"
      "    -- Aggregation[5]\n"
      "      -- Project[4]\n"
      "        -- Filter[3]\n"
      "          -- Unnest[2]\n"
      "            -- Project[1]\n"
      "              -- Values[0]\n");
  // JOIN
  
}

} // namespace facebook::velox::core::test
