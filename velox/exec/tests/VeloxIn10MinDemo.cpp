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
#include <folly/init/Init.h>
#include "velox/common/memory/Memory.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/core/Expressions.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/tpch/gen/TpchGen.h"
#include "velox/vector/tests/utils/VectorTestBase.h"
#include <algorithm>
#include <chrono>
#include <typeinfo>

#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/functions/Udf.h"
#include "velox/expression/ExprToSubfieldFilter.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec::test;

using namespace facebook::velox::dwio::common;
using namespace facebook::velox::dwrf;


class VeloxIn10MinDemo : public VectorTestBase {
 public:
  const std::string kTpchConnectorId = "test-tpch";

  VeloxIn10MinDemo() {
    // Register Presto scalar functions.
    functions::prestosql::registerAllScalarFunctions();

    // Register Presto aggregate functions.
    aggregate::prestosql::registerAllAggregateFunctions();

    // Register type resolver with DuckDB SQL parser.
    parse::registerTypeResolver();

    // Register the TPC-H Connector Factory.
    connector::registerConnectorFactory(
        std::make_shared<connector::tpch::TpchConnectorFactory>());

    // Create and register a TPC-H connector.
    auto tpchConnector =
        connector::getConnectorFactory(
            connector::tpch::TpchConnectorFactory::kTpchConnectorName)
            ->newConnector(
                kTpchConnectorId,
                std::make_shared<config::ConfigBase>(
                    std::unordered_map<std::string, std::string>()));
    connector::registerConnector(tpchConnector);
  }

  ~VeloxIn10MinDemo() {
    connector::unregisterConnector(kTpchConnectorId);
    connector::unregisterConnectorFactory(
        connector::tpch::TpchConnectorFactory::kTpchConnectorName);
  }

  /// Parse SQL expression into a typed expression tree using DuckDB SQL parser.
  core::TypedExprPtr parseExpression(
      const std::string& text,
      const RowTypePtr& rowType) {
    parse::ParseOptions options;
    auto untyped = parse::parseExpr(text, options);
    return core::Expressions::inferTypes(untyped, rowType, execCtx_->pool());
  }

  /// Compile typed expression tree into an executable ExprSet.
  std::unique_ptr<exec::ExprSet> compileExpression(
      const std::string& expr,
      const RowTypePtr& rowType) {
    std::vector<core::TypedExprPtr> expressions = {
        parseExpression(expr, rowType)};
    return std::make_unique<exec::ExprSet>(
        std::move(expressions), execCtx_.get());
  }

  /// Evaluate an expression on one batch of data.
  VectorPtr evaluate(exec::ExprSet& exprSet, const RowVectorPtr& input) {
    exec::EvalCtx context(execCtx_.get(), &exprSet, input.get());

    SelectivityVector rows(input->size());
    std::vector<VectorPtr> result(1);
    exprSet.eval(rows, context, result);
    return result[0];
  }

  /// Make TPC-H split to add to TableScan node.
  exec::Split makeTpchSplit() const {
    return exec::Split(std::make_shared<connector::tpch::TpchConnectorSplit>(
        kTpchConnectorId));
  }

  /// Run the demo.
  void run();

  std::shared_ptr<folly::Executor> executor_{
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency())};
  std::shared_ptr<core::QueryCtx> queryCtx_{
      core::QueryCtx::create(executor_.get())};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
};

namespace fs = std::filesystem;

void scanDirectory(const std::string& directoryPath, std::vector<std::string>& filePaths) {
    try {
        // 检查提供的路径是否确实是一个目录
        if (fs::is_directory(directoryPath)) {
            // 遍历目录及其子目录中的所有文件
            for (const auto& entry : fs::recursive_directory_iterator(directoryPath)) {
                if (fs::is_regular_file(entry)) {
                    // 获取文件的绝对路径并添加到向量中
                    filePaths.push_back(entry.path().string());
                }
            }
        } else {
            std::cerr << "Provided path is not a directory: " << directoryPath << std::endl;
        }
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Filesystem error: " << e.what() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "Standard exception: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "An unknown error occurred." << std::endl;
    }
}

std::mutex batchMutex;

struct ParquetIndex {
    int64_t offset = 0;
    int64_t limit = 0;
};

// void processParquetFile(const std::string& filePath,
//                         const std::vector<ParquetIndex>& indexs,
//                         std::vector<String> filters,
//                         std::vector<RowVectorPtr>& rowBatches,
//                         memory::MemoryPool* pool) {

// }

void processParquetFile(const std::string& filePath, std::vector<RowVectorPtr>& rowBatches, memory::MemoryPool* pool) {
    dwio::common::ReaderOptions readerOpts{pool};
    readerOpts.setFileFormat(FileFormat::PARQUET);
    auto reader = getReaderFactory(FileFormat::PARQUET)
                      ->createReader(
                          std::make_unique<BufferedInput>(
                              std::make_shared<LocalReadFile>(filePath),
                              readerOpts.memoryPool()),
                          readerOpts);
    if (!reader) {
        std::cerr << "Failed to create reader for file: " << filePath << std::endl;
        return;
    }

    RowReaderOptions rowReaderOptions;
    auto rowType = ROW({"measurement", "timestamp", "arch", "datacenter", "hostname", "os", "rack", "region", "service", "service_environment", "service_version", "team", 
                        "usage_guest", "usage_guest_nice", "usage_idle", "usage_iowait", "usage_irq", "usage_nice", "usage_softirq", "usage_steal", "usage_system", "usage_user"},
                       {VARCHAR(), TIMESTAMP(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(),
                        DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});
    rowReaderOptions.select(
      std::make_shared<facebook::velox::dwio::common::ColumnSelector>(
        rowType, rowType->names(), nullptr, false));
    auto scanSpec = std::make_shared<facebook::velox::common::ScanSpec>("");
  
    auto untyped = parse::parseExpr("usage_steal >= 3.0", parse::ParseOptions());
    auto filterExpr = core::Expressions::inferTypes(untyped, rowType, pool);
    std::shared_ptr<core::QueryCtx> queryCtx{core::QueryCtx::create()};
    exec::SimpleExpressionEvaluator evaluator{queryCtx.get(), pool};
    auto [subfield, filter] = exec::toSubfieldFilter(filterExpr, &evaluator);
    auto fieldSpec = scanSpec->getOrCreateChild(subfield);
    fieldSpec->addFilter(*filter);

    scanSpec->addAllChildFields(*rowType);
    rowReaderOptions.setScanSpec(scanSpec);
    auto rowReader = reader->createRowReader(rowReaderOptions);
    std::cout << "The type of rowReader is: " << typeid(*rowReader).name() << std::endl;

    auto rowBatch = BaseVector::create(rowType, 50000, pool);

    while (rowReader->next(50000, rowBatch)) {
        auto rowVector = std::dynamic_pointer_cast<RowVector>(rowBatch);
        if (rowVector) {
            std::lock_guard<std::mutex> lock(batchMutex);
            rowBatches.push_back(rowVector);
        } else {
            std::cerr << "Error: Batch is not a RowVector for file: " << filePath << std::endl;
        }
    }
}

// 单field表达式计算
void func1(const std::vector<RowVectorPtr>& allRowBatches, memory::MemoryPool* pool, bool print_result) {
  // project可以对原始filed做加工，支持的函数在velox/functions/prestosql/registration/StringFunctionsRegistration.cpp
  auto start = std::chrono::high_resolution_clock::now();
  auto test_plan1 = PlanBuilder()
                  .values({allRowBatches})
                  .project({"measurement",
                            "arch",
                            "datacenter",
                            "usage_nice * 10.0 as sum_usage_nice",
                            "usage_guest * 100.0 as sum_usage_guest"})
                  .singleAggregation(
                    {"measurement", "arch", "datacenter"},
                    {"SUM(sum_usage_nice)  as sum_usage_nice",
                     "SUM(sum_usage_guest)  as sum_usage_guest"})
                  .planNode();

  auto testAvg1 = AssertQueryBuilder(test_plan1).copyResults(pool);

  if (print_result) {
    std::cout << std::endl
                << "test_plan1: " << testAvg1->toString()
                << std::endl;
    std::cout << testAvg1->toString(0, testAvg1->size()) << std::endl;
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration<double>(end - start).count();

  std::cout << "func1 Execution time: " << duration << " seconds; Result size " << testAvg1->size() << std::endl;
}

// add split
void func2_kSerial(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "timestamp", "arch", "datacenter", "hostname", "os", "rack", "region", "service", "service_environment", "service_version", "team", 
                    "usage_guest", "usage_guest_nice", "usage_idle", "usage_iowait", "usage_irq", "usage_nice", "usage_softirq", "usage_steal", "usage_system", "usage_user"},
                    {VARCHAR(), TIMESTAMP(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(),
                    DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});
    core::PlanNodeId scanNodeId;
    auto readPlanFragment = exec::test::PlanBuilder()
                                .tableScan(inputRowType)
                                .capturePlanNodeId(scanNodeId)
                                .singleAggregation(
                                    {
                                        "measurement",
                                        "arch",
                                        "datacenter"
                                    },
                                    {
                                        "SUM(usage_nice)  as sum_usage_nice",
                                        "SUM(usage_guest)  as sum_usage_guest"
                                    })
                                .orderBy({"arch"}, /*isPartial=*/false)
                                .limit(0, 10, true)
                                .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kSerial);

    std::string directoryPath = "/velox/lzl_parquet_test/";

    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        readTask->addSplit(scanNodeId, exec::Split{connectorSplit});
    }
    readTask->noMoreSplits(scanNodeId);

    if (print_result) {
        while (auto result = readTask->next()) {
            std::cerr << "func2 kSerial input->size():" << result->size() << std::endl;
            for (vector_size_t i = 0; i < result->size(); ++i) {
                std::cerr << result->toString(i) << std::endl;
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration<double>(end - start).count();
        std::cout << "func2 Execution time: " << duration << " seconds" << std::endl;
    }
}

void func2_kParallel(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "timestamp", "arch", "datacenter", "hostname", "os", "rack", "region", "service", "service_environment", "service_version", "team", 
                    "usage_guest", "usage_guest_nice", "usage_idle", "usage_iowait", "usage_irq", "usage_nice", "usage_softirq", "usage_steal", "usage_system", "usage_user"},
                    {VARCHAR(), TIMESTAMP(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(), VARCHAR(),
                    DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE(), DOUBLE()});
    core::PlanNodeId scanNodeId;
    auto readPlanFragment = exec::test::PlanBuilder()
                                .tableScan(inputRowType)
                                .capturePlanNodeId(scanNodeId)
                                .singleAggregation(
                                    {
                                        "measurement",
                                        "arch",
                                        "datacenter"
                                    },
                                    {
                                        "SUM(usage_nice)  as sum_usage_nice",
                                        "SUM(usage_guest)  as sum_usage_guest"
                                    })
                                .orderBy({"arch"}, /*isPartial=*/false)
                                .limit(0, 10, true)
                                .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "func2 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        if (print_result) {
            std::cerr << "func2 kParallel input->size():" << input->size() << std::endl;
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cerr << input->toString(i) << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/lzl_parquet_test/";

    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        readTask->addSplit(scanNodeId, exec::Split{connectorSplit});
    }

    auto maxDrivers = 5;
    readTask->start(maxDrivers);
    readTask->noMoreSplits(scanNodeId);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "func2 kParallel after sleep" << std::endl;
}

// localMerge
void func3_kParallel(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "datacenter", "hostname","usage_guest"},
                    {VARCHAR(), VARCHAR(), VARCHAR(), DOUBLE()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .singleAggregation(
                        {"measurement", "datacenter", "hostname"},{ "max(usage_guest) as max_usage_guest"})
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .singleAggregation(
                        {"measurement", "datacenter", "hostname"},{ "max(usage_guest) as max_usage_guest"})
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"measurement", "datacenter", "hostname"},
                {
                    test_plan1,
                    test_plan2,
                })
                .singleAggregation(
                    {
                        "measurement",
                        "datacenter",
                        "hostname"
                    },
                    {
                        "max(max_usage_guest)  as max_usage_guest"
                    })
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "func3 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        if (print_result) {
            std::cerr << "func3 kParallel input->size():" << input->size() << std::endl;
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cerr << input->toString(i) << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/lzl_parquet_test/";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 2 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);

    auto maxDrivers = 5;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "func3 kParallel after sleep" << std::endl;
}

void func3_kSerial(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    //   单field表达式计算
    //   project可以对原始filed做加工，支持的函数在velox/functions/prestosql/registration/StringFunctionsRegistration.cpp
    auto inputRowType = ROW({"measurement", "datacenter", "hostname","usage_guest"},
                    {VARCHAR(), VARCHAR(), VARCHAR(), DOUBLE()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .singleAggregation(
                        {"measurement", "datacenter", "hostname"},{ "max(usage_guest) as max_usage_guest"})
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .singleAggregation(
                        {"measurement", "datacenter", "hostname"},{ "max(usage_guest) as max_usage_guest"})
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"measurement", "datacenter", "hostname"},
                {
                    test_plan1,
                    test_plan2,
                })
                .singleAggregation(
                    {
                        "measurement",
                        "datacenter",
                        "hostname"
                    },
                    {
                        "max(max_usage_guest)  as max_usage_guest"
                    })
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kSerial);

    std::string directoryPath = "/velox/lzl_parquet_test/";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 2 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }
    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);

    while (auto result = readTask->next()) {
        std::cerr << "func3 kSerial input->size():" << result->size() << std::endl;
        if (print_result) {
            for (vector_size_t i = 0; i < result->size(); ++i) {
                std::cerr << result->toString(i) << std::endl;
            }
        }
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration<double>(end - start).count();
    std::cout << "func3 kSerial Execution time: " << duration << " seconds" << std::endl;
}

// 多field表达式计算
void func4(const std::vector<RowVectorPtr>& allRowBatches, memory::MemoryPool* pool, bool print_result) {
  auto start = std::chrono::high_resolution_clock::now();
  auto test_plan1 = PlanBuilder()
        .values({allRowBatches})
        .singleAggregation(
          {"measurement", "arch", "datacenter"},
          {"SUM(usage_nice)  as sum_usage_nice",
           "SUM(usage_guest)  as sum_usage_guest"})
        .project({
          "measurement",
          "arch",
          "datacenter",
          "(sum_usage_nice + sum_usage_guest)*10.0/3.0 AS total_usage"})
        .planNode();

  auto testAvg1 = AssertQueryBuilder(test_plan1).copyResults(pool);

  if (print_result) {
    std::cout << std::endl
                << "test_plan: " << testAvg1->toString()
                << std::endl;
    std::cout << testAvg1->toString(0, testAvg1->size()) << std::endl;
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration<double>(end - start).count();

  std::cout << "func4 Execution time: " << duration << " seconds; Result size " << testAvg1->size() << std::endl;
}

// select max(bucket) group by appid
void func5(const std::vector<RowVectorPtr>& allRowBatches, memory::MemoryPool* pool, bool print_result) {
  auto start = std::chrono::high_resolution_clock::now();
  auto test_plan2 = PlanBuilder()
                  .values({allRowBatches})
                  .singleAggregation(
                    {"arch"},
                    {"min(datacenter) AS min_b",
                     "max(\"hostname\") AS max_d"})
                  .planNode();

  auto testAvg2 = AssertQueryBuilder(test_plan2).copyResults(pool);

  if (print_result) {
    std::cout << std::endl
                << "func5: " << testAvg2->toString()
                << std::endl;
    std::cout << testAvg2->toString(0, testAvg2->size()) << std::endl;
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration<double>(end - start).count();

  std::cout << "func5 Execution time: " << duration << " seconds; Result size " << testAvg2->size() << std::endl;
}

// show tag values cardinality from measurement WITH KEY = "arch" group by appid
void func6(const std::vector<RowVectorPtr>& allRowBatches, memory::MemoryPool* pool, bool print_result) {
  auto start = std::chrono::high_resolution_clock::now();
  auto test_plan3 = PlanBuilder()
                  .values({allRowBatches})
                  .singleAggregation(
                    {"measurement", "datacenter"},
                    {"count(DISTINCT arch)"})
                  .planNode();

  auto testAvg3 = AssertQueryBuilder(test_plan3).copyResults(pool);

  if (print_result) {
    std::cout << std::endl
                << "func6: " << testAvg3->toString()
                << std::endl;
    std::cout << testAvg3->toString(0, testAvg3->size()) << std::endl;
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration<double>(end - start).count();

  std::cout << "func6 Execution time: " << duration << " seconds; Result size " << testAvg3->size() << std::endl;
}

// show measurements
void func7(const std::vector<RowVectorPtr>& allRowBatches, memory::MemoryPool* pool, bool print_result) {
  auto start = std::chrono::high_resolution_clock::now();
  auto test_plan4 = PlanBuilder()
                  .values({allRowBatches})
                  .aggregation(
                      {"measurement"},  // 使用measurement作为分组键
                      {},               // 没有聚合函数
                      {},               // 没有额外的计算
                      {},
                      core::AggregationNode::Step::kSingle,
                      false)
                  .planNode();

  auto testAvg4 = AssertQueryBuilder(test_plan4).copyResults(pool);
 
  if (print_result) {
    std::cout << std::endl
                << "test_plan7: " << testAvg4->toString()
                << std::endl;
    std::cout << testAvg4->toString(0, testAvg4->size()) << std::endl;
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration<double>(end - start).count();

  std::cout << "func7 Execution time: " << duration << " seconds; Result size " << testAvg4->size() << std::endl;
}

// show series cardinality from measurement group by arch
void func8(const std::vector<RowVectorPtr>& allRowBatches, memory::MemoryPool* pool, bool print_result) {
  auto start = std::chrono::high_resolution_clock::now();
  auto test_plan4 = PlanBuilder()
                  .values({allRowBatches})
                  .project({"measurement", "arch", "datacenter", "hostname",
                            "concat(measurement, '-', arch, '-', datacenter, '-', hostname) as unique_id"})
                  .singleAggregation(
                      {"measurement", "arch"},
                      {"count(DISTINCT unique_id)",
                       "max(datacenter)"})
                  .planNode();

  auto testAvg4 = AssertQueryBuilder(test_plan4).copyResults(pool);

  if (print_result) {
    std::cout << std::endl
                << "test_plan8: " << testAvg4->toString()
                << std::endl;
    std::cout << testAvg4->toString(0, testAvg4->size()) << std::endl;
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration<double>(end - start).count();

  std::cout << "func8 Execution time: " << duration << " seconds; Result size " << testAvg4->size() << std::endl;
}

// group by tag, time(60s)
void func9(const std::vector<RowVectorPtr>& allRowBatches, memory::MemoryPool* pool, bool print_result) {
  auto start = std::chrono::high_resolution_clock::now();
  auto test_plan4 = PlanBuilder()
                  .values({allRowBatches})
                  //.limit(0, 10000, false)
                  .project({
                    "CAST((CAST(to_unixtime(timestamp) AS BIGINT) / 60000000) * 60000000 AS BIGINT) AS timestamp60",
                    "hostname",
                    "usage_guest"})
                  .singleAggregation(
                    {"timestamp60"},
                      {"max(usage_guest)"})
                  .orderBy({"timestamp60"}, false)
                  //.limit(0, 10, false)
                  .planNode();

  auto testAvg4 = AssertQueryBuilder(test_plan4).copyResults(pool);

  if (print_result) {
    std::cout << std::endl
                << "test_plan9: " << testAvg4->toString()
                << std::endl;
    std::cout << testAvg4->toString(0, testAvg4->size()) << std::endl;
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration<double>(end - start).count();

  std::cout << "func9 Execution time: " << duration << " seconds; Result size " << testAvg4->size() << std::endl;
}

// group by tag, time(60s) udf
void func10(const std::vector<RowVectorPtr>& allRowBatches, memory::MemoryPool* pool, bool print_result) {
  auto start = std::chrono::high_resolution_clock::now();
  auto test_plan1 = PlanBuilder()
                  .values({allRowBatches})
                  //.limit(0, 10000, false)
                  .project({
                    "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                    "hostname",
                    "usage_guest"})
                  .singleAggregation(
                      {"truncated_timestamp"},
                      {"max(usage_guest)"})
                  .orderBy({"truncated_timestamp"}, false)
                  //.limit(0, 10, false)
                  .planNode();
  auto testAvg1 = AssertQueryBuilder(test_plan1).copyResults(pool);

  if (print_result) {
    std::cout << std::endl
                << "test_plan1: " << testAvg1->toString()
                << std::endl;
    std::cout << testAvg1->toString(0, testAvg1->size()) << std::endl;
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration<double>(end - start).count();

  std::cout << "func10 Execution time: " << duration << " seconds; Result size " << testAvg1->size() << std::endl;
}


void scan(const std::vector<RowVectorPtr>& allRowBatches, memory::MemoryPool* pool, bool print_result) {
  auto start = std::chrono::high_resolution_clock::now();
  auto test_plan1 = PlanBuilder()
                  .values({allRowBatches})
                  .limit(0, 20, true)
                  .project({"measurement",
                            "arch",
                            "datacenter",
                            "usage_nice",
                            "usage_steal"})
                  .planNode();

  auto testAvg1 = AssertQueryBuilder(test_plan1).copyResults(pool);

  if (print_result) {
    std::cout << std::endl
                << "test_plan1: " << testAvg1->toString()
                << std::endl;
    std::cout << testAvg1->toString(0, testAvg1->size()) << std::endl;
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration<double>(end - start).count();

  std::cout << "func1 Execution time: " << duration << " seconds; Result size " << testAvg1->size() << std::endl;
}

void BaradCOSQ1(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "2xx_response_max", "2xx_response_rate_max","timestamp"},
                    {VARCHAR(), DOUBLE(), DOUBLE(), TIMESTAMP()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .limit(0, 1000, true)
                    .orderBy({"timestamp"}, false)
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .limit(0, 1000, true)
                    .orderBy({"timestamp"}, false)
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .limit(0, 1000, true)
                    .orderBy({"timestamp"}, false)
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .limit(0, 1000, true)
                    .orderBy({"timestamp"}, false)
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .limit(0, 1000, true)
                    .orderBy({"timestamp"}, false)
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .limit(0, 1000, true)
                .orderBy({"timestamp"}, false)
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        std::cerr <<  "enter printer\n";
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOSQ1 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        if (print_result) {
            std::cout << "BaradCOSQ1 kParallel input->size():" << input->size() << std::endl;
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 1;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ2(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "2xx_response_max", "2xx_response_rate_max","timestamp"},
                    {VARCHAR(), DOUBLE(), DOUBLE(), TIMESTAMP()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .singleAggregation(
                    {},
                    {"sum(\"max_response_max\") as max_response_max",
                    "sum(\"max_response_rate_max\") as max_response_rate_max"})
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        std::cerr <<  "enter printer\n";
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOSQ1 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        if (print_result) {
            std::cout << "BaradCOSQ1 kParallel input->size():" << input->size() << std::endl;
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 1;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ2_TableScan(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "appid", "2xx_response_max", "2xx_response_rate_max","timestamp"},
                    {VARCHAR(), VARCHAR(), DOUBLE(), DOUBLE(), TIMESTAMP()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .singleAggregation(
                    {},
                    {"sum(\"max_response_max\") as max_response_max",
                    "sum(\"max_response_rate_max\") as max_response_rate_max"})
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        std::cerr <<  "enter printer\n";
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOSQ1 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        if (print_result) {
            std::cout << "BaradCOSQ1 kParallel input->size():" << input->size() << std::endl;
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 10;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ2_Filter(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "appid", "2xx_response_max", "2xx_response_rate_max","timestamp"},
                    {VARCHAR(), VARCHAR(), DOUBLE(), DOUBLE(), TIMESTAMP()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .singleAggregation(
                    {},
                    {"sum(\"max_response_max\") as max_response_max",
                    "sum(\"max_response_rate_max\") as max_response_rate_max"})
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        std::cerr <<  "enter printer\n";
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOSQ1 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        if (print_result) {
            std::cout << "BaradCOSQ1 kParallel input->size():" << input->size() << std::endl;
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 10;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ2_TableScan_15(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "appid", "2xx_response_max", "2xx_response_rate_max","timestamp"},
                    {VARCHAR(), VARCHAR(), DOUBLE(), DOUBLE(), TIMESTAMP()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'"})
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'"})
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'"})
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'"})
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'"})
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .singleAggregation(
                    {},
                    {"sum(\"max_response_max\") as max_response_max",
                    "sum(\"max_response_rate_max\") as max_response_rate_max"})
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        std::cerr <<  "enter printer\n";
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOSQ1 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        if (print_result) {
            std::cout << "BaradCOSQ1 kParallel input->size():" << input->size() << std::endl;
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 10;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ2_Filter_15(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "appid", "2xx_response_max", "2xx_response_rate_max","timestamp"},
                    {VARCHAR(), VARCHAR(), DOUBLE(), DOUBLE(), TIMESTAMP()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'")
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'")
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'")
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'")
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'")
                    .singleAggregation(
                        {},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .singleAggregation(
                    {},
                    {"sum(\"max_response_max\") as max_response_max",
                    "sum(\"max_response_rate_max\") as max_response_rate_max"})
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        std::cerr <<  "enter printer\n";
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOSQ1 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        if (print_result) {
            std::cout << "BaradCOSQ1 kParallel input->size():" << input->size() << std::endl;
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 10;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ5Q6_TableScan(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "appid", "2xx_response_max", "2xx_response_rate_max","timestamp"},
                    {VARCHAR(), VARCHAR(), DOUBLE(), DOUBLE(), TIMESTAMP()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"measurement", "appid"},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .singleAggregation(
                    {"measurement", "appid"},
                    {"sum(\"max_response_max\") as max_response_max",
                    "sum(\"max_response_rate_max\") as max_response_rate_max"})
                .orderBy({"max_response_max", "max_response_rate_max"}, true)
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        std::cerr <<  "enter printer\n";
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOS Q5/Q6 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        if (print_result) {
            std::cout << "BaradCOS Q5/Q6 kParallel input->size():" << input->size() << std::endl;
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 10;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ5Q6_Filter(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "appid", "2xx_response_max", "2xx_response_rate_max","timestamp"},
                    {VARCHAR(), VARCHAR(), DOUBLE(), DOUBLE(), TIMESTAMP()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"measurement", "appid"},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .singleAggregation(
                    {"measurement", "appid"},
                    {"sum(\"max_response_max\") as max_response_max",
                    "sum(\"max_response_rate_max\") as max_response_rate_max"})
                .orderBy({"max_response_max", "max_response_rate_max"}, true)
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        std::cerr <<  "enter printer\n";
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOS Q5/Q6 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        if (print_result) {
            std::cout << "BaradCOS Q5/Q6 kParallel input->size():" << input->size() << std::endl;
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 10;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ5Q6_TableScan_15(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "appid", "2xx_response_max", "2xx_response_rate_max","timestamp"},
                    {VARCHAR(), VARCHAR(), DOUBLE(), DOUBLE(), TIMESTAMP()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'"})
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'"})
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'"})
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'"})
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'"})
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"measurement", "appid"},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .singleAggregation(
                    {"measurement", "appid"},
                    {"sum(\"max_response_max\") as max_response_max",
                    "sum(\"max_response_rate_max\") as max_response_rate_max"})
                .orderBy({"max_response_max", "max_response_rate_max"}, true)
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        std::cerr <<  "enter printer\n";
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOS Q5/Q6 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        if (print_result) {
            std::cout << "BaradCOS Q5/Q6 kParallel input->size():" << input->size() << std::endl;
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 10;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ5Q6_Filter_15(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "appid", "2xx_response_max", "2xx_response_rate_max","timestamp"},
                    {VARCHAR(), VARCHAR(), DOUBLE(), DOUBLE(), TIMESTAMP()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'")
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'")
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'")
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'")
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(\"2xx_response_max\") as max_response_max",
                         "count(\"2xx_response_rate_max\") as max_response_rate_max"})
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'")
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"measurement", "appid"},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .singleAggregation(
                    {"measurement", "appid"},
                    {"sum(\"max_response_max\") as max_response_max",
                    "sum(\"max_response_rate_max\") as max_response_rate_max"})
                .orderBy({"max_response_max", "max_response_rate_max"}, true)
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        std::cerr <<  "enter printer\n";
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOS Q5/Q6 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        if (print_result) {
            std::cout << "BaradCOS Q5/Q6 kParallel input->size():" << input->size() << std::endl;
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 10;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ7(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "2xx_response_max", "2xx_response_rate_max","timestamp"},
                    {VARCHAR(), VARCHAR(), DOUBLE(), DOUBLE(), TIMESTAMP()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .singleAggregation(
                        {"measurement"},
                        {"max(\"2xx_response_max\") as max_response_max"})
                    .project({"measurement",
                              "appid",
                              "timestamp",
                              "max_response_max",
                              "2xx_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .singleAggregation(
                        {"measurement"},
                        {"max(\"2xx_response_max\") as max_response_max"})
                    .project({"measurement",
                              "appid",
                              "timestamp",
                              "max_response_max",
                              "2xx_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .singleAggregation(
                        {"measurement"},
                        {"max(\"2xx_response_max\") as max_response_max"})
                    .project({"measurement",
                              "appid",
                              "timestamp",
                              "max_response_max",
                              "2xx_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .singleAggregation(
                        {"measurement"},
                        {"max(\"2xx_response_max\") as max_response_max"})
                    .project({"measurement",
                              "appid",
                              "timestamp",
                              "max_response_max",
                              "2xx_response_rate_max"})
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .singleAggregation(
                        {"measurement"},
                        {"max(\"2xx_response_max\") as max_response_max"})
                    .project({"measurement",
                              "appid",
                              "timestamp",
                              "max_response_max",
                              "2xx_response_rate_max"})

                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"measurement"},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .singleAggregation(
                    {"measurement"},
                    {"max(\"max_response_max\") as max_response_max"})
                .project({"measurement",
                            "appid",
                            "timestamp",
                            "max_response_max",
                            "2xx_response_rate_max"})
                .orderBy({"max_response_max"}, true)
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        std::cerr <<  "enter printer\n";
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOSQ1 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        if (print_result) {
            std::cout << "BaradCOSQ1 kParallel input->size():" << input->size() << std::endl;
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 10;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ8(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "appid", "2xx_response_max", "2xx_response_rate_max","timestamp"},
                    {VARCHAR(), VARCHAR(), DOUBLE(), DOUBLE(), TIMESTAMP()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"appid", "truncated_timestamp"},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .singleAggregation(
                    {"appid", "truncated_timestamp"},
                        {"max(\"max1\") as max1",
                        "max(\"max2\") as max2"})
                .orderBy({"appid", "truncated_timestamp"}, true)
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOSQ8 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        std::cout << "BaradCOSQ8 kParallel input->size():" << input->size() << std::endl;
        if (print_result) {
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 10;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ8_TableScan(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "appid", "2xx_response_max", "2xx_response_rate_max","timestamp"},
                    {VARCHAR(), VARCHAR(), DOUBLE(), DOUBLE(), TIMESTAMP()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'"})
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"appid", "truncated_timestamp"},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .singleAggregation(
                    {"appid", "truncated_timestamp"},
                        {"max(\"max1\") as max1",
                        "max(\"max2\") as max2"})
                .orderBy({"appid", "truncated_timestamp"}, true)
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOSQ8 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        std::cout << "BaradCOSQ8 kParallel input->size():" << input->size() << std::endl;
        if (print_result) {
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 10;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ8_Filter(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "appid", "2xx_response_max", "2xx_response_rate_max","timestamp"},
                    {VARCHAR(), VARCHAR(), DOUBLE(), DOUBLE(), TIMESTAMP()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"appid", "truncated_timestamp"},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .singleAggregation(
                    {"appid", "truncated_timestamp"},
                        {"max(\"max1\") as max1",
                        "max(\"max2\") as max2"})
                .orderBy({"appid", "truncated_timestamp"}, true)
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOSQ8 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        std::cout << "BaradCOSQ8 kParallel input->size():" << input->size() << std::endl;
        if (print_result) {
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 10;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ8_TableScan_15(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "appid", "2xx_response_max", "2xx_response_rate_max","timestamp"},
                    {VARCHAR(), VARCHAR(), DOUBLE(), DOUBLE(), TIMESTAMP()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'"})
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'"})
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'"})
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'"})
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType, {"appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'"})
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"appid", "truncated_timestamp"},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .singleAggregation(
                    {"appid", "truncated_timestamp"},
                        {"max(\"max1\") as max1",
                        "max(\"max2\") as max2"})
                .orderBy({"appid", "truncated_timestamp"}, true)
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOSQ8 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        std::cout << "BaradCOSQ8 kParallel input->size():" << input->size() << std::endl;
        if (print_result) {
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 10;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ8_Filter_15(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "appid", "2xx_response_max", "2xx_response_rate_max","timestamp"},
                    {VARCHAR(), VARCHAR(), DOUBLE(), DOUBLE(), TIMESTAMP()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'")
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'")
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'")
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'")
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284'")
                    .project({
                        "to_start_of_interval(timestamp, 60000000000) AS truncated_timestamp",
                        "appid",
                        "\"2xx_response_max\"",
                        "\"2xx_response_rate_max\""})
                    .singleAggregation(
                        {"appid", "truncated_timestamp"},
                        {"max(\"2xx_response_max\") as max1",
                        "max(\"2xx_response_rate_max\") as max2"})
                    //.filter("appid = '1306836858' or appid = '1322122694' or appid = '1307892150' or appid = '1322054006' or appid = '1318627516' or appid = '1253294642' or appid = '1321433947' or appid = '1322064440' or appid = '1322066414' or appid = '1323303425' or appid = '1307424400' or appid = '1317494014' or appid = '1318620775' or appid = '1322118508' or appid = '1324568284' or appid = '1323305552' or appid = '1325044998' or appid = '1317677652' or appid = '1322101944' or appid = '1309288448' or appid = '1309288373' or appid = '1251347466' or appid = '1317999259' or appid = '1317784339' or appid = '1322346896' or appid = '1322569459' or appid = '1307797246' or appid = '1307289124' or appid = '1322481800' or appid = '1322332980' or appid = '1311834546' or appid = '1317762239' or appid = '1324973457' or appid = '1320483507' or appid = '1321755170' or appid = '1313876562' or appid = '1323315569' or appid = '1258879759' or appid = '1251021019' or appid = '1305829787' or appid = '1323294209' or appid = '1300209989' or appid = '1322822988' or appid = '1323045694' or appid = '1324626218' or appid = '1316795528' or appid = '1323088523' or appid = '1322930990' or appid = '1320193118' or appid = '1321840768' or appid = '1317495544' or appid = '1324521439' or appid = '1257122416' or appid = '1321916070' or appid = '1321736097' or appid = '1318061733' or appid = '1320441055' or appid = '1306530656' or appid = '1320866967' or appid = '1323084697' or appid = '1322641502' or appid = '1318097515' or appid = '1322290945' or appid = '1322350941' or appid = '1318660421' or appid = '1318303559' or appid = '1311352952' or appid = '1321933002' or appid = '1316087616' or appid = '1326715548' or appid = '1322521951' or appid = '1256826131' or appid = '1323234235' or appid = '1321415444' or appid = '1321289750' or appid = '1310550420' or appid = '1313125604' or appid = '1316767306' or appid = '1308711956' or appid = '1309288092' or appid = '1316737941' or appid = '1307893560' or appid = '1312471972' or appid = '1317791835' or appid = '1255396242' or appid = '1321228048' or appid = '1309288190' or appid = '1309465239' or appid = '1323414661' or appid = '1320097917' or appid = '1309372262' or appid = '1323343969' or appid = '1302008357' or appid = '1302248825' or appid = '1309290656' or appid = '1258273208' or appid = '1318313730' or appid = '1258272081' or appid = '1323345348' or appid = '1323345487' or appid = '1319136965' or appid = '1318648400' or appid = '1306223988' or appid = '1319372016' or appid = '1321059894' or appid = '1323142339' or appid = '1311997601' or appid = '1323319095' or appid = '1309127877' or appid = '1259687498' or appid = '1308159426' or appid = '1312567013' or appid = '1323112216' or appid = '1323346705' or appid = '1311377385' or appid = '1253271207' or appid = '1317807399' or appid = '1311315173' or appid = '1322607595' or appid = '1323298868' or appid = '1257703699' or appid = '1309838984' or appid = '1317869977' or appid = '1317167735' or appid = '1321569257' or appid = '1259038742' or appid = '1312660216' or appid = '1253536072' or appid = '1322373670' or appid = '1251671073' or appid = '1318018891' or appid = '1321248387' or appid = '1322118584' or appid = '1322351238' or appid = '1307813112' or appid = '1318700733' or appid = '1318654059' or appid = '1318088018' or appid = '1252571540' or appid = '1318971916' or appid = '1321314981' or appid = '1321236364' or appid = '1309347150' or appid = '1320291712' or appid = '1320291723' or appid = '1314103746' or appid = '1321265080' or appid = '1322956306' or appid = '1304205662' or appid = '1309290157' or appid = '1314762179' or appid = '1318557889' or appid = '1311454970' or appid = '1319116612' or appid = '1322954015' or appid = '1321397187' or appid = '1322537468' or appid = '1300961709' or appid = '1317377286' or appid = '1322641740' or appid = '1317710441' or appid = '1259455335' or appid = '1323391253' or appid = '1322537655' or appid = '1317695983' or appid = '1325421967' or appid = '1316687976' or appid = '1259007386' or appid = '1251961879' or appid = '1323914510' or appid = '1322567637' or appid = '1323418585' or appid = '1327366517' or appid = '1304943718' or appid = '1321409455' or appid = '1316603219' or appid = '1319024038' or appid = '1323337673' or appid = '1318180635' or appid = '1312802364' or appid = '1322677882' or appid = '1323093345' or appid = '1314230769' or appid = '1309290014' or appid = '1317206254' or appid = '1312005402' or appid = '1316967449' or appid = '1311771498' or appid = '1319116176' or appid = '1322371280' or appid = '1317666037' or appid = '1319986359' or appid = '1321946775' or appid = '1324456275' or appid = '1323320956' or appid = '1257121556' or appid = '1253831162' or appid = '1311513850' or appid = '1323032304' or appid = '1328655881' or appid = '1306032166' or appid = '1322333574' or appid = '1302486051' or appid = '1302341041' or appid = '1320313251' or appid = '1306223740' or appid = '1323914827' or appid = '1324152967' or appid = '1318593706' or appid = '1300966417' or appid = '1322453195' or appid = '1317749612' or appid = '1315212065' or appid = '1318242486' or appid = '1323944863' or appid = '1323109327' or appid = '1323130891' or appid = '1323360913' or appid = '1311472121' or appid = '1317999173' or appid = '1319731546' or appid = '1252831244' or appid = '1317991547' or appid = '1320616831' or appid = '1322955949' or appid = '1317899597' or appid = '1322655757' or appid = '1322511090' or appid = '1309361264' or appid = '1321297807' or appid = '1323007157' or appid = '1322961108' or appid = '1321830724' or appid = '1319068435' or appid = '1319323218' or appid = '1321994729' or appid = '1322547912' or appid = '1315011287' or appid = '1322599498' or appid = '1320052299' or appid = '1316484572' or appid = '1319606596' or appid = '1313861771' or appid = '1321344686' or appid = '1317993811' or appid = '1314238494' or appid = '1321265788' or appid = '1322303063' or appid = '1318061729' or appid = '1319960455' or appid = '1321647899' or appid = '1318061710' or appid = '1320022339' or appid = '1322947395' or appid = '1258944054' or appid = '1318313568' or appid = '1324975041' or appid = '1326937973' or appid = '1322342061' or appid = '1321397198' or appid = '1323011439' or appid = '1309290105' or appid = '1310904612' or appid = '1321610524' or appid = '1319815610' or appid = '1313222818' or appid = '1323131803' or appid = '1251178014' or appid = '1322218856' or appid = '1323306261' or appid = '1321362235' or appid = '1321046592' or appid = '1317446406' or appid = '1313551663' or appid = '1309132841' or appid = '1309169164' or appid = '1322592166' or appid = '1307784656' or appid = '1322599323' or appid = '1314839933' or appid = '1322583009' or appid = '1323023931' or appid = '1319216831' or appid = '1319703593' or appid = '1324556892' or appid = '1318259848' or appid = '1323004146' or appid = '1317494009' or appid = '1316247630' or appid = '1321213454' or appid = '1324521215' or appid = '1251966257' or appid = '1320315800' or appid = '1318660291' or appid = '1307786009' or appid = '1315928145' or appid = '1317915338' or appid = '1321552520' or appid = '1322632684' or appid = '1320539357' or appid = '1321362135' or appid = '1309347031' or appid = '1316561194' or appid = '1256911967' or appid = '1311258067' or appid = '1322351125' or appid = '1309288544' or appid = '1317547540' or appid = '1319101002' or appid = '1318069902' or appid = '1324521437' or appid = '1324324573' or appid = '1313412502' or appid = '1258639055' or appid = '1323305469' or appid = '1311573317' or appid = '1323306944' or appid = '1318002097' or appid = '1324400612' or appid = '1323141025' or appid = '1322332992' or appid = '1322355285' or appid = '1256266908' or appid = '1321362196' or appid = '1318608404' or appid = '1324521377' or appid = '1321990431' or appid = '1313357897' or appid = '1323943236' or appid = '1322583413' or appid = '1323347148' or appid = '1322230876' or appid = '1321649777' or appid = '1324521454' or appid = '1313122938' or appid = '1314230507' or appid = '1323030690' or appid = '1324456782' or appid = '1300537147' or appid = '1323393791' or appid = '1317772658' or appid = '1320767464' or appid = '1323384828' or appid = '1323004629' or appid = '1317989556' or appid = '1316499970' or appid = '1322530785' or appid = '1321852012' or appid = '1323537275' or appid = '1309290158' or appid = '1319731557' or appid = '1322105015' or appid = '1318689608' or appid = '1322537772' or appid = '1322538015' or appid = '1316103211' or appid = '1323045233' or appid = '1303115834' or appid = '1324570129' or appid = '1316649107' or appid = '1320861302' or appid = '1318268228' or appid = '1309290300' or appid = '1321647739' or appid = '1322736618' or appid = '1316686114' or appid = '1323067454' or appid = '1307827306' or appid = '1255892227' or appid = '1306286044' or appid = '1323083703' or appid = '1321639420' or appid = '1304283830' or appid = '1317899503' or appid = '1322251786' or appid = '1311512319' or appid = '1311511678' or appid = '1311504485' or appid = '1322696340' or appid = '1322247827' or appid = '1321954843' or appid = '1318379923' or appid = '1319436254' or appid = '1323113130' or appid = '1323317713' or appid = '1322658524' or appid = '1323115766' or appid = '1315819770' or appid = '1321610465' or appid = '1309950756' or appid = '1323122671' or appid = '1321330518' or appid = '1309361228' or appid = '1251131906' or appid = '1324521360' or appid = '1321426898' or appid = '1321142900' or appid = '1322559428' or appid = '1322453191' or appid = '1318313827' or appid = '1258234669' or appid = '1309352875' or appid = '1323345212' or appid = '1255385461' or appid = '1309347142' or appid = '1323344351' or appid = '1323304190' or appid = '1324207371' or appid = '1321233685' or appid = '1324556941' or appid = '1309335755' or appid = '1321330536' or appid = '1322599423' or appid = '1317495575' or appid = '1322026534' or appid = '1321362167' or appid = '1303247135' or appid = '1317278618' or appid = '1322515672' or appid = '1327117748' or appid = '1317292783' or appid = '1322522056' or appid = '1322601573' or appid = '1322932743' or appid = '1322905048' or appid = '1320358524' or appid = '1314068247' or appid = '1320315790' or appid = '1321410471' or appid = '1306184118' or appid = '1322575629' or appid = '1323385268' or appid = '1317334891' or appid = '1309290019' or appid = '1323408772' or appid = '1322870629' or appid = '1308130805' or appid = '1320243987' or appid = '1257952279' or appid = '1322994943' or appid = '1320223386' or appid = '1313222809'")
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"appid", "truncated_timestamp"},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .singleAggregation(
                    {"appid", "truncated_timestamp"},
                        {"max(\"max1\") as max1",
                        "max(\"max2\") as max2"})
                .orderBy({"appid", "truncated_timestamp"}, true)
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOSQ8 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        std::cout << "BaradCOSQ8 kParallel input->size():" << input->size() << std::endl;
        if (print_result) {
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 10;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ9(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement"}, {VARCHAR()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .aggregation(
                        {"measurement"},
                        {},               // 没有聚合函数
                        {},               // 没有额外的计算
                        {},
                        core::AggregationNode::Step::kSingle,
                        false)
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .aggregation(
                        {"measurement"},
                        {},               // 没有聚合函数
                        {},               // 没有额外的计算
                        {},
                        core::AggregationNode::Step::kSingle,
                        false)
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .aggregation(
                        {"measurement"},
                        {},               // 没有聚合函数
                        {},               // 没有额外的计算
                        {},
                        core::AggregationNode::Step::kSingle,
                        false)
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .aggregation(
                        {"measurement"},
                        {},               // 没有聚合函数
                        {},               // 没有额外的计算
                        {},
                        core::AggregationNode::Step::kSingle,
                        false)
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .aggregation(
                        {"measurement"},
                        {},               // 没有聚合函数
                        {},               // 没有额外的计算
                        {},
                        core::AggregationNode::Step::kSingle,
                        false)
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"measurement"},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .aggregation(
                    {"measurement"},
                    {},               // 没有聚合函数
                    {},               // 没有额外的计算
                    {},
                    core::AggregationNode::Step::kSingle,
                    false)
                .orderBy({"measurement"}, true)
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOSQ9 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        std::cout << "BaradCOSQ9 kParallel input->size():" << input->size() << std::endl;
        if (print_result) {
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 10;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}

void BaradCOSQ12(bool print_result) {
    auto start = std::chrono::high_resolution_clock::now();
    auto inputRowType = ROW({"measurement", "appid", "bucket"}, {VARCHAR(), VARCHAR(), VARCHAR()});
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    core::PlanNodeId test_plan1_scanNodeId;
    auto test_plan1 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan1_scanNodeId)
                    .project({"measurement", "appid", "bucket",
                                "concat(measurement, '-', appid, '-', bucket) as unique_id"})
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(DISTINCT unique_id) as count1"})
                    .planNode();

    core::PlanNodeId test_plan2_scanNodeId;
    auto test_plan2 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan2_scanNodeId)
                    .project({"measurement", "appid", "bucket",
                                "concat(measurement, '-', appid, '-', bucket) as unique_id"})
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(DISTINCT unique_id) as count1"})
                    .planNode();

    core::PlanNodeId test_plan3_scanNodeId;
    auto test_plan3 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan3_scanNodeId)
                    .project({"measurement", "appid", "bucket",
                                "concat(measurement, '-', appid, '-', bucket) as unique_id"})
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(DISTINCT unique_id) as count1"})
                    .planNode();

    core::PlanNodeId test_plan4_scanNodeId;
    auto test_plan4 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan4_scanNodeId)
                    .project({"measurement", "appid", "bucket",
                                "concat(measurement, '-', appid, '-', bucket) as unique_id"})
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(DISTINCT unique_id) as count1"})
                    .planNode();

    core::PlanNodeId test_plan5_scanNodeId;
    auto test_plan5 = PlanBuilder(planNodeIdGenerator)
                    .tableScan(inputRowType)
                    .capturePlanNodeId(test_plan5_scanNodeId)
                    .project({"measurement", "appid", "bucket",
                                "concat(measurement, '-', appid, '-', bucket) as unique_id"})
                    .singleAggregation(
                        {"measurement", "appid"},
                        {"count(DISTINCT unique_id) as count1"})
                    .planNode();

    auto readPlanFragment =
        PlanBuilder(planNodeIdGenerator)
            .localMerge(
                {"measurement", "appid"},
                {
                    test_plan1,
                    test_plan2,
                    test_plan3,
                    test_plan4,
                    test_plan5,
                })
                .singleAggregation(
                    {"measurement", "appid"},
                    {"sum(count1)"})
                .orderBy({"measurement", "appid"}, true)
            .planFragment();

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

    std::shared_ptr<folly::Executor> executor(
        std::make_shared<folly::CPUThreadPoolExecutor>(
            std::thread::hardware_concurrency()));

    auto printer = [print_result, start] (
                        RowVectorPtr input, ContinueFuture* future) {
        if (!input) {
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration<double>(end - start).count();
            std::cout << "BaradCOSQ9 kParallel Execution time: " << duration << " seconds" << std::endl;
            return exec::BlockingReason::kNotBlocked;
        }
        std::cout << "BaradCOSQ9 kParallel input->size():" << input->size() << std::endl;
        if (print_result) {
            for (vector_size_t i = 0; i < input->size(); ++i) {
                std::cout << i << ": " << input->toString(i) << "|||" << std::endl;
            }
        }
        return exec::BlockingReason::kNotBlocked;
    };

    auto readTask = exec::Task::create(
        "my_read_task",
        readPlanFragment,
        /*destination=*/0,
        core::QueryCtx::create(executor.get()),
        exec::Task::ExecutionMode::kParallel,
        printer);

    std::string directoryPath = "/velox/wal_QgKJAydY_parquet";

    int index = 0;
    for (auto& filePath : fs::directory_iterator(directoryPath)) {
        auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
            kHiveConnectorId,
            filePath.path().string(),
            dwio::common::FileFormat::PARQUET);
        if (index % 5 == 0) {
            readTask->addSplit(test_plan1_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 1) {
            readTask->addSplit(test_plan2_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 2) {
            readTask->addSplit(test_plan3_scanNodeId, exec::Split{connectorSplit});
        } else if (index % 5 == 3) {
            readTask->addSplit(test_plan4_scanNodeId, exec::Split{connectorSplit});
        } else  {
            readTask->addSplit(test_plan5_scanNodeId, exec::Split{connectorSplit});
        }
        index++;
    }

    readTask->noMoreSplits(test_plan1_scanNodeId);
    readTask->noMoreSplits(test_plan2_scanNodeId);
    readTask->noMoreSplits(test_plan3_scanNodeId);
    readTask->noMoreSplits(test_plan4_scanNodeId);
    readTask->noMoreSplits(test_plan5_scanNodeId);

    auto maxDrivers = 10;
    readTask->start(maxDrivers);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cerr << "BaradCOSQ1 kParallel after sleep" << std::endl;
}


template <typename TExecParams>
class ToStartOfInterval {
public:
    void call(Timestamp& out, const Timestamp& timestamp, const int64_t& interval_nanos) {
        int64_t truncatedNanos = (timestamp.getNanos()*1000 / interval_nanos) * interval_nanos;
        out = Timestamp::fromNanos(truncatedNanos);
        // std::cerr << timestamp.getSeconds() << " " << timestamp.getNanos()  << " " << truncatedNanos << " " << out.toNanos() << std::endl;
        // std::cerr << out.getSeconds() << " " << out.getNanos()  << std::endl;
    }
};

void VeloxIn10MinDemo::run() {
  parquet::registerParquetReaderFactory();

  std::vector<std::string> filePaths;
  std::string directoryPath = "/velox/lzl_parquet_test/";
  scanDirectory(directoryPath, filePaths);
  for (const auto& path : filePaths) {
    std::cout << path << std::endl;
  }

  registerFunction<ToStartOfInterval, Timestamp, Timestamp, int64_t>(
        {"to_start_of_interval"});
  
  // step1 读取parquet
  std::vector<std::thread> threads;
  std::vector<RowVectorPtr> allRowBatches;
  auto begin = std::chrono::high_resolution_clock::now();
  for (const auto& filePath : filePaths) {
      threads.emplace_back(std::thread(processParquetFile, filePath, std::ref(allRowBatches), pool()));
  }

  for (auto& thread : threads) {
      thread.join();
  }
  auto start = std::chrono::high_resolution_clock::now();

  auto duration = std::chrono::duration<double>(start - begin).count();

  std::cout << "parse parquet: " << duration << " seconds" << std::endl;

  // step2 各个语句执行引擎性能
//   // 单field表达式计算
//   func1(allRowBatches, pool(), false);
  
  // 串行add split
  //func2_kSerial(true);

  //func2_kParallel(true);

//   // localMerge
  //func3_kParallel(false);
  
  //func3_kSerial(false);

    BaradCOSQ8_TableScan_15(true);
//   // 多field表达式计算
//   func4(allRowBatches, pool(), false);

//   // select max(bucket) group by appid
//   func5(allRowBatches, pool(), true);

//   // show tag values cardinality from measurement WITH KEY = "bucket" group by appid
//   func6(allRowBatches, pool(), false);
  
//   // show measurements
//   func7(allRowBatches, pool(), false);

//   // show series cardinality from measurement group by arch
//   func8(allRowBatches, pool(), true);

  // group by tag, time(60s)
//   func9(allRowBatches, pool(), false);

  
//   // group by tag, time(60s) udf
//   func10(allRowBatches, pool(), false);

  //scan(allRowBatches, pool(), true);

  // step3 执行引擎性能

  parquet::unregisterParquetReaderFactory();
}

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv, false};

  // Initializes the process-wide memory-manager with the default options.
  memory::initializeMemoryManager({});

  VeloxIn10MinDemo demo;
  demo.run();
}
