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
#pragma once
#include <memory>

#include "velox/core/PlanNode.h"
#include "velox/parse/PlanNodeIdGenerator.h"

#include <duckdb.hpp> // @manual

namespace facebook::velox::core {

struct SplitInfo {
  // necessary
//   std::string connector_id_,
//   std::string file_path;
  RowType row_type_;
//   dwio::common::FileFormat file_format_;
  
  // unnecessary
//   uint64_t _start = 0;
//   uint64_t _length = std::numeric_limits<uint64_t>::max();
//   std::unordered_map<std::string, std::optional<std::string>> _partitionKeys;
//   std::optional<int32_t> _tableBucketNumber = std::nullopt;
//   std::unordered_map<std::string, std::string> _customSplitInfo;
//   std::shared_ptr<std::string> _extraFileInfo;
//   std::unordered_map<std::string, std::string> _serdeParameters;
//   int64_t _splitWeight = 0;
//   std::unordered_map<std::string, std::string> _infoColumns;
//   std::optional<FileProperties> _properties = std::nullopt;
//   dwio::common::FormatSpecificOptions _format_specific_options;
};

struct TableScanInfo {
  std::string connector_id_;
  RowTypePtr dataColumns_;
  TableScanInfo() : connector_id_(""), dataColumns_(nullptr) {}
  TableScanInfo(const std::string& connector_id, RowTypePtr dataColumns) :
    connector_id_(connector_id), dataColumns_(dataColumns) {}
};

class DuckDbQueryPlanner {
 public:
  DuckDbQueryPlanner(memory::MemoryPool* pool) : pool_{pool} {}

  void registerTable(
      const std::string& name,
      const std::vector<RowVectorPtr>& data);

  void registerTableScan(
      const std::string& name,
      const TableScanInfo& tableScanInfo);

  void registerScalarFunction(
      const std::string& name,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& returnType);

  // TODO Allow replacing built-in DuckDB functions. Currently, replacing "sum"
  // causes a crash (a bug in DuckDB). Replacing existing functions is useful
  // when signatures don't match.
  void registerAggregateFunction(
      const std::string& name,
      const std::vector<TypePtr>& argTypes,
      const TypePtr& returnType);

  PlanNodePtr plan(const std::string& sql);

  std::unique_ptr<::duckdb::LogicalOperator> duckPlan(const std::string& sql);

  PlanNodePtr duckPlanConvertVeloxPlan(const std::unique_ptr<::duckdb::LogicalOperator>& duckdb_plan);

 private:
  ::duckdb::DuckDB db_;
  ::duckdb::Connection conn_{db_};
  memory::MemoryPool* pool_;
  std::unordered_map<std::string, std::vector<RowVectorPtr>> tables_;
  std::unordered_map<std::string, TableScanInfo> tableScans_;
};

PlanNodePtr parseQuery(
    const std::string& sql,
    memory::MemoryPool* pool,
    const std::unordered_map<std::string, std::vector<RowVectorPtr>>&
        inMemoryTables = {});

// hiveDataSources: Files contained in data sources
PlanNodePtr parseQuery(
    const std::string& sql,
    memory::MemoryPool* pool,
    const std::unordered_map<std::string, TableScanInfo>& hiveDataSources);

} // namespace facebook::velox::core
