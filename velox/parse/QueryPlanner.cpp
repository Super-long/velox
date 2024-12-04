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
#include <unordered_map>
#include <iostream>

#include "velox/parse/QueryPlanner.h"
#include "velox/duckdb/conversion/DuckConversion.h"
#include "velox/parse/DuckLogicalOperator.h"
#include "velox/dwio/common/tests/utils/FilterGenerator.h"
#include "velox/connectors/hive/TableHandle.h"

#include <duckdb.hpp> // @manual
#include <duckdb/main/connection.hpp> // @manual
#include <duckdb/planner/expression/bound_aggregate_expression.hpp> // @manual
#include <duckdb/planner/expression/bound_cast_expression.hpp> // @manual
#include <duckdb/planner/expression/bound_comparison_expression.hpp> // @manual
#include <duckdb/planner/expression/bound_constant_expression.hpp> // @manual
#include <duckdb/planner/expression/bound_function_expression.hpp> // @manual
#include <duckdb/planner/expression/bound_reference_expression.hpp> // @manual

namespace facebook::velox::core {

namespace {

class ColumnNameGenerator {
 public:
  std::string next(const std::string& prefix = "_c") {
    if (names_.count(prefix)) {
      auto name = fmt::format("{}{}", prefix, nextId_++);
      names_.insert(name);
      return name;
    }

    names_.insert(prefix);
    return prefix;
  }

 private:
  std::unordered_set<std::string> names_;
  int nextId_{0};
};

struct QueryContext {
  PlanNodeIdGenerator planNodeIdGenerator;
  ColumnNameGenerator columnNameGenerator;
  const std::unordered_map<std::string, std::vector<RowVectorPtr>>& inMemoryTables;
  const std::unordered_map<std::string, TableScanInfo>& inStorageTables;

  QueryContext(const std::unordered_map<std::string, std::vector<RowVectorPtr>>& _inMemoryTables,
               const std::unordered_map<std::string, TableScanInfo>&  _inStorageTables)
      : inMemoryTables{_inMemoryTables}, inStorageTables{_inStorageTables} {}

  std::string nextNodeId() {
    return planNodeIdGenerator.next();
  }

  std::string nextColumnName() {
    return columnNameGenerator.next();
  }

  std::string nextColumnName(const std::string& prefix) {
    return columnNameGenerator.next(prefix);
  }
};

std::string mapScalarFunctionName(const std::string& name) {
  static const std::unordered_map<std::string, std::string> kMapping = {
      {"+", "plus"},
      {"-", "minus"},
      {"*", "multiply"},
      {"/", "divide"},
      {"%", "mod"},
      {"list_value", "array_constructor"},
  };

  auto it = kMapping.find(name);
  if (it != kMapping.end()) {
    return it->second;
  }

  return name;
}

std::string mapAggregateFunctionName(const std::string& name) {
  static const std::unordered_map<std::string, std::string> kMapping = {
      {"count_star", "count"},
  };

  auto it = kMapping.find(name);
  if (it != kMapping.end()) {
    return it->second;
  }

  return name;
}

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalDummyScan& logicalDummyScan,
    memory::MemoryPool* pool,
    QueryContext& queryContext) {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (auto i = 0; i < logicalDummyScan.types.size(); ++i) {
    names.push_back(queryContext.nextColumnName());
    types.push_back(duckdb::toVeloxType(logicalDummyScan.types[i]));
  }

  auto rowType = ROW(std::move(names), std::move(types));

  std::vector<RowVectorPtr> vectors = {std::make_shared<RowVector>(
      pool, rowType, nullptr, 1, std::vector<VectorPtr>{})};
  return std::make_shared<ValuesNode>(queryContext.nextNodeId(), vectors);
}

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalGet& logicalGet,
    memory::MemoryPool* pool,
    std::vector<PlanNodePtr> sources,
    QueryContext& queryContext) {
  if (logicalGet.function.name == "unnest") {
    VELOX_CHECK_EQ(1, sources.size());
    return std::make_shared<UnnestNode>(
        queryContext.nextNodeId(),
        std::vector<FieldAccessTypedExprPtr>{}, // replicateVariables
        std::vector<FieldAccessTypedExprPtr>{
            std::make_shared<FieldAccessTypedExpr>(
                sources[0]->outputType()->childAt(0),
                sources[0]->outputType()->asRow().nameOf(0))},
        std::vector<std::string>{"a"},
        std::nullopt, // ordinalityName
        std::move(sources[0]));
  }

  VELOX_CHECK_EQ(logicalGet.function.name, "seq_scan");
  VELOX_CHECK_EQ(0, sources.size());

  const auto& columnIds = logicalGet.column_ids;
  std::vector<std::string> names(columnIds.size());
  std::vector<TypePtr> types(columnIds.size());

  for (auto i = 0; i < columnIds.size(); ++i) {
    names[i] = queryContext.nextColumnName(logicalGet.names[columnIds[i]]);
    types[i] = duckdb::toVeloxType(logicalGet.returned_types[columnIds[i]]);
  }

  auto rowType = ROW(std::move(names), std::move(types));

  //std::cerr << "rowType: " << rowType->toString() << std::endl;

  auto tableName = logicalGet.function.to_string(logicalGet.bind_data.get());
  auto in_memory_it = queryContext.inMemoryTables.find(tableName);
  if (in_memory_it != queryContext.inMemoryTables.end()) {
    std::vector<RowVectorPtr> data;
    for (auto& rowVector : in_memory_it->second) {
      std::vector<VectorPtr> children;
      if (rowVector->size() > 0) {
        for (auto i = 0; i < columnIds.size(); ++i) {
          children.push_back(rowVector->childAt(columnIds[i]));
        }
      }
      data.push_back(std::make_shared<RowVector>(
          pool, rowType, nullptr, rowVector->size(), children));
    }
    return std::make_shared<ValuesNode>(queryContext.nextNodeId(), data);
  }

  auto in_storage_it = queryContext.inStorageTables.find(tableName);
  if (in_storage_it != queryContext.inStorageTables.end()) {
    auto tableScanInfo = in_storage_it->second;
    dwio::common::SubfieldFilters filters;
    auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
        tableScanInfo.connector_id_,
        tableName,
        true,
        std::move(filters),
        /*remainingFilterExpr*/ nullptr,
        tableScanInfo.dataColumns_);

    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>> assignments;
    for (uint32_t i = 0; i < rowType->size(); ++i) {
      const auto& name = rowType->nameOf(i);
      const auto& type = rowType->childAt(i);
      // 暂时不考虑别名
      assignments.insert(
          {name,
          std::make_shared<connector::hive::HiveColumnHandle>(
              name,
              connector::hive::HiveColumnHandle::ColumnType::kRegular,
              type,
              type)});
    }

    return std::make_shared<core::TableScanNode>( 
      queryContext.nextNodeId(), rowType, tableHandle, assignments);
  }

  VELOX_CHECK(
      false,
      "Can't find in-memory or in-storage table: {}",
      tableName);
}

TypedExprPtr toVeloxExpression(
    ::duckdb::Expression& expression,
    const TypePtr& inputType);

TypedExprPtr toVeloxComparisonExpression(
    const std::string& name,
    ::duckdb::Expression& expression,
    const TypePtr& inputType) {
  auto* comparison =
      dynamic_cast<::duckdb::BoundComparisonExpression*>(&expression);
  std::vector<TypedExprPtr> children{
      toVeloxExpression(*comparison->left, inputType),
      toVeloxExpression(*comparison->right, inputType)};

  return std::make_shared<CallTypedExpr>(BOOLEAN(), std::move(children), name);
}

TypedExprPtr toVeloxExpression(
    ::duckdb::Expression& expression,
    const TypePtr& inputType) {
  switch (expression.type) {
    case ::duckdb::ExpressionType::VALUE_CONSTANT: {
      auto* constant =
          dynamic_cast<::duckdb::BoundConstantExpression*>(&expression);
      return std::make_shared<ConstantTypedExpr>(
          duckdb::toVeloxType(constant->return_type),
          duckdb::duckValueToVariant(constant->value));
    }
    case ::duckdb::ExpressionType::COMPARE_EQUAL:
      return toVeloxComparisonExpression("eq", expression, inputType);
    case ::duckdb::ExpressionType::COMPARE_GREATERTHAN:
      return toVeloxComparisonExpression("gt", expression, inputType);
    case ::duckdb::ExpressionType::OPERATOR_CAST: {
      auto* cast = dynamic_cast<::duckdb::BoundCastExpression*>(&expression);
      return std::make_shared<CastTypedExpr>(
          duckdb::toVeloxType(cast->return_type),
          std::vector<TypedExprPtr>{toVeloxExpression(*cast->child, inputType)},
          cast->try_cast);
    }
    case ::duckdb::ExpressionType::BOUND_FUNCTION: {
      auto* func =
          dynamic_cast<::duckdb::BoundFunctionExpression*>(&expression);

      std::vector<TypedExprPtr> children;
      for (auto& child : func->children) {
        children.push_back(toVeloxExpression(*child, inputType));
      }

      return std::make_shared<CallTypedExpr>(
          duckdb::toVeloxType(func->function.return_type),
          std::move(children),
          mapScalarFunctionName(func->function.name));
    }
    case ::duckdb::ExpressionType::BOUND_REF: {
      auto* ref =
          dynamic_cast<::duckdb::BoundReferenceExpression*>(&expression);
      return std::make_shared<FieldAccessTypedExpr>(
          duckdb::toVeloxType(ref->return_type),
          inputType->asRow().nameOf(ref->index));
    }
    case ::duckdb::ExpressionType::BOUND_AGGREGATE: {
      auto* agg =
          dynamic_cast<::duckdb::BoundAggregateExpression*>(&expression);

      std::vector<TypedExprPtr> children;
      for (auto& child : agg->children) {
        children.push_back(toVeloxExpression(*child, inputType));
      }

      // std::cerr << "agg->return_type: " << agg->return_type.ToString() << std::endl;
      return std::make_shared<CallTypedExpr>(
          duckdb::toVeloxType(agg->return_type),
          std::move(children),
          mapAggregateFunctionName(agg->function.name));
    }
    default:
      VELOX_NYI(
          "Expression type {} is not supported yet: {}",
          ::duckdb::ExpressionTypeToString(expression.type),
          expression.ToString());
  }
}

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalFilter& logicalFilter,
    memory::MemoryPool* pool,
    std::vector<PlanNodePtr> sources,
    QueryContext& queryContext) {
  VELOX_CHECK_EQ(1, logicalFilter.expressions.size());
  auto filter = toVeloxExpression(
      *logicalFilter.expressions.front(), sources[0]->outputType());
  return std::make_shared<FilterNode>(
      queryContext.nextNodeId(), std::move(filter), std::move(sources[0]));
}

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalProjection& logicalProjection,
    memory::MemoryPool* pool,
    std::vector<PlanNodePtr> sources,
    QueryContext& queryContext) {
  std::vector<TypedExprPtr> projections;
  for (auto& expression : logicalProjection.expressions) {
    projections.push_back(
        toVeloxExpression(*expression, sources[0]->outputType()));
  }

  // TODO Figure out how to use these.
  auto columnBindings = logicalProjection.GetColumnBindings();

  std::vector<std::string> names;
  for (auto i = 0; i < projections.size(); ++i) {
    names.push_back(queryContext.nextColumnName("_p"));
  }
  return std::make_shared<ProjectNode>(
      queryContext.nextNodeId(),
      std::move(names),
      std::move(projections),
      std::move(sources[0]));
}

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalAggregate& logicalAggregate,
    memory::MemoryPool* pool,
    std::vector<PlanNodePtr> sources,
    QueryContext& queryContext) {
  std::vector<AggregationNode::Aggregate> aggregates;

  std::vector<std::string> projectNames;
  std::vector<TypedExprPtr> projections;

  bool identityProjection = true;
  for (auto& expression : logicalAggregate.expressions) {
    auto call = std::dynamic_pointer_cast<const CallTypedExpr>(
        toVeloxExpression(*expression, sources[0]->outputType()));
    std::vector<TypedExprPtr> fieldInputs;
    std::vector<TypePtr> rawInputTypes;

    for (auto& input : call->inputs()) {
      projections.push_back(input);
      rawInputTypes.push_back(input->type());

      if (auto field =
              std::dynamic_pointer_cast<const FieldAccessTypedExpr>(input)) {
        projectNames.push_back(field->name());
        fieldInputs.push_back(field);
      } else {
        identityProjection = false;
        projectNames.push_back(queryContext.nextColumnName("_p"));
        fieldInputs.push_back(std::make_shared<FieldAccessTypedExpr>(
            input->type(), projectNames.back()));
      }
    }

    // std::cerr << call->type()->toString() << " " << call->name() << std::endl;

    aggregates.push_back({
        std::make_shared<CallTypedExpr>(
            call->type(), fieldInputs, call->name()),
        rawInputTypes,
        nullptr, // mask
        {}, // sortingKeys
        {} // sortingOrders
    });
  }

  std::vector<FieldAccessTypedExprPtr> groupingKeys;
  for (auto& expression : logicalAggregate.groups) {
    auto groupingExpr =
        toVeloxExpression(*expression, sources[0]->outputType());
    projections.push_back(groupingExpr);
    if (auto field = std::dynamic_pointer_cast<const FieldAccessTypedExpr>(
            groupingExpr)) {
      projectNames.push_back(field->name());
      groupingKeys.push_back(field);
    } else {
      identityProjection = false;
      projectNames.push_back(queryContext.nextColumnName("_p"));
      groupingKeys.push_back(std::make_shared<FieldAccessTypedExpr>(
          groupingExpr->type(), projectNames.back()));
    }
  }

  auto source = sources[0];

  if (!identityProjection) {
    source = std::make_shared<ProjectNode>(
        queryContext.nextNodeId(),
        std::move(projectNames),
        std::move(projections),
        std::move(sources[0]));
  }

  std::vector<std::string> names;
  for (auto i = 0; i < aggregates.size(); ++i) {
    names.push_back(queryContext.nextColumnName("_a"));
  }

  return std::make_shared<AggregationNode>(
      queryContext.nextNodeId(),
      AggregationNode::Step::kSingle,
      groupingKeys,
      std::vector<FieldAccessTypedExprPtr>{}, // preGroupedKeys
      names,
      std::move(aggregates),
      false, // ignoreNullKeys
      source);
}

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalCrossProduct& logicalCrossProduct,
    memory::MemoryPool* pool,
    std::vector<PlanNodePtr> sources,
    QueryContext& queryContext) {
  VELOX_CHECK_EQ(2, sources.size());

  const auto& leftInputType = sources[0]->outputType()->asRow();
  const auto& rightInputType = sources[1]->outputType()->asRow();

  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (auto i = 0; i < leftInputType.size(); ++i) {
    names.push_back(leftInputType.nameOf(i));
    types.push_back(leftInputType.childAt(i));
  }
  for (auto i = 0; i < rightInputType.size(); ++i) {
    names.push_back(rightInputType.nameOf(i));
    types.push_back(rightInputType.childAt(i));
  }

  return std::make_shared<NestedLoopJoinNode>(
      queryContext.nextNodeId(),
      std::move(sources[0]),
      std::move(sources[1]),
      ROW(std::move(names), std::move(types)));
}

PlanNodePtr toVeloxPlan(
    ::duckdb::LogicalOperator& plan,
    memory::MemoryPool* pool,
    QueryContext& queryContext) {
  std::vector<PlanNodePtr> sources;
  for (auto& child : plan.children) {
    sources.push_back(toVeloxPlan(*child, pool, queryContext));
  }

  switch (plan.type) {
    case ::duckdb::LogicalOperatorType::LOGICAL_DUMMY_SCAN:
      return toVeloxPlan(
          dynamic_cast<::duckdb::LogicalDummyScan&>(plan), pool, queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_GET:
      return toVeloxPlan(
          dynamic_cast<::duckdb::LogicalGet&>(plan),
          pool,
          std::move(sources),
          queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_FILTER:
      return toVeloxPlan(
          dynamic_cast<::duckdb::LogicalFilter&>(plan),
          pool,
          std::move(sources),
          queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_PROJECTION:
      return toVeloxPlan(
          dynamic_cast<::duckdb::LogicalProjection&>(plan),
          pool,
          std::move(sources),
          queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
      return toVeloxPlan(
          dynamic_cast<::duckdb::LogicalAggregate&>(plan),
          pool,
          std::move(sources),
          queryContext);
    case ::duckdb::LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
      return toVeloxPlan(
          dynamic_cast<::duckdb::LogicalCrossProduct&>(plan),
          pool,
          std::move(sources),
          queryContext);
    default:
      VELOX_NYI(
          "Plan node is not supported yet: {}",
          ::duckdb::LogicalOperatorToString(plan.type));
  }
}

static void customScalarFunction(
    ::duckdb::DataChunk& args,
    ::duckdb::ExpressionState& state,
    ::duckdb::Vector& result) {
  VELOX_UNREACHABLE();
}

static ::duckdb::idx_t customAggregateState() {
  VELOX_UNREACHABLE();
}

static void customAggregateInitialize(::duckdb::data_ptr_t) {
  VELOX_UNREACHABLE();
}

static void customAggregateUpdate(
    ::duckdb::Vector inputs[],
    ::duckdb::AggregateInputData& aggr_input_data,
    ::duckdb::idx_t input_count,
    ::duckdb::Vector& state,
    ::duckdb::idx_t count) {
  VELOX_UNREACHABLE();
}

static void customAggregateCombine(
    ::duckdb::Vector& state,
    ::duckdb::Vector& combined,
    ::duckdb::AggregateInputData& aggr_input_data,
    ::duckdb::idx_t count) {
  VELOX_UNREACHABLE();
}

static void customAggregateFinalize(
    ::duckdb::Vector& state,
    ::duckdb::AggregateInputData& aggr_input_data,
    ::duckdb::Vector& result,
    ::duckdb::idx_t count,
    ::duckdb::idx_t offset) {
  VELOX_UNREACHABLE();
}

} // namespace

PlanNodePtr parseQuery(
    const std::string& sql,
    memory::MemoryPool* pool,
    const std::unordered_map<std::string, std::vector<RowVectorPtr>>&
        inMemoryTables) {
  DuckDbQueryPlanner planner(pool);

  for (auto& [name, data] : inMemoryTables) {
    planner.registerTable(name, data);
  }

  return planner.plan(sql);
}

std::shared_ptr<RowType> ConstructDataColumns(const std::vector<SplitInfo>& splitInfos) {
  std::unordered_map<std::string, TypePtr> mergedColumns;

  for (const auto& splitInfo : splitInfos) {
    const auto& rowType = splitInfo.row_type_;

    const auto& names = rowType.names();
    const auto& types = rowType.children();

    for (size_t i = 0; i < names.size(); ++i) {
      const auto& name = names[i];
      const auto& type = types[i];
      if (mergedColumns.find(name) == mergedColumns.end()) {
        mergedColumns[name] = type;
      } else {
        // 同名的情况下需要考虑类型冲突，暂时不考虑
      }
    }
  }

  std::vector<std::string> finalNames;
  std::vector<TypePtr> finalTypes;

  for (const auto& pair : mergedColumns) {
    finalNames.push_back(pair.first);
    finalTypes.push_back(pair.second);
  }

  return std::make_shared<RowType>(std::move(finalNames), std::move(finalTypes));
}

PlanNodePtr parseQuery(
    const std::string& sql,
    memory::MemoryPool* pool,
    const std::unordered_map<std::string, TableScanInfo>&
        hiveDataSources) {
  DuckDbQueryPlanner planner(pool);

  for (auto& [name, tableScanInfo] : hiveDataSources) {
    planner.registerTableScan(name, tableScanInfo);
  }

  return planner.plan(sql);
}

void DuckDbQueryPlanner::registerTableScan(
      const std::string& name,
      const TableScanInfo& tableScanInfo) {
  VELOX_CHECK_EQ(
      0, tableScans_.count(name), "TableScan is already registered: {}", name);

  auto createTableSql =
      duckdb::makeCreateTableSql(name, *tableScanInfo.dataColumns_);
  auto res = conn_.Query(createTableSql);
  VELOX_CHECK(
      !res->HasError(), "Failed to create DuckDB table: {}", res->GetError());

  tableScans_.insert({name, tableScanInfo});
}

void DuckDbQueryPlanner::registerTable(
    const std::string& name,
    const std::vector<RowVectorPtr>& data) {
  VELOX_CHECK_EQ(
      0, tables_.count(name), "Table is already registered: {}", name);

  auto createTableSql =
      duckdb::makeCreateTableSql(name, *asRowType(data[0]->type()));
  auto res = conn_.Query(createTableSql);
  VELOX_CHECK(
      !res->HasError(), "Failed to create DuckDB table: {}", res->GetError());

  tables_.insert({name, data});
}

void DuckDbQueryPlanner::registerScalarFunction(
    const std::string& name,
    const std::vector<TypePtr>& argTypes,
    const TypePtr& returnType) {
  ::duckdb::vector<::duckdb::LogicalType> argDuckTypes;
  for (auto& type : argTypes) {
    argDuckTypes.push_back(duckdb::fromVeloxType(type));
  }

  conn_.CreateVectorizedFunction(
      name,
      argDuckTypes,
      duckdb::fromVeloxType(returnType),
      customScalarFunction);
}

void DuckDbQueryPlanner::registerAggregateFunction(
    const std::string& name,
    const std::vector<TypePtr>& argTypes,
    const TypePtr& returnType) {
  ::duckdb::vector<::duckdb::LogicalType> argDuckTypes;
  for (auto& type : argTypes) {
    argDuckTypes.push_back(duckdb::fromVeloxType(type));
  }

  conn_.CreateAggregateFunction(
      name,
      argDuckTypes,
      duckdb::fromVeloxType(returnType),
      customAggregateState,
      customAggregateInitialize,
      customAggregateUpdate,
      customAggregateCombine,
      customAggregateFinalize);
}

PlanNodePtr DuckDbQueryPlanner::plan(const std::string& sql) {
  // Disable the optimizer. Otherwise, the filter over table scan gets pushdown
  // as a callback that is impossible to recover.
  conn_.Query("PRAGMA disable_optimizer");

  auto plan = conn_.ExtractPlan(sql);

  QueryContext queryContext{tables_, tableScans_};
  return toVeloxPlan(*plan, pool_, queryContext);
}

std::unique_ptr<::duckdb::LogicalOperator>
  DuckDbQueryPlanner::duckPlan(const std::string& sql) {
  conn_.Query("PRAGMA disable_optimizer");
  return conn_.ExtractPlan(sql);
}

PlanNodePtr DuckDbQueryPlanner::duckPlanConvertVeloxPlan(const std::unique_ptr<::duckdb::LogicalOperator>& duckdb_plan) {
  QueryContext queryContext{tables_, tableScans_};
  return toVeloxPlan(*duckdb_plan, pool_, queryContext);
}
} // namespace facebook::velox::core
