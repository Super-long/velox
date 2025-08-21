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

#include "velox/connectors/hive/HiveDataSink.h"

#include "velox/common/base/Counters.h"
#include "velox/common/base/Fs.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/connectors/hive/HivePartitionFunction.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/core/ITypedExpr.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/SortingWriter.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/SortBuffer.h"

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <algorithm>
#include <cctype>
#include <sstream>

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::connector::hive {
namespace {
#define WRITER_NON_RECLAIMABLE_SECTION_GUARD(index)       \
  memory::NonReclaimableSectionGuard nonReclaimableGuard( \
      writerInfo_[(index)]->nonReclaimableSectionHolder.get())

// Returns the type of non-partition data columns.
RowTypePtr getNonPartitionTypes(
    const std::vector<column_index_t>& dataCols,
    const RowTypePtr& inputType) {
  std::vector<std::string> childNames;
  std::vector<TypePtr> childTypes;
  const auto& dataSize = dataCols.size();
  childNames.reserve(dataSize);
  childTypes.reserve(dataSize);
  for (int dataCol : dataCols) {
    childNames.push_back(inputType->nameOf(dataCol));
    childTypes.push_back(inputType->childAt(dataCol));
  }

  return ROW(std::move(childNames), std::move(childTypes));
}

// Filters out partition columns if there is any.
RowVectorPtr makeDataInput(
    const std::vector<column_index_t>& dataCols,
    const RowVectorPtr& input) {
  std::vector<VectorPtr> childVectors;
  childVectors.reserve(dataCols.size());
  for (int dataCol : dataCols) {
    childVectors.push_back(input->childAt(dataCol));
  }

  return std::make_shared<RowVector>(
      input->pool(),
      getNonPartitionTypes(dataCols, asRowType(input->type())),
      input->nulls(),
      input->size(),
      std::move(childVectors),
      input->getNullCount());
}

// Returns a subset of column indices corresponding to partition keys.
std::vector<column_index_t> getPartitionChannels(
    const std::shared_ptr<const HiveInsertTableHandle>& insertTableHandle) {
  std::vector<column_index_t> channels;

  for (column_index_t i = 0; i < insertTableHandle->inputColumns().size();
       i++) {
    if (insertTableHandle->inputColumns()[i]->isPartitionKey()) {
      channels.push_back(i);
    }
  }

  return channels;
}

// Returns the column indices of non-partition data columns.
std::vector<column_index_t> getNonPartitionChannels(
    const std::vector<column_index_t>& partitionChannels,
    const column_index_t childrenSize) {
  std::vector<column_index_t> dataChannels;
  dataChannels.reserve(childrenSize - partitionChannels.size());

  for (column_index_t i = 0; i < childrenSize; i++) {
    if (std::find(partitionChannels.cbegin(), partitionChannels.cend(), i) ==
        partitionChannels.cend()) {
      dataChannels.push_back(i);
    }
  }

  return dataChannels;
}

std::string makePartitionDirectory(
    const std::string& tableDirectory,
    const std::optional<std::string>& partitionSubdirectory) {
  if (partitionSubdirectory.has_value()) {
    return fs::path(tableDirectory) / partitionSubdirectory.value();
  }
  return tableDirectory;
}

std::string makeUuid() {
  return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
}

std::unordered_map<LocationHandle::TableType, std::string> tableTypeNames() {
  return {
      {LocationHandle::TableType::kNew, "kNew"},
      {LocationHandle::TableType::kExisting, "kExisting"},
  };
}

template <typename K, typename V>
std::unordered_map<V, K> invertMap(const std::unordered_map<K, V>& mapping) {
  std::unordered_map<V, K> inverted;
  for (const auto& [key, value] : mapping) {
    inverted.emplace(value, key);
  }
  return inverted;
}

std::unique_ptr<core::PartitionFunction> createBucketFunction(
    const HiveBucketProperty& bucketProperty,
    const RowTypePtr& inputType) {
  const auto& bucketedBy = bucketProperty.bucketedBy();
  const auto& bucketedTypes = bucketProperty.bucketedTypes();
  std::vector<column_index_t> bucketedByChannels;
  bucketedByChannels.reserve(bucketedBy.size());
  for (int32_t i = 0; i < bucketedBy.size(); ++i) {
    const auto& bucketColumn = bucketedBy[i];
    const auto& bucketType = bucketedTypes[i];
    const auto inputChannel = inputType->getChildIdx(bucketColumn);
    if (FOLLY_UNLIKELY(
            !inputType->childAt(inputChannel)->equivalent(*bucketType))) {
      VELOX_USER_FAIL(
          "Input column {} type {} doesn't match bucket type {}",
          inputType->nameOf(inputChannel),
          inputType->childAt(inputChannel)->toString(),
          bucketType->toString());
    }
    bucketedByChannels.push_back(inputChannel);
  }
  return std::make_unique<HivePartitionFunction>(
      bucketProperty.bucketCount(), bucketedByChannels);
}

std::string computeBucketedFileName(
    const std::string& queryId,
    int32_t bucket) {
  static const uint32_t kMaxBucketCountPadding =
      std::to_string(HiveDataSink::maxBucketCount() - 1).size();
  const std::string bucketValueStr = std::to_string(bucket);
  return fmt::format(
      "0{:0>{}}_0_{}", bucketValueStr, kMaxBucketCountPadding, queryId);
}

std::shared_ptr<memory::MemoryPool> createSinkPool(
    const std::shared_ptr<memory::MemoryPool>& writerPool) {
  return writerPool->addLeafChild(fmt::format("{}.sink", writerPool->name()));
}

std::shared_ptr<memory::MemoryPool> createSortPool(
    const std::shared_ptr<memory::MemoryPool>& writerPool) {
  return writerPool->addLeafChild(fmt::format("{}.sort", writerPool->name()));
}

uint64_t getFinishTimeSliceLimitMsFromHiveConfig(
    const std::shared_ptr<const HiveConfig>& config,
    const config::ConfigBase* sessions) {
  const uint64_t flushTimeSliceLimitMsFromConfig =
      config->sortWriterFinishTimeSliceLimitMs(sessions);
  // NOTE: if the flush time slice limit is set to 0, then we treat it as no
  // limit.
  return flushTimeSliceLimitMsFromConfig == 0
      ? std::numeric_limits<uint64_t>::max()
      : flushTimeSliceLimitMsFromConfig;
}

} // namespace

const HiveWriterId& HiveWriterId::unpartitionedId() {
  static const HiveWriterId writerId{0};
  return writerId;
}

std::string HiveWriterId::toString() const {
  if (partitionId.has_value() && bucketId.has_value()) {
    return fmt::format("part[{}.{}]", partitionId.value(), bucketId.value());
  }

  if (partitionId.has_value() && !bucketId.has_value()) {
    return fmt::format("part[{}]", partitionId.value());
  }

  // This WriterId is used to add an identifier in the MemoryPools. This could
  // indicate unpart, but the bucket number needs to be disambiguated. So
  // creating a new label using bucket.
  if (!partitionId.has_value() && bucketId.has_value()) {
    return fmt::format("bucket[{}]", bucketId.value());
  }

  return "unpart";
}

const std::string LocationHandle::tableTypeName(
    LocationHandle::TableType type) {
  static const auto tableTypes = tableTypeNames();
  return tableTypes.at(type);
}

LocationHandle::TableType LocationHandle::tableTypeFromName(
    const std::string& name) {
  static const auto nameTableTypes = invertMap(tableTypeNames());
  return nameTableTypes.at(name);
}

HiveSortingColumn::HiveSortingColumn(
    const std::string& sortColumn,
    const core::SortOrder& sortOrder)
    : sortColumn_(sortColumn), sortOrder_(sortOrder) {
  VELOX_USER_CHECK(!sortColumn_.empty(), "hive sort column must be set");

  if (FOLLY_UNLIKELY(
          (sortOrder_.isAscending() && !sortOrder_.isNullsFirst()) ||
          (!sortOrder_.isAscending() && sortOrder_.isNullsFirst()))) {
    VELOX_USER_FAIL("Bad hive sort order: {}", toString());
  }
}

folly::dynamic HiveSortingColumn::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "HiveSortingColumn";
  obj["columnName"] = sortColumn_;
  obj["sortOrder"] = sortOrder_.serialize();
  return obj;
}

std::shared_ptr<HiveSortingColumn> HiveSortingColumn::deserialize(
    const folly::dynamic& obj,
    void* context) {
  const std::string columnName = obj["columnName"].asString();
  const auto sortOrder = core::SortOrder::deserialize(obj["sortOrder"]);
  return std::make_shared<HiveSortingColumn>(columnName, sortOrder);
}

std::string HiveSortingColumn::toString() const {
  return fmt::format(
      "[COLUMN[{}] ORDER[{}]]", sortColumn_, sortOrder_.toString());
}

void HiveSortingColumn::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("HiveSortingColumn", HiveSortingColumn::deserialize);
}

HiveBucketProperty::HiveBucketProperty(
    Kind kind,
    int32_t bucketCount,
    const std::vector<std::string>& bucketedBy,
    const std::vector<TypePtr>& bucketTypes,
    const std::vector<std::shared_ptr<const HiveSortingColumn>>& sortedBy)
    : kind_(kind),
      bucketCount_(bucketCount),
      bucketedBy_(bucketedBy),
      bucketTypes_(bucketTypes),
      sortedBy_(sortedBy) {
  validate();
}

void HiveBucketProperty::validate() const {
  VELOX_USER_CHECK_GT(bucketCount_, 0, "Hive bucket count can't be zero");
  VELOX_USER_CHECK(!bucketedBy_.empty(), "Hive bucket columns must be set");
  VELOX_USER_CHECK_EQ(
      bucketedBy_.size(),
      bucketTypes_.size(),
      "The number of hive bucket columns and types do not match {}",
      toString());
}

std::string HiveBucketProperty::kindString(Kind kind) {
  switch (kind) {
    case Kind::kHiveCompatible:
      return "HIVE_COMPATIBLE";
    case Kind::kPrestoNative:
      return "PRESTO_NATIVE";
    default:
      return fmt::format("UNKNOWN {}", static_cast<int>(kind));
  }
}

folly::dynamic HiveBucketProperty::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "HiveBucketProperty";
  obj["kind"] = static_cast<int64_t>(kind_);
  obj["bucketCount"] = bucketCount_;
  obj["bucketedBy"] = ISerializable::serialize(bucketedBy_);
  obj["bucketedTypes"] = ISerializable::serialize(bucketTypes_);
  obj["sortedBy"] = ISerializable::serialize(sortedBy_);
  return obj;
}

std::shared_ptr<HiveBucketProperty> HiveBucketProperty::deserialize(
    const folly::dynamic& obj,
    void* context) {
  const Kind kind = static_cast<Kind>(obj["kind"].asInt());
  const int32_t bucketCount = obj["bucketCount"].asInt();
  const auto buckectedBy =
      ISerializable::deserialize<std::vector<std::string>>(obj["bucketedBy"]);
  const auto bucketedTypes = ISerializable::deserialize<std::vector<Type>>(
      obj["bucketedTypes"], context);
  const auto sortedBy =
      ISerializable::deserialize<std::vector<HiveSortingColumn>>(
          obj["sortedBy"], context);
  return std::make_shared<HiveBucketProperty>(
      kind, bucketCount, buckectedBy, bucketedTypes, sortedBy);
}

void HiveBucketProperty::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("HiveBucketProperty", HiveBucketProperty::deserialize);
}

std::string HiveBucketProperty::toString() const {
  std::stringstream out;
  out << "\nHiveBucketProperty[<" << kind_ << " " << bucketCount_ << ">\n";
  out << "\tBucket Columns:\n";
  for (const auto& column : bucketedBy_) {
    out << "\t\t" << column << "\n";
  }
  out << "\tBucket Types:\n";
  for (const auto& type : bucketTypes_) {
    out << "\t\t" << type->toString() << "\n";
  }
  if (!sortedBy_.empty()) {
    out << "\tSortedBy Columns:\n";
    for (const auto& sortColum : sortedBy_) {
      out << "\t\t" << sortColum->toString() << "\n";
    }
  }
  out << "]\n";
  return out.str();
}

HiveDataSink::HiveDataSink(
    RowTypePtr inputType,
    std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy,
    const std::shared_ptr<const HiveConfig>& hiveConfig)
    : inputType_(std::move(inputType)),
      insertTableHandle_(std::move(insertTableHandle)),
      connectorQueryCtx_(connectorQueryCtx),
      commitStrategy_(commitStrategy),
      hiveConfig_(hiveConfig),
      updateMode_(getUpdateMode()),
      maxOpenWriters_(hiveConfig_->maxPartitionsPerWriters(
          connectorQueryCtx->sessionProperties())),
      partitionChannels_(getPartitionChannels(insertTableHandle_)),
      partitionIdGenerator_(
          !partitionChannels_.empty()
              ? std::make_unique<PartitionIdGenerator>(
                    inputType_,
                    partitionChannels_,
                    maxOpenWriters_,
                    connectorQueryCtx_->memoryPool(),
                    hiveConfig_->isPartitionPathAsLowerCase(
                        connectorQueryCtx->sessionProperties()))
              : nullptr),
      dataChannels_(
          getNonPartitionChannels(partitionChannels_, inputType_->size())),
      bucketCount_(
          insertTableHandle_->bucketProperty() == nullptr
              ? 0
              : insertTableHandle_->bucketProperty()->bucketCount()),
      bucketFunction_(
          isBucketed() ? createBucketFunction(
                             *insertTableHandle_->bucketProperty(),
                             inputType_)
                       : nullptr),
      writerFactory_(dwio::common::getWriterFactory(
          insertTableHandle_->tableStorageFormat())),
      spillConfig_(connectorQueryCtx->spillConfig()),
      sortWriterFinishTimeSliceLimitMs_(getFinishTimeSliceLimitMsFromHiveConfig(
          hiveConfig_,
          connectorQueryCtx->sessionProperties())),
      compactModeEnabled_(insertTableHandle_->compactProperty() != nullptr),
      compactFileSizeThreshold_(
          insertTableHandle_->compactProperty() != nullptr
              ? insertTableHandle_->compactProperty()->fileSizeThreshold()
              : 0),
      compactSortKeyColumns_(
          insertTableHandle_->compactProperty() != nullptr
              ? insertTableHandle_->compactProperty()->sortKeyColumns()
              : std::vector<std::string>()),
      compactTimeColumn_(
          insertTableHandle_->compactProperty() != nullptr
              ? insertTableHandle_->compactProperty()->timeColumn()
              : "") {
  // Initialize custom file name generator if provided
  if (isCompactMode() &&
      insertTableHandle_->compactProperty()->hasCustomFileNameGenerator()) {
    customFileNameGenerator_ =
        insertTableHandle_->compactProperty()->fileNameGenerator();
  }

  // Initialize compact mode if enabled
  if (isCompactMode()) {
    VELOX_USER_CHECK(
        !isPartitioned() && !isBucketed(),
        "Compact mode cannot be used with partitioned or bucketed tables");

    // Initialize sort key column indices
    const auto nonPartitionTypes =
        getNonPartitionTypes(dataChannels_, inputType_);
    const auto& sortKeyColumns =
        insertTableHandle_->compactProperty()->sortKeyColumns();
    VELOX_USER_CHECK(
        !sortKeyColumns.empty(),
        "Sort key columns must be set when compact mode is enabled");
    for (const auto& columnName : sortKeyColumns) {
      auto columnIndex = nonPartitionTypes->getChildIdxIfExists(columnName);
      if (columnIndex.has_value()) {
        compactSortKeyColumnIndices_.push_back(columnIndex.value());

        // Validate that sort key column is VARBINARY type
        auto sortKeyColumnType =
            nonPartitionTypes->childAt(columnIndex.value());
        if (sortKeyColumnType->kind() != TypeKind::VARBINARY) {
          VELOX_USER_FAIL(
              "Sort key column '{}' must be VARBINARY type, but got '{}'",
              columnName,
              sortKeyColumnType->toString());
        }
      } else {
        VELOX_USER_FAIL("Sort key column '{}' not found in input", columnName);
      }
    }

    // Initialize time column index and validate type
    compactTimeColumnIndex_ =
        static_cast<column_index_t>(-1); // Initialize to invalid
    const auto& timeColumn =
        insertTableHandle_->compactProperty()->timeColumn();
    VELOX_USER_CHECK(
        !timeColumn.empty(),
        "Time column must be set when compact mode is enabled");
    if (!timeColumn.empty()) {
      auto timeColumnIndex = nonPartitionTypes->getChildIdxIfExists(timeColumn);
      if (timeColumnIndex.has_value()) {
        compactTimeColumnIndex_ = timeColumnIndex.value();

        // Validate that time column is BIGINT type
        auto timeColumnType =
            nonPartitionTypes->childAt(timeColumnIndex.value());
        if (timeColumnType->kind() != TypeKind::BIGINT) {
          VELOX_USER_FAIL(
              "Time column '{}' must be BIGINT type, but got '{}'",
              timeColumn,
              timeColumnType->toString());
        }
      } else {
        VELOX_USER_FAIL("Time column '{}' not found in input", timeColumn);
      }
    }

    return;
  }

  if (!isBucketed()) {
    return;
  }
  const auto& sortedProperty = insertTableHandle_->bucketProperty()->sortedBy();
  if (!sortedProperty.empty()) {
    sortColumnIndices_.reserve(sortedProperty.size());
    sortCompareFlags_.reserve(sortedProperty.size());
    for (int i = 0; i < sortedProperty.size(); ++i) {
      auto columnIndex =
          getNonPartitionTypes(dataChannels_, inputType_)
              ->getChildIdxIfExists(sortedProperty.at(i)->sortColumn());
      if (columnIndex.has_value()) {
        sortColumnIndices_.push_back(columnIndex.value());
        sortCompareFlags_.push_back(
            {sortedProperty.at(i)->sortOrder().isNullsFirst(),
             sortedProperty.at(i)->sortOrder().isAscending(),
             false,
             CompareFlags::NullHandlingMode::kNullAsValue});
      }
    }
  }
}

bool HiveDataSink::canReclaim() const {
  // Currently, we only support memory reclaim on dwrf file writer.
  return (spillConfig_ != nullptr) &&
      (insertTableHandle_->tableStorageFormat() ==
       dwio::common::FileFormat::DWRF);
}

void HiveDataSink::appendData(RowVectorPtr input) {
  checkRunning();

  // Handle compact mode
  if (isCompactMode()) {
    // Check if we need to switch to a new file
    if (shouldSwitchFile()) {
      switchToNewFile();
    }

    // Ensure we have a writer
    if (writers_.empty()) {
      switchToNewFile();
    }

    // Write data and update statistics
    write(currentWriterIndex_, input);
    updateFileStats(currentWriterIndex_, input);
    return;
  }

  // Write to unpartitioned (and unbucketed) table.
  if (!isPartitioned() && !isBucketed()) {
    const auto index = ensureWriter(HiveWriterId::unpartitionedId());
    write(index, input);
    return;
  }

  // Compute partition and bucket numbers.
  computePartitionAndBucketIds(input);

  // Lazy load all the input columns.
  for (column_index_t i = 0; i < input->childrenSize(); ++i) {
    input->childAt(i)->loadedVector();
  }

  // All inputs belong to a single non-bucketed partition. The partition id
  // must be zero.
  if (!isBucketed() && partitionIdGenerator_->numPartitions() == 1) {
    const auto index = ensureWriter(HiveWriterId{0});
    write(index, input);
    return;
  }

  splitInputRowsAndEnsureWriters();

  for (auto index = 0; index < writers_.size(); ++index) {
    const vector_size_t partitionSize = partitionSizes_[index];
    if (partitionSize == 0) {
      continue;
    }

    RowVectorPtr writerInput = partitionSize == input->size()
        ? input
        : exec::wrap(partitionSize, partitionRows_[index], input);
    write(index, writerInput);
  }
}

void HiveDataSink::write(size_t index, RowVectorPtr input) {
  WRITER_NON_RECLAIMABLE_SECTION_GUARD(index);
  auto dataInput = makeDataInput(dataChannels_, input);

  writers_[index]->write(dataInput);
  writerInfo_[index]->numWrittenRows += dataInput->size();
}

std::string HiveDataSink::stateString(State state) {
  switch (state) {
    case State::kRunning:
      return "RUNNING";
    case State::kFinishing:
      return "FLUSHING";
    case State::kClosed:
      return "CLOSED";
    case State::kAborted:
      return "ABORTED";
    default:
      VELOX_UNREACHABLE("BAD STATE: {}", static_cast<int>(state));
  }
}

void HiveDataSink::computePartitionAndBucketIds(const RowVectorPtr& input) {
  VELOX_CHECK(isPartitioned() || isBucketed());
  if (isPartitioned()) {
    if (!hiveConfig_->allowNullPartitionKeys(
            connectorQueryCtx_->sessionProperties())) {
      // Check that there are no nulls in the partition keys.
      for (auto& partitionIdx : partitionChannels_) {
        auto col = input->childAt(partitionIdx);
        if (col->mayHaveNulls()) {
          for (auto i = 0; i < col->size(); ++i) {
            VELOX_USER_CHECK(
                !col->isNullAt(i),
                "Partition key must not be null: {}",
                input->type()->asRow().nameOf(partitionIdx));
          }
        }
      }
    }
    partitionIdGenerator_->run(input, partitionIds_);
  }

  if (isBucketed()) {
    bucketFunction_->partition(*input, bucketIds_);
  }
}

DataSink::Stats HiveDataSink::stats() const {
  Stats stats;
  if (state_ == State::kAborted) {
    return stats;
  }

  int64_t numWrittenBytes{0};
  int64_t writeIOTimeUs{0};
  for (const auto& ioStats : ioStats_) {
    numWrittenBytes += ioStats->rawBytesWritten();
    writeIOTimeUs += ioStats->writeIOTimeUs();
  }
  stats.numWrittenBytes = numWrittenBytes;
  stats.writeIOTimeUs = writeIOTimeUs;

  if (state_ != State::kClosed) {
    return stats;
  }

  stats.numWrittenFiles = writers_.size();
  for (int i = 0; i < writerInfo_.size(); ++i) {
    const auto& info = writerInfo_.at(i);
    VELOX_CHECK_NOT_NULL(info);
    const auto spillStats = info->spillStats->rlock();
    if (!spillStats->empty()) {
      stats.spillStats += *spillStats;
    }
  }
  return stats;
}

std::shared_ptr<memory::MemoryPool> HiveDataSink::createWriterPool(
    const HiveWriterId& writerId) {
  auto* connectorPool = connectorQueryCtx_->connectorMemoryPool();
  return connectorPool->addAggregateChild(
      fmt::format("{}.{}", connectorPool->name(), writerId.toString()));
}

void HiveDataSink::setMemoryReclaimers(
    HiveWriterInfo* writerInfo,
    io::IoStatistics* ioStats) {
  auto* connectorPool = connectorQueryCtx_->connectorMemoryPool();
  if (connectorPool->reclaimer() == nullptr) {
    return;
  }
  writerInfo->writerPool->setReclaimer(
      WriterReclaimer::create(this, writerInfo, ioStats));
  writerInfo->sinkPool->setReclaimer(exec::MemoryReclaimer::create());
  // NOTE: we set the memory reclaimer for sort pool when we construct the sort
  // writer.
}

void HiveDataSink::setState(State newState) {
  checkStateTransition(state_, newState);
  state_ = newState;
}

/// Validates the state transition from 'oldState' to 'newState'.
void HiveDataSink::checkStateTransition(State oldState, State newState) {
  switch (oldState) {
    case State::kRunning:
      if (newState == State::kAborted || newState == State::kFinishing) {
        return;
      }
      break;
    case State::kFinishing:
      if (newState == State::kAborted || newState == State::kClosed ||
          // The finishing state is reentry state if we yield in the middle of
          // finish processing if a single run takes too long.
          newState == State::kFinishing) {
        return;
      }
      [[fallthrough]];
    case State::kAborted:
    case State::kClosed:
    default:
      break;
  }
  VELOX_FAIL("Unexpected state transition from {} to {}", oldState, newState);
}

bool HiveDataSink::finish() {
  // Flush is reentry state.
  setState(State::kFinishing);

  // As for now, only sorted writer needs flush buffered data. For non-sorted
  // writer, data is directly written to the underlying file writer.
  if (!sortWrite()) {
    return true;
  }

  // TODO: we might refactor to move the data sorting logic into hive data sink.
  const uint64_t startTimeMs = getCurrentTimeMs();
  for (auto i = 0; i < writers_.size(); ++i) {
    WRITER_NON_RECLAIMABLE_SECTION_GUARD(i);
    if (!writers_[i]->finish()) {
      return false;
    }
    if (getCurrentTimeMs() - startTimeMs > sortWriterFinishTimeSliceLimitMs_) {
      return false;
    }
  }
  return true;
}

std::vector<std::string> HiveDataSink::close() {
  setState(State::kClosed);

  // Update final file sizes for compact mode
  if (isCompactMode()) {
    for (size_t i = 0; i < compactFileStats_.size() && i < ioStats_.size();
         ++i) {
      compactFileStats_[i].fileSize = ioStats_[i]->rawBytesWritten();
    }
  }

  closeInternal();

  std::vector<std::string> partitionUpdates;
  partitionUpdates.reserve(writerInfo_.size());
  for (int i = 0; i < writerInfo_.size(); ++i) {
    const auto& info = writerInfo_.at(i);
    VELOX_CHECK_NOT_NULL(info);

    std::string partitionUpdateJson;

    if (isCompactMode() && i < compactFileStats_.size()) {
      // Compact mode: include statistics information
      const auto& stats = compactFileStats_[i];

      // Create sort key statistics
      folly::dynamic sortKeyStats = folly::dynamic::object;
      const auto& sortKeyColumns =
          insertTableHandle_->compactProperty()->sortKeyColumns();

      if (stats.sortKeyInitialized && !sortKeyColumns.empty()) {
        folly::dynamic minValues = folly::dynamic::array;
        folly::dynamic maxValues = folly::dynamic::array;

        for (size_t i = 0;
             i < sortKeyColumns.size() && i < stats.sortKeyMinValues.size() &&
             i < stats.sortKeyMaxValues.size(); ++i) {
          const auto& minValue = stats.sortKeyMinValues[i];
          const auto& maxValue = stats.sortKeyMaxValues[i];

          if (!minValue.empty() && !maxValue.empty()) {
            minValues.push_back(minValue);
            maxValues.push_back(maxValue);
          }
        }

        if (!minValues.empty() && !maxValues.empty()) {
          sortKeyStats["min"] = minValues;
          sortKeyStats["max"] = maxValues;
        }
      }

      // Create time column statistics
      folly::dynamic timeColumnStats = folly::dynamic::object;
      if (!compactTimeColumn_.empty()) {
        timeColumnStats = folly::dynamic::object(
            "min", stats.timeColumnMinValue)("max", stats.timeColumnMaxValue);
      }

      // clang-format off
      partitionUpdateJson = folly::toJson(
        folly::dynamic::object
          ("part_index", i)
          ("writePath", info->writerParameters.writeDirectory())
          ("targetPath", info->writerParameters.targetDirectory())
          ("fileWriteInfos", folly::dynamic::object
              ("writeFileName", info->writerParameters.writeFileName())
              ("targetFileName", info->writerParameters.targetFileName())
              ("fileSize", ioStats_.at(i)->rawBytesWritten()))
          ("rowCount", info->numWrittenRows)
          ("onDiskDataSizeInBytes", ioStats_.at(i)->rawBytesWritten())
          ("compactStats", folly::dynamic::object
              ("sortKeyStats", sortKeyStats)
              ("timeColumnStats", timeColumnStats)
              ("sortKeyColumns", ISerializable::serialize(insertTableHandle_->compactProperty()->sortKeyColumns()))
              ("timeColumn", insertTableHandle_->compactProperty()->timeColumn()))
      );
    } else {
      // Standard mode: use existing logic
      // clang-format off
      partitionUpdateJson = folly::toJson(
        folly::dynamic::object
          ("name", info->writerParameters.partitionName().value_or(""))
          ("updateMode",
            HiveWriterParameters::updateModeToString(
              info->writerParameters.updateMode()))
          ("writePath", info->writerParameters.writeDirectory())
          ("targetPath", info->writerParameters.targetDirectory())
          ("fileWriteInfos", folly::dynamic::array(
            folly::dynamic::object
              ("writeFileName", info->writerParameters.writeFileName())
              ("targetFileName", info->writerParameters.targetFileName())
              ("fileSize", ioStats_.at(i)->rawBytesWritten())))
          ("rowCount", info->numWrittenRows)
          // TODO(gaoge): track and send the fields when inMemoryDataSizeInBytes
          // and containsNumberedFileNames are needed at coordinator when file_renaming_enabled are turned on.
          ("inMemoryDataSizeInBytes", 0)
          ("onDiskDataSizeInBytes", ioStats_.at(i)->rawBytesWritten())
          ("containsNumberedFileNames", true));
      // clang-format on
    }

    partitionUpdates.push_back(partitionUpdateJson);
  }
  return partitionUpdates;
}

void HiveDataSink::abort() {
  setState(State::kAborted);
  closeInternal();
}

void HiveDataSink::closeInternal() {
  VELOX_CHECK_NE(state_, State::kRunning);
  VELOX_CHECK_NE(state_, State::kFinishing);

  TestValue::adjust(
      "facebook::velox::connector::hive::HiveDataSink::closeInternal", this);

  if (state_ == State::kClosed) {
    for (int i = 0; i < writers_.size(); ++i) {
      WRITER_NON_RECLAIMABLE_SECTION_GUARD(i);
      writers_[i]->close();
    }
  } else {
    for (int i = 0; i < writers_.size(); ++i) {
      WRITER_NON_RECLAIMABLE_SECTION_GUARD(i);
      writers_[i]->abort();
    }
  }
}

uint32_t HiveDataSink::ensureWriter(const HiveWriterId& id) {
  auto it = writerIndexMap_.find(id);
  if (it != writerIndexMap_.end()) {
    return it->second;
  }
  return appendWriter(id);
}

uint32_t HiveDataSink::appendWriter(const HiveWriterId& id) {
  // Check max open writers.
  VELOX_USER_CHECK_LE(
      writers_.size(), maxOpenWriters_, "Exceeded open writer limit");
  VELOX_CHECK_EQ(writers_.size(), writerInfo_.size());
  VELOX_CHECK_EQ(writerIndexMap_.size(), writerInfo_.size());

  std::optional<std::string> partitionName;
  if (isPartitioned()) {
    partitionName =
        partitionIdGenerator_->partitionName(id.partitionId.value());
  }

  // Without explicitly setting flush policy, the default memory based flush
  // policy is used.
  auto writerParameters = getWriterParameters(partitionName, id.bucketId);
  const auto writePath = fs::path(writerParameters.writeDirectory()) /
      writerParameters.writeFileName();
  auto writerPool = createWriterPool(id);
  auto sinkPool = createSinkPool(writerPool);
  std::shared_ptr<memory::MemoryPool> sortPool{nullptr};
  if (sortWrite()) {
    sortPool = createSortPool(writerPool);
  }
  writerInfo_.emplace_back(std::make_shared<HiveWriterInfo>(
      std::move(writerParameters),
      std::move(writerPool),
      std::move(sinkPool),
      std::move(sortPool)));
  ioStats_.emplace_back(std::make_shared<io::IoStatistics>());
  setMemoryReclaimers(writerInfo_.back().get(), ioStats_.back().get());

  // Take the writer options provided by the user as a starting point, or
  // allocate a new one.
  auto options = insertTableHandle_->writerOptions();
  if (!options) {
    options = writerFactory_->createWriterOptions();
  }

  const auto* connectorSessionProperties =
      connectorQueryCtx_->sessionProperties();

  // Only overwrite options in case they were not already provided.
  if (options->schema == nullptr) {
    options->schema = getNonPartitionTypes(dataChannels_, inputType_);
  }

  if (options->memoryPool == nullptr) {
    options->memoryPool = writerInfo_.back()->writerPool.get();
  }

  if (!options->compressionKind) {
    options->compressionKind = insertTableHandle_->compressionKind();
  }

  if (options->spillConfig == nullptr && canReclaim()) {
    options->spillConfig = spillConfig_;
  }

  if (options->nonReclaimableSection == nullptr) {
    options->nonReclaimableSection =
        writerInfo_.back()->nonReclaimableSectionHolder.get();
  }

  if (options->memoryReclaimerFactory == nullptr ||
      options->memoryReclaimerFactory() == nullptr) {
    options->memoryReclaimerFactory = []() {
      return exec::MemoryReclaimer::create();
    };
  }

  if (options->serdeParameters.empty()) {
    options->serdeParameters = std::map<std::string, std::string>(
        insertTableHandle_->serdeParameters().begin(),
        insertTableHandle_->serdeParameters().end());
  }

  updateWriterOptionsFromHiveConfig(
      insertTableHandle_->tableStorageFormat(),
      hiveConfig_,
      connectorSessionProperties,
      options);

  const auto& sessionTimeZoneName = connectorQueryCtx_->sessionTimezone();
  if (!sessionTimeZoneName.empty()) {
    options->sessionTimezone = tz::locateZone(sessionTimeZoneName);
  }
  options->adjustTimestampToTimezone =
      connectorQueryCtx_->adjustTimestampToTimezone();

  // Prevents the memory allocation during the writer creation.
  WRITER_NON_RECLAIMABLE_SECTION_GUARD(writerInfo_.size() - 1);
  auto writer = writerFactory_->createWriter(
      dwio::common::FileSink::create(
          writePath,
          {
              .bufferWrite = false,
              .connectorProperties = hiveConfig_->config(),
              .fileCreateConfig = hiveConfig_->writeFileCreateConfig(),
              .pool = writerInfo_.back()->sinkPool.get(),
              .metricLogger = dwio::common::MetricsLog::voidLog(),
              .stats = ioStats_.back().get(),
          }),
      options);
  writer = maybeCreateBucketSortWriter(std::move(writer));
  writers_.emplace_back(std::move(writer));
  // Extends the buffer used for partition rows calculations.
  partitionSizes_.emplace_back(0);
  partitionRows_.emplace_back(nullptr);
  rawPartitionRows_.emplace_back(nullptr);

  writerIndexMap_.emplace(id, writers_.size() - 1);
  return writerIndexMap_[id];
}

std::unique_ptr<facebook::velox::dwio::common::Writer>
HiveDataSink::maybeCreateBucketSortWriter(
    std::unique_ptr<facebook::velox::dwio::common::Writer> writer) {
  if (!sortWrite()) {
    return writer;
  }
  auto* sortPool = writerInfo_.back()->sortPool.get();
  VELOX_CHECK_NOT_NULL(sortPool);
  auto sortBuffer = std::make_unique<exec::SortBuffer>(
      getNonPartitionTypes(dataChannels_, inputType_),
      sortColumnIndices_,
      sortCompareFlags_,
      sortPool,
      writerInfo_.back()->nonReclaimableSectionHolder.get(),
      connectorQueryCtx_->prefixSortConfig(),
      spillConfig_,
      writerInfo_.back()->spillStats.get());
  return std::make_unique<dwio::common::SortingWriter>(
      std::move(writer),
      std::move(sortBuffer),
      hiveConfig_->sortWriterMaxOutputRows(
          connectorQueryCtx_->sessionProperties()),
      hiveConfig_->sortWriterMaxOutputBytes(
          connectorQueryCtx_->sessionProperties()),
      sortWriterFinishTimeSliceLimitMs_);
}

HiveWriterId HiveDataSink::getWriterId(size_t row) const {
  std::optional<int32_t> partitionId;
  if (isPartitioned()) {
    VELOX_CHECK_LT(partitionIds_[row], std::numeric_limits<uint32_t>::max());
    partitionId = static_cast<uint32_t>(partitionIds_[row]);
  }

  std::optional<int32_t> bucketId;
  if (isBucketed()) {
    bucketId = bucketIds_[row];
  }
  return HiveWriterId{partitionId, bucketId};
}

void HiveDataSink::splitInputRowsAndEnsureWriters() {
  VELOX_CHECK(isPartitioned() || isBucketed());
  if (isBucketed() && isPartitioned()) {
    VELOX_CHECK_EQ(bucketIds_.size(), partitionIds_.size());
  }

  std::fill(partitionSizes_.begin(), partitionSizes_.end(), 0);

  const auto numRows =
      isPartitioned() ? partitionIds_.size() : bucketIds_.size();
  for (auto row = 0; row < numRows; ++row) {
    auto id = getWriterId(row);
    uint32_t index = ensureWriter(id);

    VELOX_DCHECK_LT(index, partitionSizes_.size());
    VELOX_DCHECK_EQ(partitionSizes_.size(), partitionRows_.size());
    VELOX_DCHECK_EQ(partitionRows_.size(), rawPartitionRows_.size());
    if (FOLLY_UNLIKELY(partitionRows_[index] == nullptr) ||
        (partitionRows_[index]->capacity() < numRows * sizeof(vector_size_t))) {
      partitionRows_[index] =
          allocateIndices(numRows, connectorQueryCtx_->memoryPool());
      rawPartitionRows_[index] =
          partitionRows_[index]->asMutable<vector_size_t>();
    }
    rawPartitionRows_[index][partitionSizes_[index]] = row;
    ++partitionSizes_[index];
  }

  for (uint32_t i = 0; i < partitionSizes_.size(); ++i) {
    if (partitionSizes_[i] != 0) {
      VELOX_CHECK_NOT_NULL(partitionRows_[i]);
      partitionRows_[i]->setSize(partitionSizes_[i] * sizeof(vector_size_t));
    }
  }
}

HiveWriterParameters HiveDataSink::getWriterParameters(
    const std::optional<std::string>& partition,
    std::optional<uint32_t> bucketId) const {
  auto [targetFileName, writeFileName] = getWriterFileNames(bucketId);

  return HiveWriterParameters{
      updateMode_,
      partition,
      targetFileName,
      makePartitionDirectory(
          insertTableHandle_->locationHandle()->targetPath(), partition),
      writeFileName,
      makePartitionDirectory(
          insertTableHandle_->locationHandle()->writePath(), partition)};
}

std::pair<std::string, std::string> HiveDataSink::getWriterFileNames(
    std::optional<uint32_t> bucketId) const {
  auto targetFileName = insertTableHandle_->locationHandle()->targetFileName();
  const bool generateFileName = targetFileName.empty();

  // Use custom file name generator if available and in compact mode
  if (isCompactMode() && customFileNameGenerator_ && generateFileName) {
    targetFileName = customFileNameGenerator_(writers_.size());
  } else if (bucketId.has_value()) {
    VELOX_CHECK(generateFileName);
    // TODO: add hive.file_renaming_enabled support.
    targetFileName = computeBucketedFileName(
        connectorQueryCtx_->queryId(), bucketId.value());
  } else if (generateFileName) {
    // targetFileName includes planNodeId and Uuid. As a result, different
    // table writers run by the same task driver or the same table writer
    // run in different task tries would have different targetFileNames.
    targetFileName = fmt::format(
        "{}_{}_{}_{}",
        connectorQueryCtx_->taskId(),
        connectorQueryCtx_->driverId(),
        connectorQueryCtx_->planNodeId(),
        makeUuid());
  }
  VELOX_CHECK(!targetFileName.empty());
  const std::string writeFileName = isCommitRequired()
      ? fmt::format(".tmp.velox.{}_{}", targetFileName, makeUuid())
      : targetFileName;
  if (generateFileName &&
      insertTableHandle_->tableStorageFormat() ==
          dwio::common::FileFormat::PARQUET) {
    return {
        fmt::format("{}{}", targetFileName, ".parquet"),
        fmt::format("{}{}", writeFileName, ".parquet")};
  }
  return {targetFileName, writeFileName};
}

HiveWriterParameters::UpdateMode HiveDataSink::getUpdateMode() const {
  if (insertTableHandle_->isExistingTable()) {
    if (insertTableHandle_->isPartitioned()) {
      const auto insertBehavior = hiveConfig_->insertExistingPartitionsBehavior(
          connectorQueryCtx_->sessionProperties());
      switch (insertBehavior) {
        case HiveConfig::InsertExistingPartitionsBehavior::kOverwrite:
          return HiveWriterParameters::UpdateMode::kOverwrite;
        case HiveConfig::InsertExistingPartitionsBehavior::kError:
          return HiveWriterParameters::UpdateMode::kNew;
        default:
          VELOX_UNSUPPORTED(
              "Unsupported insert existing partitions behavior: {}",
              HiveConfig::insertExistingPartitionsBehaviorString(
                  insertBehavior));
      }
    } else {
      if (insertTableHandle_->isBucketed()) {
        VELOX_USER_FAIL("Cannot insert into bucketed unpartitioned Hive table");
      }
      if (hiveConfig_->immutablePartitions()) {
        VELOX_USER_FAIL("Unpartitioned Hive tables are immutable.");
      }
      return HiveWriterParameters::UpdateMode::kAppend;
    }
  } else {
    return HiveWriterParameters::UpdateMode::kNew;
  }
}

bool HiveInsertTableHandle::isPartitioned() const {
  return std::any_of(
      inputColumns_.begin(), inputColumns_.end(), [](auto column) {
        return column->isPartitionKey();
      });
}

const HiveBucketProperty* HiveInsertTableHandle::bucketProperty() const {
  return bucketProperty_.get();
}

bool HiveInsertTableHandle::isBucketed() const {
  return bucketProperty() != nullptr;
}

bool HiveInsertTableHandle::isExistingTable() const {
  return locationHandle_->tableType() == LocationHandle::TableType::kExisting;
}

folly::dynamic HiveInsertTableHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "HiveInsertTableHandle";
  folly::dynamic arr = folly::dynamic::array;
  for (const auto& ic : inputColumns_) {
    arr.push_back(ic->serialize());
  }

  obj["inputColumns"] = arr;
  obj["locationHandle"] = locationHandle_->serialize();
  obj["tableStorageFormat"] = dwio::common::toString(tableStorageFormat_);

  if (bucketProperty_) {
    obj["bucketProperty"] = bucketProperty_->serialize();
  }

  if (compressionKind_.has_value()) {
    obj["compressionKind"] = common::compressionKindToString(*compressionKind_);
  }

  folly::dynamic params = folly::dynamic::object;
  for (const auto& [key, value] : serdeParameters_) {
    params[key] = value;
  }
  obj["serdeParameters"] = params;

  if (compactProperty_) {
    obj["compactProperty"] = compactProperty_->serialize();
  }

  return obj;
}

HiveInsertTableHandlePtr HiveInsertTableHandle::create(
    const folly::dynamic& obj) {
  auto inputColumns = ISerializable::deserialize<std::vector<HiveColumnHandle>>(
      obj["inputColumns"]);
  auto locationHandle =
      ISerializable::deserialize<LocationHandle>(obj["locationHandle"]);
  auto storageFormat =
      dwio::common::toFileFormat(obj["tableStorageFormat"].asString());

  std::optional<common::CompressionKind> compressionKind = std::nullopt;
  if (obj.count("compressionKind") > 0) {
    compressionKind =
        common::stringToCompressionKind(obj["compressionKind"].asString());
  }

  std::shared_ptr<const HiveBucketProperty> bucketProperty;
  if (obj.count("bucketProperty") > 0) {
    bucketProperty =
        ISerializable::deserialize<HiveBucketProperty>(obj["bucketProperty"]);
  }

  std::unordered_map<std::string, std::string> serdeParameters;
  for (const auto& pair : obj["serdeParameters"].items()) {
    serdeParameters.emplace(pair.first.asString(), pair.second.asString());
  }

  std::shared_ptr<const CompactProperty> compactProperty;
  if (obj.count("compactProperty") > 0) {
    compactProperty =
        ISerializable::deserialize<CompactProperty>(obj["compactProperty"]);
  }

  return std::make_shared<HiveInsertTableHandle>(
      inputColumns,
      locationHandle,
      storageFormat,
      bucketProperty,
      compressionKind,
      serdeParameters,
      nullptr, // writerOptions
      compactProperty);
}

void HiveInsertTableHandle::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("HiveInsertTableHandle", HiveInsertTableHandle::create);
}

std::string HiveInsertTableHandle::toString() const {
  std::ostringstream out;
  out << "HiveInsertTableHandle ["
      << dwio::common::toString(tableStorageFormat_);
  if (compressionKind_.has_value()) {
    out << " " << common::compressionKindToString(compressionKind_.value());
  } else {
    out << " none";
  }
  out << "], [inputColumns: [";
  for (const auto& i : inputColumns_) {
    out << " " << i->toString();
  }
  out << " ], locationHandle: " << locationHandle_->toString();
  if (bucketProperty_) {
    out << ", bucketProperty: " << bucketProperty_->toString();
  }

  if (serdeParameters_.size() > 0) {
    std::map<std::string, std::string> sortedSerdeParams(
        serdeParameters_.begin(), serdeParameters_.end());
    out << ", serdeParameters: ";
    for (const auto& [key, value] : sortedSerdeParams) {
      out << "[" << key << ", " << value << "] ";
    }
  }

  if (compactProperty_) {
    out << ", compactProperty: " << compactProperty_->toString();
  }

  out << "]";
  return out.str();
}

std::string LocationHandle::toString() const {
  return fmt::format(
      "LocationHandle [targetPath: {}, writePath: {}, tableType: {},",
      targetPath_,
      writePath_,
      tableTypeName(tableType_));
}

void LocationHandle::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("LocationHandle", LocationHandle::create);
}

folly::dynamic LocationHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "LocationHandle";
  obj["targetPath"] = targetPath_;
  obj["writePath"] = writePath_;
  obj["tableType"] = tableTypeName(tableType_);
  return obj;
}

LocationHandlePtr LocationHandle::create(const folly::dynamic& obj) {
  auto targetPath = obj["targetPath"].asString();
  auto writePath = obj["writePath"].asString();
  auto tableType = tableTypeFromName(obj["tableType"].asString());
  return std::make_shared<LocationHandle>(targetPath, writePath, tableType);
}

std::unique_ptr<memory::MemoryReclaimer> HiveDataSink::WriterReclaimer::create(
    HiveDataSink* dataSink,
    HiveWriterInfo* writerInfo,
    io::IoStatistics* ioStats) {
  return std::unique_ptr<memory::MemoryReclaimer>(
      new HiveDataSink::WriterReclaimer(dataSink, writerInfo, ioStats));
}

bool HiveDataSink::WriterReclaimer::reclaimableBytes(
    const memory::MemoryPool& pool,
    uint64_t& reclaimableBytes) const {
  VELOX_CHECK_EQ(pool.name(), writerInfo_->writerPool->name());
  reclaimableBytes = 0;
  if (!dataSink_->canReclaim()) {
    return false;
  }
  return exec::MemoryReclaimer::reclaimableBytes(pool, reclaimableBytes);
}

uint64_t HiveDataSink::WriterReclaimer::reclaim(
    memory::MemoryPool* pool,
    uint64_t targetBytes,
    uint64_t maxWaitMs,
    memory::MemoryReclaimer::Stats& stats) {
  VELOX_CHECK_EQ(pool->name(), writerInfo_->writerPool->name());
  if (!dataSink_->canReclaim()) {
    return 0;
  }

  if (*writerInfo_->nonReclaimableSectionHolder.get()) {
    RECORD_METRIC_VALUE(kMetricMemoryNonReclaimableCount);
    LOG(WARNING) << "Can't reclaim from hive writer pool " << pool->name()
                 << " which is under non-reclaimable section, "
                 << " reserved memory: "
                 << succinctBytes(pool->reservedBytes());
    ++stats.numNonReclaimableAttempts;
    return 0;
  }

  const uint64_t memoryUsageBeforeReclaim = pool->reservedBytes();
  const std::string memoryUsageTreeBeforeReclaim = pool->treeMemoryUsage();
  const auto writtenBytesBeforeReclaim = ioStats_->rawBytesWritten();
  const auto reclaimedBytes =
      exec::MemoryReclaimer::reclaim(pool, targetBytes, maxWaitMs, stats);
  const auto earlyFlushedRawBytes =
      ioStats_->rawBytesWritten() - writtenBytesBeforeReclaim;
  addThreadLocalRuntimeStat(
      kEarlyFlushedRawBytes,
      RuntimeCounter(earlyFlushedRawBytes, RuntimeCounter::Unit::kBytes));
  if (earlyFlushedRawBytes > 0) {
    RECORD_METRIC_VALUE(
        kMetricFileWriterEarlyFlushedRawBytes, earlyFlushedRawBytes);
  }
  const uint64_t memoryUsageAfterReclaim = pool->reservedBytes();
  if (memoryUsageAfterReclaim > memoryUsageBeforeReclaim) {
    VELOX_FAIL(
        "Unexpected memory growth after memory reclaim from {}, the memory usage before reclaim: {}, after reclaim: {}\nThe memory tree usage before reclaim:\n{}\nThe memory tree usage after reclaim:\n{}",
        pool->name(),
        succinctBytes(memoryUsageBeforeReclaim),
        succinctBytes(memoryUsageAfterReclaim),
        memoryUsageTreeBeforeReclaim,
        pool->treeMemoryUsage());
  }
  return reclaimedBytes;
}

folly::dynamic CompactProperty::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "CompactProperty";
  obj["enabled"] = enabled_;
  obj["fileSizeThreshold"] = static_cast<int64_t>(fileSizeThreshold_);
  obj["sortKeyColumns"] = ISerializable::serialize(sortKeyColumns_);
  obj["timeColumn"] = timeColumn_;
  obj["hasCustomFileNameGenerator"] = hasCustomFileNameGenerator();
  return obj;
}

std::shared_ptr<CompactProperty> CompactProperty::deserialize(
    const folly::dynamic& obj,
    void* context) {
  const bool enabled = obj["enabled"].asBool();
  const uint64_t fileSizeThreshold =
      static_cast<uint64_t>(obj["fileSizeThreshold"].asInt());
  const auto sortKeyColumns =
      ISerializable::deserialize<std::vector<std::string>>(
          obj["sortKeyColumns"]);
  const std::string timeColumn = obj["timeColumn"].asString();

  // Note: Custom file name generators cannot be deserialized
  // They must be set at runtime through the appropriate constructor
  return std::make_shared<CompactProperty>(
      enabled, fileSizeThreshold, sortKeyColumns, timeColumn);
}

std::string CompactProperty::toString() const {
  std::stringstream out;
  out << "CompactProperty[enabled=" << (enabled_ ? "true" : "false");
  out << ", fileSizeThreshold=" << fileSizeThreshold_;
  out << ", sortKeyColumns=[";
  for (size_t i = 0; i < sortKeyColumns_.size(); ++i) {
    if (i > 0)
      out << ", ";
    out << sortKeyColumns_[i];
  }
  out << "], timeColumn=" << timeColumn_ << "]";
  return out.str();
}

void CompactProperty::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("CompactProperty", CompactProperty::deserialize);
}

bool HiveDataSink::isCompactMode() const {
  return insertTableHandle_->compactProperty() != nullptr &&
      insertTableHandle_->compactProperty()->enabled();
}

bool HiveDataSink::shouldSwitchFile() const {
  if (!isCompactMode() || writers_.empty()) {
    return false;
  }

  const auto& currentStats = ioStats_[currentWriterIndex_];

  return currentStats->rawBytesWritten() >=
      insertTableHandle_->compactProperty()->fileSizeThreshold();
}

void HiveDataSink::switchToNewFile() {
  if (!isCompactMode()) {
    return;
  }

  // Finish current writer if it exists and update final file size
  if (!writers_.empty() && currentWriterIndex_ < compactFileStats_.size()) {
    WRITER_NON_RECLAIMABLE_SECTION_GUARD(currentWriterIndex_);
    writers_[currentWriterIndex_]->finish();

    // Update final file size
    if (currentWriterIndex_ < ioStats_.size()) {
      compactFileStats_[currentWriterIndex_].fileSize =
          ioStats_[currentWriterIndex_]->rawBytesWritten();
    }
  }

  // Create new writer with unique ID
  // Use the current number of writers as a unique partition ID to avoid
  // conflicts
  uint32_t uniquePartitionId = static_cast<uint32_t>(writers_.size());
  HiveWriterId uniqueWriterId{uniquePartitionId};
  currentWriterIndex_ = appendWriter(uniqueWriterId);

  // Initialize file stats for the new file
  compactFileStats_.emplace_back();
}

void HiveDataSink::updateFileStats(
    size_t writerIndex,
    const RowVectorPtr& input) {
  if (!isCompactMode() || writerIndex >= compactFileStats_.size()) {
    return;
  }

  auto& stats = compactFileStats_[writerIndex];
  const size_t previousRows = stats.numRows;
  stats.numRows += input->size();

  // Update sort key column statistics (simplified for sorted data)
  collectSortKeyColumnStats(input, stats);

  // Update time column statistics (requires full comparison)
  collectTimeColumnStats(input, compactTimeColumnIndex_, stats);
}

void HiveDataSink::collectSortKeyColumnStats(
    const RowVectorPtr& input,
    CompactFileStats& stats) {
  if (compactSortKeyColumnIndices_.empty() || !input || input->size() == 0) {
    return;
  }

  const auto& sortKeyColumns =
      insertTableHandle_->compactProperty()->sortKeyColumns();

  // Ensure the vectors are large enough
  if (stats.sortKeyMinValues.size() < sortKeyColumns.size()) {
    stats.sortKeyMinValues.resize(sortKeyColumns.size());
    stats.sortKeyMaxValues.resize(sortKeyColumns.size());
  }

  // Set minimum values from the first row (only if not already initialized)
  if (!stats.sortKeyInitialized) {
    for (size_t i = 0; i < compactSortKeyColumnIndices_.size(); ++i) {
      const auto columnIndex = compactSortKeyColumnIndices_[i];
      auto column = input->childAt(columnIndex);
      if (!column || column->size() == 0) {
        continue;
      }

      // Use the first row, regardless of null values
      if (!column->isNullAt(0)) {
        auto varbinaryColumn = column->as<SimpleVector<StringView>>();
        VELOX_CHECK_NOT_NULL(
            varbinaryColumn, "Sort key column should be VARBINARY type");

        stats.sortKeyMinValues[i] = varbinaryColumn->valueAt(0).str();
      }
    }
    stats.sortKeyInitialized = true;
  }

  // Always update maximum values from the last row
  vector_size_t lastRow = input->size() - 1;
  for (size_t i = 0; i < compactSortKeyColumnIndices_.size(); ++i) {
    const auto columnIndex = compactSortKeyColumnIndices_[i];
    if (columnIndex >= input->childrenSize()) {
      continue;
    }

    auto column = input->childAt(columnIndex);
    if (!column || column->size() == 0) {
      continue;
    }

    // Use the last row, regardless of null values
    if (!column->isNullAt(lastRow)) {
      auto varbinaryColumn = column->as<SimpleVector<StringView>>();
      VELOX_CHECK_NOT_NULL(
          varbinaryColumn, "Sort key column should be VARBINARY type");

      stats.sortKeyMaxValues[i] = varbinaryColumn->valueAt(lastRow).str();
    }
  }
}

void HiveDataSink::collectTimeColumnStats(
    const RowVectorPtr& input,
    column_index_t columnIndex,
    CompactFileStats& stats) {
  // Check if time column is configured and valid
  if (columnIndex == static_cast<column_index_t>(-1) ||
      columnIndex >= input->childrenSize()) {
    return;
  }

  auto column = input->childAt(columnIndex);
  if (!column || column->size() == 0) {
    return;
  }

  auto timeColumn = column->as<SimpleVector<int64_t>>();
  VELOX_CHECK_NOT_NULL(timeColumn, "Time column should be int64_t type");

  for (vector_size_t i = 0; i < column->size(); ++i) {
    if (column->isNullAt(i)) {
      continue;
    }

    int64_t value = timeColumn->valueAt(i);

    if (value < stats.timeColumnMinValue) {
      stats.timeColumnMinValue = value;
    }
    if (value > stats.timeColumnMaxValue) {
      stats.timeColumnMaxValue = value;
    }
  }
}
} // namespace facebook::velox::connector::hive
