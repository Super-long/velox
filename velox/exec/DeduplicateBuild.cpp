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

#include "velox/exec/DeduplicateBuild.h"
#include "velox/exec/OperatorUtils.h"

#include <algorithm>
#include <iostream>
#include <limits>
#include <numeric>

namespace facebook::velox::exec {

RowVectorPtr DeduplicatePartition::extractAllRows(
    memory::MemoryPool* pool) const {
  if (rows_.empty()) {
    return nullptr;
  }

  auto result = BaseVector::create<RowVector>(outputType_, rows_.size(), pool);

  for (column_index_t i = 0; i < outputType_->size(); ++i) {
    data_->extractColumn(rows_.data(), rows_.size(), i, result->childAt(i));
  }

  return result;
}

vector_size_t DeduplicatePartition::extractOneRowTo(
    RowVectorPtr& targetResult,
    vector_size_t targetStartRow) const {
  if (rows_.empty()) {
    return 0;
  }

  // Copy data directly to target vector without creating intermediate vectors
  for (column_index_t i = 0; i < outputType_->size(); ++i) {
    data_->extractColumn(
        rows_.data(), 1, i, targetStartRow, targetResult->childAt(i));
  }

  return 1;
}

vector_size_t DeduplicatePartition::performFieldDeduplicationDirect(
    RowVectorPtr& targetResult,
    vector_size_t targetStartRow,
    column_index_t sortingColumnIndex) const {
  if (rows_.empty()) {
    return 0;
  }

  // For multiple rows, find the best row for each column based on sorting order
  // First sort row indices by the sorting column
  std::vector<int32_t> sortedIndices(rows_.size());
  std::iota(sortedIndices.begin(), sortedIndices.end(), 0);

  std::sort(
      sortedIndices.begin(), sortedIndices.end(), [&](int32_t a, int32_t b) {
        // Compare rows[a] vs rows[b] on sortingColumnIndex in descending order
        // (latest first)
        CompareFlags flags{
            false,
            true,
            false}; // nullsFirst=false, ascending=true, equalsOnly=false
        auto result =
            data_->compare(rows_[a], rows_[b], sortingColumnIndex, flags);
        return result > 0; // Descending order
      });

  // For each column, find the first non-null value from sorted rows
  for (column_index_t col = 0; col < outputType_->size(); ++col) {
    bool found = false;
    for (int32_t idx : sortedIndices) {
      if (!data_->isNullAt(rows_[idx], data_->columnAt(col))) {
        // Found first non-null value, extract it
        data_->extractColumn(
            &rows_[idx], 1, col, targetStartRow, targetResult->childAt(col));
        found = true;
        break;
      }
    }
    // If all values are null, set target to null
    if (!found) {
      targetResult->childAt(col)->setNull(targetStartRow, true);
    }
  }

  return 1;
}

vector_size_t DeduplicatePartition::performRowDeduplicationDirect(
    RowVectorPtr& targetResult,
    vector_size_t targetStartRow,
    column_index_t sortingColumnIndex) const {
  if (rows_.empty()) {
    return 0;
  }

  // Find the row with the maximum value in sortingColumnIndex
  int32_t bestRowIndex = 0;

  for (int32_t i = 1; i < static_cast<int32_t>(rows_.size()); ++i) {
    // Handle nulls: non-null values are always preferred over null values
    bool currentIsNull =
        data_->isNullAt(rows_[i], data_->columnAt(sortingColumnIndex));
    bool bestIsNull = data_->isNullAt(
        rows_[bestRowIndex], data_->columnAt(sortingColumnIndex));

    if (bestIsNull && !currentIsNull) {
      // Current value is non-null, best is null -> current wins
      bestRowIndex = i;
    } else if (!bestIsNull && !currentIsNull) {
      // Both are non-null, compare values
      CompareFlags flags{
          false,
          true,
          false}; // nullsFirst=false, ascending=true, equalsOnly=false
      auto comparison = data_->compare(
          rows_[i], rows_[bestRowIndex], sortingColumnIndex, flags);
      if (comparison > 0) { // current > best
        bestRowIndex = i;
      }
    }
    // If current is null and best is non-null, best remains
    // If both are null, keep the first one (best remains)
  }

  // Extract the best row to target
  for (column_index_t col = 0; col < outputType_->size(); ++col) {
    data_->extractColumn(
        &rows_[bestRowIndex],
        1,
        col,
        targetStartRow,
        targetResult->childAt(col));
  }

  return 1;
}

std::vector<TypePtr> deduplicateSlice(
    const std::vector<TypePtr>& types,
    int32_t start,
    int32_t end) {
  std::vector<TypePtr> result;
  result.reserve(end - start);
  for (auto i = start; i < end; ++i) {
    result.push_back(types[i]);
  }
  return result;
}

DeduplicateBuild::DeduplicateBuild(
    const std::shared_ptr<const core::DeduplicateNode>& deduplicateNode,
    memory::MemoryPool* pool)
    : deduplicateNode_(deduplicateNode),
      keyChannels_(
          toChannels(deduplicateNode_->outputType(), deduplicateNode_->keys())),
      pool_(pool) {
  // Setup partition key info for comparison
  partitionKeyInfo_.reserve(keyChannels_.size());
  auto sortOrder = deduplicateNode_->sortingOrders();
  for (int i = 0; i < keyChannels_.size(); i++) {
    partitionKeyInfo_.emplace_back(keyChannels_[i], sortOrder[i]);
  }

  data_ = std::make_unique<RowContainer>(
      deduplicateSlice(
          deduplicateNode_->outputType()->children(), 0, keyChannels_.size()),
      deduplicateSlice(
          deduplicateNode_->outputType()->children(),
          keyChannels_.size(),
          deduplicateNode_->outputType()->size()),
      pool_);

  // Setup decoded vectors for input processing
  decodedInputVectors_.resize(deduplicateNode_->outputType()->size());

  // Initialize partition boundaries with first element as 0
  // This establishes clear partition semantics: partition count =
  // partitionStartRows_.size() - 1
  partitionStartRows_.push_back(0);
}

void DeduplicateBuild::addInput(RowVectorPtr input) {
  // Decode input vectors for efficient processing
  for (auto i = 0; i < input->childrenSize(); ++i) {
    decodedInputVectors_[i].decode(*input->childAt(i));
  }

  for (auto row = 0; row < input->size(); ++row) {
    char* newRow = data_->newRow();

    // Store the row data
    for (auto col = 0; col < input->childrenSize(); ++col) {
      data_->store(decodedInputVectors_[col], row, newRow, col);
    }

    // Check if this starts a new partition
    if (previousRow_ != nullptr && comparePartitionKeys(previousRow_, newRow)) {
      buildNextPartition();
    }

    inputRows_.push_back(newRow);
    previousRow_ = newRow;
  }
}

void DeduplicateBuild::noMoreInput() {
  if (noMoreInput_) {
    return; // Already processed, avoid duplicate calls
  }

  noMoreInput_ = true;

  // Build the final partition from remaining input rows
  if (!inputRows_.empty()) {
    buildNextPartition();
  }
}

bool DeduplicateBuild::hasNextPartition() const {
  // Clear semantics: total complete partitions = partitionStartRows_.size() - 1
  // Available partition index range: [0, partitionStartRows_.size() - 2]
  const auto totalCompletePartitions = partitionStartRows_.size() - 1;
  const auto nextPartitionIndex = currentPartition_ + 1;

  return nextPartitionIndex < totalCompletePartitions;
}

std::unique_ptr<DeduplicatePartition> DeduplicateBuild::nextPartition() {
  VELOX_CHECK(hasNextPartition(), "No more partitions available");

  ++currentPartition_;

  // Clear index semantics: partition i range is [partitionStartRows_[i],
  // partitionStartRows_[i+1])
  const auto partitionSize = partitionStartRows_[currentPartition_ + 1] -
      partitionStartRows_[currentPartition_];

  const auto partition = folly::Range(
      sortedRows_.data() + partitionStartRows_[currentPartition_],
      partitionSize);

  return std::make_unique<DeduplicatePartition>(
      data_.get(), partition, keyChannels_, deduplicateNode_->outputType());
}

bool DeduplicateBuild::needsInput() const {
  return !noMoreInput_ && !hasNextPartition();
}

bool DeduplicateBuild::comparePartitionKeys(const char* lhs, const char* rhs)
    const {
  for (const auto& keyInfo : partitionKeyInfo_) {
    CompareFlags flags{
        .nullsFirst = keyInfo.second.isNullsFirst(),
        .ascending = keyInfo.second.isAscending(),
        .equalsOnly = false};
    if (auto result = data_->compare(lhs, rhs, keyInfo.first, flags)) {
      return true; // Keys are different, this is a new partition
    }
  }
  return false; // All keys are equal, same partition
}

void DeduplicateBuild::buildNextPartition() {
  // Directly add the end position of the new partition
  // Add input rows to sorted rows
  sortedRows_.insert(sortedRows_.end(), inputRows_.begin(), inputRows_.end());
  inputRows_.clear();
  const auto oldSize = partitionStartRows_.size();
  partitionStartRows_.push_back(sortedRows_.size());
  const auto newSize = partitionStartRows_.size();
}

vector_size_t DeduplicateBuild::batchCopyConsecutiveSingleRows(
    RowVectorPtr& targetResult,
    vector_size_t targetStartRow,
    vector_size_t numPartitions) {
  // Collect row pointers for the specified number of single-row partitions
  // Trust that numPartitions has been validated by
  // countConsecutiveSingleRowPartitions
  std::vector<char*> rowPtrs;
  rowPtrs.reserve(numPartitions);

  for (vector_size_t i = 0; i < numPartitions; ++i) {
    const auto nextPartitionIndex = currentPartition_ + 1 + i;
    // Trust the caller - these should all be single-row partitions
    char* rowPtr = sortedRows_[partitionStartRows_[nextPartitionIndex]];
    rowPtrs.push_back(rowPtr);
  }

  // Batch copy all collected rows using extractColumn
  for (column_index_t col = 0; col < deduplicateNode_->outputType()->size();
       ++col) {
    data_->extractColumn(
        rowPtrs.data(),
        numPartitions,
        col,
        targetStartRow,
        targetResult->childAt(col));
  }

  return numPartitions;
}

vector_size_t DeduplicateBuild::advanceConsecutiveSingleRowPartitions(
    vector_size_t numPartitions) {
  // Trust that numPartitions has been validated - just advance the metadata
  for (vector_size_t i = 0; i < numPartitions; ++i) {
    // Advance partition metadata (similar to nextPartition but simplified)
    ++currentPartition_;
  }

  return numPartitions;
}

vector_size_t DeduplicateBuild::countConsecutiveSingleRowPartitions(
    vector_size_t maxPartitions) const {
  vector_size_t count = 0;
  int32_t checkPartition = currentPartition_ + 1;
  const auto totalCompletePartitions = partitionStartRows_.size() - 1;

  while (checkPartition < totalCompletePartitions && count < maxPartitions) {
    const auto partitionSize = partitionStartRows_[checkPartition + 1] -
        partitionStartRows_[checkPartition];

    if (partitionSize != 1) {
      // Found a partition with multiple rows, stop counting
      break;
    }

    ++count;
    ++checkPartition;
  }

  return count;
}

bool DeduplicateBuild::cleanupProcessedPartitions() {
  if (currentPartition_ < 0) {
    return false; // No processed partitions to cleanup
  }

  // Partitions to cleanup: [0, currentPartition_], total of currentPartition_+1
  // partitions
  const auto numPartitionsToCleanup = currentPartition_ + 1;
  const auto numRowsToErase = partitionStartRows_[numPartitionsToCleanup];

  if (numRowsToErase > 0) {
    // Erase the processed partition data to save memory
    data_->eraseRows(folly::Range<char**>(sortedRows_.data(), numRowsToErase));
    sortedRows_.erase(
        sortedRows_.begin(), sortedRows_.begin() + numRowsToErase);
    sortedRows_.shrink_to_fit();

    // Adjust partition start indices for remaining partitions
    for (int i = numPartitionsToCleanup; i < partitionStartRows_.size(); ++i) {
      partitionStartRows_[i] -= numRowsToErase;
    }

    // Remove processed partition start indices (preserve partitionStartRows_[0]
    // = 0)
    partitionStartRows_.erase(
        partitionStartRows_.begin() + 1, // Preserve the first 0
        partitionStartRows_.begin() + numPartitionsToCleanup + 1);
    partitionStartRows_.shrink_to_fit();

    // Reset current partition index to indicate cleanup happened
    currentPartition_ = -1;

    return true; // Cleanup was performed
  }

  return false; // No cleanup needed
}

} // namespace facebook::velox::exec
