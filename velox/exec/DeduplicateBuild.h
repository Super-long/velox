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

#include "velox/exec/Operator.h"
#include "velox/exec/RowContainer.h"

namespace facebook::velox::exec {

class DeduplicatePartition {
 public:
  DeduplicatePartition(
      RowContainer* data,
      folly::Range<char**> rows,
      const std::vector<column_index_t>& keyChannels,
      const RowTypePtr& outputType)
      : data_(data),
        rows_(rows),
        keyChannels_(keyChannels),
        outputType_(outputType) {}

  RowVectorPtr extractAllRows(memory::MemoryPool* pool) const;

  /// More efficient extraction that copies rows directly to target buffer
  /// @param targetResult The target RowVector to copy data into
  /// @param targetStartRow The starting row index in target vector
  /// @return Number of rows actually copied
  vector_size_t extractOneRowTo(
      RowVectorPtr& targetResult,
      vector_size_t targetStartRow) const;

  /// Perform field deduplication directly on raw row data without intermediate
  /// vectors
  /// @param targetResult Target vector to write the deduplicated result
  /// @param targetStartRow Starting row in target vector
  /// @param sortingColumnIndex Column to sort by for deduplication
  /// @return 1 if successful, 0 if no data
  vector_size_t performFieldDeduplicationDirect(
      RowVectorPtr& targetResult,
      vector_size_t targetStartRow,
      column_index_t sortingColumnIndex) const;

  /// Perform row deduplication directly on raw row data without intermediate
  /// vectors
  /// @param targetResult Target vector to write the deduplicated result
  /// @param targetStartRow Starting row in target vector
  /// @param sortingColumnIndex Column to sort by for deduplication
  /// @return 1 if successful, 0 if no data
  vector_size_t performRowDeduplicationDirect(
      RowVectorPtr& targetResult,
      vector_size_t targetStartRow,
      column_index_t sortingColumnIndex) const;

  vector_size_t size() const {
    return rows_.size();
  }
  bool empty() const {
    return rows_.empty();
  }

 private:
  RowContainer* data_;
  folly::Range<char**> rows_;
  const std::vector<column_index_t>& keyChannels_;
  const RowTypePtr outputType_;
};

class DeduplicateBuild {
 public:
  DeduplicateBuild(
      const std::shared_ptr<const core::DeduplicateNode>& deduplicateNode,
      memory::MemoryPool* pool);

  void addInput(RowVectorPtr input);
  void noMoreInput();
  bool hasNextPartition() const;
  std::unique_ptr<DeduplicatePartition> nextPartition();
  bool needsInput() const;

  /// Batch copy N consecutive single-row partitions to target (data copy only)
  /// @param targetResult The target RowVector to copy data into
  /// @param targetStartRow The starting row index in target vector
  /// @param numPartitions Number of single-row partitions to copy
  /// @return Number of rows actually copied
  vector_size_t batchCopyConsecutiveSingleRows(
      RowVectorPtr& targetResult,
      vector_size_t targetStartRow,
      vector_size_t numPartitions);

  /// Advance partition metadata for N consecutive single-row partitions
  /// @param numPartitions Number of single-row partitions to advance
  /// @return Number of partitions actually advanced
  vector_size_t advanceConsecutiveSingleRowPartitions(
      vector_size_t numPartitions);

  /// Count consecutive single-row partitions starting from current position
  /// @param maxPartitions Maximum number of partitions to check
  /// @return Number of consecutive single-row partitions available (up to
  /// maxPartitions)
  vector_size_t countConsecutiveSingleRowPartitions(
      vector_size_t maxPartitions) const;

  /// Manually trigger cleanup of processed partitions
  /// @return true if cleanup was performed, false if no cleanup needed
  bool cleanupProcessedPartitions();

  int32_t currentPartition() const {
    return currentPartition_;
  }

 private:
  bool comparePartitionKeys(const char* lhs, const char* rhs) const;
  void buildNextPartition();

  /// Perform batch cleanup of processed partitions to improve performance
  void batchCleanupPartitions();

  const std::shared_ptr<const core::DeduplicateNode> deduplicateNode_;
  const std::vector<column_index_t> keyChannels_;
  memory::MemoryPool* pool_;
  std::unique_ptr<RowContainer> data_;
  std::vector<DecodedVector> decodedInputVectors_;
  std::vector<char*> inputRows_;
  std::vector<char*> sortedRows_;
  std::vector<vector_size_t> partitionStartRows_;
  char* previousRow_{nullptr};
  int32_t currentPartition_{-1};
  bool noMoreInput_{false};
  std::vector<std::pair<column_index_t, core::SortOrder>> partitionKeyInfo_;
};

} // namespace facebook::velox::exec
