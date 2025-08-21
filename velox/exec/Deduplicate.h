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

#include "velox/exec/DeduplicateBuild.h"
#include "velox/exec/Operator.h"

#include <iostream>

namespace facebook::velox::exec {

/// Deduplicate operator for time series data supporting both field-level and
/// row-level deduplication. Uses DeduplicateBuild for efficient partition
/// management similar to Window operator architecture.
class Deduplicate : public Operator {
 public:
  Deduplicate(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::DeduplicateNode>& deduplicateNode);

  void initialize() override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !noMoreInput_ && deduplicateBuild_->needsInput();
  }

  void noMoreInput() override;

  BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return noMoreInput_ && numProcessedRows_ >= numRows_ &&
           !currentPartition_ && !deduplicateBuild_->hasNextPartition();
  }

  bool canSpill() const override {
    return false;
  }

 private:
  /// Reset current partition and fetch next partition from build
  void resetPartition();

  /// Performs field-level deduplication on partition data
  /// @param partitionData The partition data to deduplicate
  /// @param sortingColumnIndex The column index to sort by (typically
  /// timestamp)
  RowVectorPtr performFieldDeduplication(
      RowVectorPtr partitionData,
      column_index_t sortingColumnIndex) const;

  /// Performs row-level deduplication on partition data
  /// @param partitionData The partition data to deduplicate
  /// @param sortingColumnIndex The column index to sort by (typically
  /// timestamp)
  RowVectorPtr performRowDeduplication(
      RowVectorPtr partitionData,
      column_index_t sortingColumnIndex) const;

  /// Process multiple partitions to fill output batch up to numRowsPerOutput_
  /// Returns number of rows actually processed
  vector_size_t processPartitionsForOutput(
      vector_size_t maxOutputRows,
      RowVectorPtr& result);

  /// Perform in-place deduplication directly on target buffer to avoid
  /// intermediate vectors
  /// @param partitionData The partition to deduplicate
  /// @param result The target output buffer
  /// @param targetStartRow Starting row in target buffer
  /// @param maxRows Maximum rows to process
  /// @return Number of deduplicated rows written to target
  vector_size_t deduplicatePartitionInPlace(
      std::unique_ptr<DeduplicatePartition>& partition,
      RowVectorPtr& result,
      vector_size_t targetStartRow,
      vector_size_t maxRows) const;

  /// Helper struct to reference rows for sorting
  struct RowRef {
    RowVectorPtr batch;
    vector_size_t rowIndex;
  };

  /// Configuration
  const std::shared_ptr<const core::DeduplicateNode> deduplicateNode_;
  const std::vector<column_index_t> keyChannels_;
  const core::DeduplicateNode::Mode mode_;
  column_index_t sortingColumnIndex_;

  /// Build for managing partitioned data
  std::unique_ptr<DeduplicateBuild> deduplicateBuild_;

  /// Current partition being processed
  std::unique_ptr<DeduplicatePartition> currentPartition_;

  /// Batch output configuration
  vector_size_t numRowsPerOutput_{1024};
  vector_size_t numRows_{0};
  vector_size_t numProcessedRows_{0};
  vector_size_t numCleanProcessedRows_{0};


  /// Statistics
  vector_size_t numPartitions_{0};
  vector_size_t totalInputRows_{0};
  vector_size_t totalOutputRows_{0};
};

} // namespace facebook::velox::exec
