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
#include "velox/exec/Deduplicate.h"
#include "velox/exec/DeduplicateBuild.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"

#include <glog/logging.h>
#include <algorithm>
#include <iostream>

namespace facebook::velox::exec {

Deduplicate::Deduplicate(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::DeduplicateNode>& deduplicateNode)
    : Operator(
          driverCtx,
          deduplicateNode->outputType(),
          operatorId,
          deduplicateNode->id(),
          "Deduplicate"),
      deduplicateNode_(deduplicateNode),
      keyChannels_(
          toChannels(deduplicateNode_->outputType(), deduplicateNode_->keys())),
      mode_(deduplicateNode_->mode()) {
  VELOX_CHECK(
      deduplicateNode_->sortingKey() != nullptr,
      "sortingKey is nullptr not supported in Deduplicate operator");
  auto sortingKeys =
      std::vector<core::TypedExprPtr>{deduplicateNode_->sortingKey()};
  auto sortingChannels =
      toChannels(deduplicateNode_->outputType(), sortingKeys);
  if (!sortingChannels.empty()) {
    sortingColumnIndex_ = sortingChannels[0];
  } else {
    sortingColumnIndex_ = 0; // fallback to first column
  }

  // Create the build for managing partitioned data
  deduplicateBuild_ = std::make_unique<DeduplicateBuild>(
      deduplicateNode_, operatorCtx_->pool());
}

void Deduplicate::initialize() {
  Operator::initialize();
  VELOX_CHECK_NOT_NULL(deduplicateNode_);

  // TODO(kavinli): get the average row size from the deduplicate build
  numRowsPerOutput_ = outputBatchRows(std::nullopt);
}

void Deduplicate::addInput(RowVectorPtr input) {
  totalInputRows_ += input->size();
  numRows_ += input->size(); // Track total rows for batch output
  deduplicateBuild_->addInput(input);
}

RowVectorPtr Deduplicate::getOutput() {
  if (numRows_ == 0) {
    return nullptr;
  }

  const auto numRowsLeft = numRows_ - numProcessedRows_;
  if (numRowsLeft == 0) {
    return nullptr;
  }

  if (currentPartition_ == nullptr) {
    resetPartition();
    if (currentPartition_ == nullptr) {
      // DeduplicateBuild doesn't have a partition to output.
      return nullptr;
    }
  }

  const auto numOutputRows = std::min(numRowsPerOutput_, numRowsLeft);
  auto result = BaseVector::create<RowVector>(
      outputType_, numOutputRows, operatorCtx_->pool());

  // Process partitions to fill output batch
  auto numResultRows = processPartitionsForOutput(numOutputRows, result);

  if (numResultRows == 0) {
    return nullptr;
  }

  totalOutputRows_ += numResultRows;

  return numResultRows < numOutputRows
      ? std::dynamic_pointer_cast<RowVector>(result->slice(0, numResultRows))
      : result;
}

vector_size_t Deduplicate::processPartitionsForOutput(
    vector_size_t maxOutputRows,
    RowVectorPtr& result) {
  vector_size_t outputRowIndex = 0;
  const vector_size_t kCleanupThreshold = 1000; 
  while (outputRowIndex < maxOutputRows) {
    // Get current partition if we don't have one
    if (!currentPartition_) {
      resetPartition();
      if (!currentPartition_) {
        // No more partitions available
        break;
      }
    }

    if (currentPartition_->size() == 1) {
      currentPartition_->extractOneRowTo(result, outputRowIndex);
      outputRowIndex++;
      numProcessedRows_++;
      numCleanProcessedRows_++;

      // Reset current partition since consecutive partitions are processed
      currentPartition_.reset();

      // Check for consecutive single-row partitions and batch process them
      const auto remainingSpace = maxOutputRows - outputRowIndex;
      const auto consecutiveSingleRowPartitions =
          deduplicateBuild_->countConsecutiveSingleRowPartitions(remainingSpace);

      if (consecutiveSingleRowPartitions > 0) {
        // Add 1 for the current partition and the consecutive single-row
        // partitions
        numPartitions_ += consecutiveSingleRowPartitions;

        // First copy the data
        const auto batchedRows =
            deduplicateBuild_->batchCopyConsecutiveSingleRows(
                result, outputRowIndex, consecutiveSingleRowPartitions);

        // Then advance the metadata
        const auto advancedPartitions =
            deduplicateBuild_->advanceConsecutiveSingleRowPartitions(batchedRows);

        outputRowIndex += batchedRows;
        numProcessedRows_ += batchedRows;
        numCleanProcessedRows_ += batchedRows;

        // Check if we should trigger cleanup
        if (numCleanProcessedRows_ >= kCleanupThreshold) {
          if (deduplicateBuild_->cleanupProcessedPartitions()) {
            numCleanProcessedRows_ = 0; // Reset counter after cleanup
            currentPartition_.reset(); // Ensure current partition is reset
          }
        }

        // Continue to next iteration - either we're done or there are more partitions
        continue;
      }
    } else {
      const auto remainingSpace = maxOutputRows - outputRowIndex;
      // Perform in-place deduplication for multi-row partitions
      const auto deduplicatedRows = deduplicatePartitionInPlace(
          currentPartition_, result, outputRowIndex, remainingSpace);

      if (deduplicatedRows > 0) {
        outputRowIndex += deduplicatedRows;
        numProcessedRows_ += currentPartition_->size();
        numCleanProcessedRows_ += currentPartition_->size();
      }

      currentPartition_.reset();
      // Check if we should trigger cleanup for multi-row partitions too
      if (numCleanProcessedRows_ >= kCleanupThreshold) {
        if (deduplicateBuild_->cleanupProcessedPartitions()) {
          numCleanProcessedRows_ = 0; // Reset counter after cleanup
          currentPartition_.reset(); // Ensure current partition is reset
        }
      }
    }
  }

  return outputRowIndex;
}

vector_size_t Deduplicate::deduplicatePartitionInPlace(
    std::unique_ptr<DeduplicatePartition>& partition,
    RowVectorPtr& result,
    vector_size_t targetStartRow,
    vector_size_t maxRows) const {
  if (!partition || partition->empty() || maxRows == 0) {
    return 0;
  }

  // For deduplication, we always output exactly 1 row per partition
  if (maxRows < 1) {
    return 0;
  }

  if (mode_ == core::DeduplicateNode::Mode::kField) {
    return partition->performFieldDeduplicationDirect(
        result, targetStartRow, sortingColumnIndex_);
  } else {
    return partition->performRowDeduplicationDirect(
        result, targetStartRow, sortingColumnIndex_);
  }
}

void Deduplicate::resetPartition() {
  currentPartition_ = nullptr;
  if (deduplicateBuild_->hasNextPartition()) {
    currentPartition_ = deduplicateBuild_->nextPartition();
    numPartitions_++;
  }
}

void Deduplicate::noMoreInput() {
  Operator::noMoreInput();
  deduplicateBuild_->noMoreInput();
  // Record runtime metrics when processing is complete
  auto lockedStats = stats_.wlock();

  // Add runtime metrics for deduplicate statistics
  lockedStats->runtimeStats["numPartitions"] = RuntimeMetric(numPartitions_);
  lockedStats->runtimeStats["totalInputRows"] = RuntimeMetric(totalInputRows_);
  lockedStats->runtimeStats["totalOutputRows"] =
      RuntimeMetric(totalOutputRows_);
  lockedStats->runtimeStats["numRowsPerOutput"] =
      RuntimeMetric(numRowsPerOutput_);

  // Calculate and add derived metrics (multiply by 100 to convert to integers)
  if (totalInputRows_ > 0) {
    double deduplicationRatio =
        1.0 - (static_cast<double>(totalOutputRows_) / totalInputRows_);
    // Convert to integer by multiplying by 100 (represents percentage * 100)
    lockedStats->runtimeStats["deduplicationRatio*100"] =
        RuntimeMetric(static_cast<int64_t>(deduplicationRatio * 100));

    double avgRowsPerPartition = static_cast<double>(totalInputRows_) /
        std::max(1UL, static_cast<uint64_t>(numPartitions_));
    // Convert to integer by multiplying by 100 (represents average * 100)
    lockedStats->runtimeStats["avgRowsPerPartition*100"] =
        RuntimeMetric(static_cast<int64_t>(avgRowsPerPartition * 100));
  }
}

} // namespace facebook::velox::exec
