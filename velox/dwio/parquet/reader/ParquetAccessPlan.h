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

#include "velox/dwio/common/Statistics.h"
#include "velox/dwio/common/compression/Compression.h"

namespace facebook::velox::parquet {

struct RowSelector {
    int row_count;
    bool skip;

    RowSelector() : row_count(0), skip(false) {}

    void Select(int count) {
        row_count = count;
        skip = false;
    }

    void Skip(int count) {
        row_count = count;
        skip = true;
    }

    std::string toString() const {
        std::ostringstream oss;
        oss << "RowSelector(row_count=" << row_count << ", skip=" << (skip ? "true" : "false") << ")";
        return oss.str();
    }
};

struct RowSelection {
    std::vector<RowSelector> row_selection;

    RowSelection() {}

    bool IsSelect() {
        for (const auto& selector : row_selection) {
            if (!selector.skip) {
                return true;
            }
        }
        return false;
    }

    std::string toString() const {
        std::ostringstream oss;
        oss << "RowSelection(";
        for (const auto& selector : row_selection) {
            oss << selector.toString() << ", ";
        }
        oss << ")";
        return oss.str();
    }
};

class RowGroupAccess {
public:
    RowGroupAccess(bool skip = false, bool all = false) : is_skip_(skip), is_all_(all) {}

    bool ShouldScan() {
        if (is_skip_) {
            return false;
        } else if (is_all_ || row_selection_.IsSelect()) {
            return true;
        } else {
            return false;
        }
    }

    // TODO(kavinli): row_selection之间应该合并，而不是覆盖
    void SetRowGroupAccess(const RowSelection& row_selection) {
        row_selection_ = row_selection;
        is_all_ = false;
    }
    void SetSkip(bool is_skip) { is_skip_ = is_skip; }
    void SetAll(bool is_all) { is_all_ = is_all; }

    RowSelection GetRowGroupAccess() const noexcept { return row_selection_; }
    bool IsSkip() const noexcept { return is_skip_; }
    bool IsAll() const noexcept { return is_all_; }

    std::string toString() const {
        std::ostringstream oss;
        oss << "RowGroupAccess(is_skip=" << (is_skip_ ? "true" : "false")
            << ", is_all=" << (is_all_ ? "true" : "false")
            << ", row_selection=" << row_selection_.toString() << ")";
        return oss.str();
    }
private:
    bool is_skip_;
    bool is_all_;
    RowSelection row_selection_;
};

class ParquetAccessPlan {
  public:
    static ParquetAccessPlan NewAll(size_t row_group_count) {
        ParquetAccessPlan plan;
        plan.row_groups_.resize(row_group_count, RowGroupAccess(false, true));
        return plan;
    }

    static ParquetAccessPlan NewNone(size_t row_group_count) {
        ParquetAccessPlan plan;
        plan.row_groups_.resize(row_group_count, RowGroupAccess(true, false));
        return plan;
    }

    void Set(int index, const RowGroupAccess& row_group_access) {
        if (index >= 0 && index < row_groups_.size()) {
            row_groups_[index] = row_group_access;
        }
    }

    size_t Size() const noexcept { return row_groups_.size(); }

    bool ShouldScan(int index) {
        if (index >= 0 && index < row_groups_.size()) {
            return row_groups_[index].ShouldScan();
        }
        return false;
    }

    void Skip(int index) {
        if (index >= 0 && index < row_groups_.size()) {
            row_groups_[index].SetSkip(true);
        }
    }

    void Scan(int index) {
        if (index >= 0 && index < row_groups_.size()) {
            row_groups_[index].SetAll(true);
        }
    }

    void ScanSelection(int index, const RowSelection& selection) {
        if (index >= 0 && index < row_groups_.size()) {
            if (!row_groups_[index].IsSkip()) {
                row_groups_[index].SetRowGroupAccess(selection);
            }
        }
    }

    std::string toString() const {
        std::ostringstream oss;
        oss << "ParquetAccessPlan(row_groups=[";
        for (const auto& access : row_groups_) {
            oss << access.toString() << ", ";
        }
        oss << "])";
        return oss.str();
    }
  private:
    std::vector<RowGroupAccess> row_groups_;
};

} // namespace facebook::velox::parquet