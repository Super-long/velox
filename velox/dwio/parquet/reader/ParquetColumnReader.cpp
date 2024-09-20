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

//
// Created by Ying Su on 2/14/22.
//

#include "velox/dwio/parquet/reader/ParquetColumnReader.h"

#include "velox/dwio/common/SelectiveColumnReaderInternal.h"
#include "velox/dwio/parquet/reader/BooleanColumnReader.h"
#include "velox/dwio/parquet/reader/FloatingPointColumnReader.h"
#include "velox/dwio/parquet/reader/IntegerColumnReader.h"
#include "velox/dwio/parquet/reader/RepeatedColumnReader.h"
#include "velox/dwio/parquet/reader/StringColumnReader.h"
#include "velox/dwio/parquet/reader/StructColumnReader.h"
#include "velox/dwio/parquet/reader/TimestampColumnReader.h"

namespace facebook::velox::parquet {

// static
std::unique_ptr<dwio::common::SelectiveColumnReader> ParquetColumnReader::build(
    const TypePtr& requestedType,
    const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
    ParquetParams& params,
    common::ScanSpec& scanSpec) {
  auto colName = scanSpec.fieldName();

  switch (fileType->type()->kind()) {
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::SMALLINT:
    case TypeKind::TINYINT:
    case TypeKind::HUGEINT:
      //std::cout << "ParquetColumnReader::build IntegerColumnReader\n";
      return std::make_unique<IntegerColumnReader>(
          requestedType, fileType, params, scanSpec);

    case TypeKind::REAL:
      //std::cout << "ParquetColumnReader::build FloatingPointColumnReader\n";
      return std::make_unique<FloatingPointColumnReader<float, float>>(
          requestedType, fileType, params, scanSpec);
    case TypeKind::DOUBLE:
      //std::cout << "ParquetColumnReader::build FloatingPointColumnReader\n";
      return std::make_unique<FloatingPointColumnReader<double, double>>(
          requestedType, fileType, params, scanSpec);

    case TypeKind::ROW:
      //std::cout << "ParquetColumnReader::build StructColumnReader\n";
      return std::make_unique<StructColumnReader>(
          requestedType, fileType, params, scanSpec);

    case TypeKind::VARBINARY:
    case TypeKind::VARCHAR:
      //std::cout << "ParquetColumnReader::build StringColumnReader\n";
      return std::make_unique<StringColumnReader>(fileType, params, scanSpec);

    case TypeKind::ARRAY:
      //std::cout << "ParquetColumnReader::build ListColumnReader\n";
      return std::make_unique<ListColumnReader>(
          requestedType, fileType, params, scanSpec);

    case TypeKind::MAP:
      //std::cout << "ParquetColumnReader::build MapColumnReader\n";
      return std::make_unique<MapColumnReader>(
          requestedType, fileType, params, scanSpec);

    case TypeKind::BOOLEAN:
      //std::cout << "ParquetColumnReader::build BooleanColumnReader\n";
      return std::make_unique<BooleanColumnReader>(
          requestedType, fileType, params, scanSpec);

    case TypeKind::TIMESTAMP:
      //std::cout << "ParquetColumnReader::build TimestampColumnReader\n";
      return std::make_unique<TimestampColumnReader>(
          requestedType, fileType, params, scanSpec);

    default:
      VELOX_FAIL(
          "buildReader unhandled type: " +
          mapTypeKindToName(fileType->type()->kind()));
  }
}

} // namespace facebook::velox::parquet
