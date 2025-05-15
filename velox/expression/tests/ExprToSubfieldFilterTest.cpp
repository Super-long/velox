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

#include "velox/expression/ExprToSubfieldFilter.h"
#include <gtest/gtest.h>
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"

namespace facebook::velox::exec {
namespace {

using namespace facebook::velox::common;

void validateSubfield(
    const Subfield& subfield,
    const std::vector<std::string>& expectedPath) {
  ASSERT_EQ(subfield.path().size(), expectedPath.size());
  for (int i = 0; i < expectedPath.size(); ++i) {
    ASSERT_TRUE(subfield.path()[i]);
    ASSERT_EQ(*subfield.path()[i], Subfield::NestedField(expectedPath[i]));
  }
}

class ExprToSubfieldFilterTest : public testing::Test {
 public:
  static void SetUpTestSuite() {
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
    memory::MemoryManager::testingSetInstance({});
  }

  core::TypedExprPtr parseExpr(
      const std::string& expr,
      const RowTypePtr& type) {
    return core::Expressions::inferTypes(
        parse::parseExpr(expr, {}), type, pool_.get());
  }

  core::CallTypedExprPtr parseCallExpr(
      const std::string& expr,
      const RowTypePtr& type) {
    auto call = std::dynamic_pointer_cast<const core::CallTypedExpr>(
        parseExpr(expr, type));
    VELOX_CHECK_NOT_NULL(call);
    return call;
  }

  core::ExpressionEvaluator* evaluator() {
    return &evaluator_;
  }

 private:
  std::shared_ptr<memory::MemoryPool> pool_ =
      memory::memoryManager()->addLeafPool();
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  SimpleExpressionEvaluator evaluator_{queryCtx_.get(), pool_.get()};
};

TEST_F(ExprToSubfieldFilterTest, eq) {
  auto call = parseCallExpr("a = 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  auto bigintRange = dynamic_cast<BigintRange*>(filter.get());
  ASSERT_TRUE(bigintRange);
  ASSERT_EQ(bigintRange->lower(), 42);
  ASSERT_EQ(bigintRange->upper(), 42);
  ASSERT_FALSE(bigintRange->testNull());

  call = parseCallExpr("b = 42.0", ROW({{"b", DOUBLE()}}));
  Subfield subfield2;
  filter = leafCallToSubfieldFilter(*call, subfield2, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield2, {"b"});
  auto doubleRange = dynamic_cast<DoubleRange*>(filter.get());
  ASSERT_TRUE(doubleRange);
  ASSERT_EQ(doubleRange->lower(), 42);
  ASSERT_EQ(doubleRange->upper(), 42);
  ASSERT_FALSE(doubleRange->testNull());
  ASSERT_TRUE(doubleRange->testDouble(42));

  call = parseCallExpr("c = cast('1' as VARBINARY)", ROW({{"c", VARBINARY()}}));
  Subfield subfield3;
  filter = leafCallToSubfieldFilter(*call, subfield3, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield3, {"c"});
  auto byteRange = dynamic_cast<BytesValues*>(filter.get());
  ASSERT_TRUE(byteRange);
  ASSERT_FALSE(byteRange->testNull());
  std::string cmp = "1";
  ASSERT_TRUE(byteRange->testBytes(cmp.c_str(), cmp.size()));
}

TEST_F(ExprToSubfieldFilterTest, eqExpr) {
  auto call = parseCallExpr("a = 21 * 2", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  auto bigintRange = dynamic_cast<BigintRange*>(filter.get());
  ASSERT_TRUE(bigintRange);
  ASSERT_EQ(bigintRange->lower(), 42);
  ASSERT_EQ(bigintRange->upper(), 42);
  ASSERT_FALSE(bigintRange->testNull());

  call = parseCallExpr("b = 21.0+21.0", ROW({{"b", DOUBLE()}}));
  Subfield subfield2;
  filter = leafCallToSubfieldFilter(*call, subfield2, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield2, {"b"});
  auto doubleRange = dynamic_cast<DoubleRange*>(filter.get());
  ASSERT_TRUE(doubleRange);
  ASSERT_EQ(doubleRange->lower(), 42);
  ASSERT_EQ(doubleRange->upper(), 42);
  ASSERT_FALSE(doubleRange->testNull());
  ASSERT_TRUE(doubleRange->testDouble(42));
}

TEST_F(ExprToSubfieldFilterTest, eqSubfield) {
  auto call = parseCallExpr("a.b = 42", ROW({{"a", ROW({{"b", BIGINT()}})}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a", "b"});
  auto bigintRange = dynamic_cast<BigintRange*>(filter.get());
  ASSERT_TRUE(bigintRange);
  ASSERT_EQ(bigintRange->lower(), 42);
  ASSERT_EQ(bigintRange->upper(), 42);
  ASSERT_FALSE(bigintRange->testNull());

  call = parseCallExpr("c.d = 21.0 * 2.0", ROW({{"c", ROW({{"d", DOUBLE()}})}}));
  Subfield subfield2;
  filter = leafCallToSubfieldFilter(*call, subfield2, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield2, {"c", "d"});
  auto doubleRange = dynamic_cast<DoubleRange*>(filter.get());
  ASSERT_TRUE(doubleRange);
  ASSERT_EQ(doubleRange->lower(), 42);
  ASSERT_EQ(doubleRange->upper(), 42);
  ASSERT_FALSE(doubleRange->testNull());
  ASSERT_TRUE(doubleRange->testDouble(42));  
}

TEST_F(ExprToSubfieldFilterTest, neq) {
  auto call = parseCallExpr("a <> 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_TRUE(filter->testInt64(41));
  ASSERT_FALSE(filter->testInt64(42));
  ASSERT_TRUE(filter->testInt64(43));

  call = parseCallExpr("b <> 42.0", ROW({{"b", DOUBLE()}}));
  Subfield subfield2;
  filter = leafCallToSubfieldFilter(*call, subfield2, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield2, {"b"});
  ASSERT_TRUE(filter->testDouble(41));
  ASSERT_FALSE(filter->testDouble(42));
  ASSERT_TRUE(filter->testDouble(43));
}

TEST_F(ExprToSubfieldFilterTest, lte) {
  auto call = parseCallExpr("a <= 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_TRUE(filter->testInt64(41));
  ASSERT_TRUE(filter->testInt64(42));
  ASSERT_FALSE(filter->testInt64(43));

  call = parseCallExpr("b <= 42.0", ROW({{"b", DOUBLE()}}));
  Subfield subfield2;
  filter = leafCallToSubfieldFilter(*call, subfield2, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield2, {"b"});
  ASSERT_FALSE(filter->testNull());
  ASSERT_TRUE(filter->testDouble(41));
  ASSERT_TRUE(filter->testDouble(42));
  ASSERT_FALSE(filter->testDouble(43));
}

TEST_F(ExprToSubfieldFilterTest, lt) {
  auto call = parseCallExpr("a < 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_TRUE(filter->testInt64(41));
  ASSERT_FALSE(filter->testInt64(42));
  ASSERT_FALSE(filter->testInt64(43));
}

TEST_F(ExprToSubfieldFilterTest, gte) {
  auto call = parseCallExpr("a >= 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_FALSE(filter->testInt64(41));
  ASSERT_TRUE(filter->testInt64(42));
  ASSERT_TRUE(filter->testInt64(43));
}

TEST_F(ExprToSubfieldFilterTest, gt) {
  auto call = parseCallExpr("a > 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_FALSE(filter->testInt64(41));
  ASSERT_FALSE(filter->testInt64(42));
  ASSERT_TRUE(filter->testInt64(43));
}

TEST_F(ExprToSubfieldFilterTest, between) {
  auto call = parseCallExpr("a between 40 and 42", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  for (int i = 39; i <= 43; ++i) {
    ASSERT_EQ(filter->testInt64(i), 40 <= i && i <= 42);
  }
}

TEST_F(ExprToSubfieldFilterTest, in) {
  auto call = parseCallExpr("a in (40, 42)", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  for (int i = 39; i <= 43; ++i) {
    ASSERT_EQ(filter->testInt64(i), i == 40 || i == 42);
  }
}

TEST_F(ExprToSubfieldFilterTest, isNull) {
  auto call = parseCallExpr("a is null", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_FALSE(filter->testInt64(0));
  ASSERT_FALSE(filter->testInt64(42));
  ASSERT_TRUE(filter->testNull());
}

TEST_F(ExprToSubfieldFilterTest, isNotNull) {
  auto call = parseCallExpr("a is not null", ROW({{"a", BIGINT()}}));
  auto [subfield, filter] = toSubfieldFilter(call, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_TRUE(filter->testInt64(0));
  ASSERT_TRUE(filter->testInt64(42));
  ASSERT_FALSE(filter->testNull());
}

TEST_F(ExprToSubfieldFilterTest, like) {
  auto call = parseCallExpr("a like 'foo%'", ROW({{"a", VARCHAR()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_FALSE(filter);
}

TEST_F(ExprToSubfieldFilterTest, nonConstant) {
  auto call =
      parseCallExpr("a = b + 1", ROW({{"a", BIGINT()}, {"b", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_FALSE(filter);
}

TEST_F(ExprToSubfieldFilterTest, userError) {
  auto call = parseCallExpr("a = 1 / 0", ROW({{"a", BIGINT()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_FALSE(filter);
}

TEST_F(ExprToSubfieldFilterTest, dereferenceWithEmptyField) {
  auto call = std::make_shared<core::CallTypedExpr>(
      BOOLEAN(),
      std::vector<core::TypedExprPtr>{
          std::make_shared<core::DereferenceTypedExpr>(
              REAL(),
              std::make_shared<core::FieldAccessTypedExpr>(
                  ROW({{"", DOUBLE()}, {"", REAL()}, {"", BIGINT()}}),
                  std::make_shared<core::InputTypedExpr>(ROW(
                      {{"c0",
                        ROW({{"", DOUBLE()}, {"", REAL()}, {"", BIGINT()}})}})),
                  "c0"),
              1)},
      "is_null");
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_FALSE(filter);
}

TEST_F(ExprToSubfieldFilterTest, doubleBetweenAndInTests) {
  // 测试DOUBLE类型的between操作
  auto call = parseCallExpr("a between 40.5 and 42.5", ROW({{"a", DOUBLE()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  ASSERT_FALSE(filter->testDouble(40.0));
  ASSERT_TRUE(filter->testDouble(40.5));
  ASSERT_TRUE(filter->testDouble(41.5));
  ASSERT_TRUE(filter->testDouble(42.5));
  ASSERT_FALSE(filter->testDouble(43.0));
}

TEST_F(ExprToSubfieldFilterTest, varbinaryTests) {
  // 测试VARBINARY类型的等值比较
  auto call = parseCallExpr("a = cast('binary' as VARBINARY)", ROW({{"a", VARBINARY()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  auto bytesValues = dynamic_cast<BytesValues*>(filter.get());
  ASSERT_TRUE(bytesValues);
  ASSERT_FALSE(bytesValues->testNull());
  std::string binary = "binary";
  ASSERT_TRUE(bytesValues->testBytes(binary.c_str(), binary.size()));
  std::string other = "other";
  ASSERT_FALSE(bytesValues->testBytes(other.c_str(), other.size()));

  // 测试VARBINARY类型的不等比较
  call = parseCallExpr("a <> cast('binary' as VARBINARY)", ROW({{"a", VARBINARY()}}));
  Subfield subfield2;
  filter = leafCallToSubfieldFilter(*call, subfield2, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield2, {"a"});
  ASSERT_FALSE(filter->testBytes(binary.c_str(), binary.size()));
  ASSERT_TRUE(filter->testBytes(other.c_str(), other.size()));
}

TEST_F(ExprToSubfieldFilterTest, varbinaryComparisonTests) {
  // 测试VARBINARY类型的小于比较
  auto call = parseCallExpr("a < cast('binary' as VARBINARY)", ROW({{"a", VARBINARY()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  std::string before = "ainary";
  std::string target = "binary";
  std::string after = "cinary";
  ASSERT_TRUE(filter->testBytes(before.c_str(), before.size()));
  ASSERT_FALSE(filter->testBytes(target.c_str(), target.size()));
  ASSERT_FALSE(filter->testBytes(after.c_str(), after.size()));

  // 测试VARBINARY类型的大于比较
  call = parseCallExpr("a > cast('binary' as VARBINARY)", ROW({{"a", VARBINARY()}}));
  Subfield subfield2;
  filter = leafCallToSubfieldFilter(*call, subfield2, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield2, {"a"});
  ASSERT_FALSE(filter->testBytes(before.c_str(), before.size()));
  ASSERT_FALSE(filter->testBytes(target.c_str(), target.size()));
  ASSERT_TRUE(filter->testBytes(after.c_str(), after.size()));

  // 测试VARBINARY类型的小于等于比较
  call = parseCallExpr("a <= cast('binary' as VARBINARY)", ROW({{"a", VARBINARY()}}));
  Subfield subfield3;
  filter = leafCallToSubfieldFilter(*call, subfield3, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield3, {"a"});
  ASSERT_TRUE(filter->testBytes(before.c_str(), before.size()));
  ASSERT_TRUE(filter->testBytes(target.c_str(), target.size()));
  ASSERT_FALSE(filter->testBytes(after.c_str(), after.size()));

  // 测试VARBINARY类型的大于等于比较
  call = parseCallExpr("a >= cast('binary' as VARBINARY)", ROW({{"a", VARBINARY()}}));
  Subfield subfield4;
  filter = leafCallToSubfieldFilter(*call, subfield4, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield4, {"a"});
  ASSERT_FALSE(filter->testBytes(before.c_str(), before.size()));
  ASSERT_TRUE(filter->testBytes(target.c_str(), target.size()));
  ASSERT_TRUE(filter->testBytes(after.c_str(), after.size()));
}

TEST_F(ExprToSubfieldFilterTest, varbinaryInTests) {
  std::string before = "aaa";
  std::string middle = "bbb";
  std::string upper = "ccc";
  std::string after = "ddd";

  // 测试VARBINARY类型的in操作
  auto call = parseCallExpr("a in (cast('aaa' as VARBINARY), cast('ccc' as VARBINARY))", 
                      ROW({{"a", VARBINARY()}}));
  Subfield subfield2;
  auto filter = leafCallToSubfieldFilter(*call, subfield2, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield2, {"a"});
  ASSERT_TRUE(filter->testBytes(before.c_str(), before.size()));
  ASSERT_FALSE(filter->testBytes(middle.c_str(), middle.size()));
  ASSERT_TRUE(filter->testBytes(upper.c_str(), upper.size()));
  ASSERT_FALSE(filter->testBytes(after.c_str(), after.size()));
}

TEST_F(ExprToSubfieldFilterTest, varbinaryIsNullTests) {
  // 测试VARBINARY类型的is null操作
  auto call = parseCallExpr("a is null", ROW({{"a", VARBINARY()}}));
  Subfield subfield;
  auto filter = leafCallToSubfieldFilter(*call, subfield, evaluator());
  ASSERT_TRUE(filter);
  validateSubfield(subfield, {"a"});
  std::string value = "value";
  ASSERT_FALSE(filter->testBytes(value.c_str(), value.size()));
  ASSERT_TRUE(filter->testNull());
}
} // namespace
} // namespace facebook::velox::exec
