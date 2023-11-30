/*
Copyright 2023 JD Project Authors. Licensed under Apache-2.0.

Copyright 2022 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.jd.jdbc.evalengine;

import static com.jd.jdbc.evalengine.EvalEngine.TRUE_FLAG;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import io.vitess.proto.Query;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class Logical {
    @Getter
    @AllArgsConstructor
    public static class LogicalExpr implements EvalEngine.BinaryExpr {
        SQLBinaryOperator op;

        @Override
        public EvalResult evaluate(EvalResult left, EvalResult right) throws SQLException {
            Boolean cmp = false;
            if (op.equals(SQLBinaryOperator.BooleanAnd)) {
                long leftEvalResult = left.value().toLong();
                long rightEvalResult = right.value().toLong();
                cmp = (leftEvalResult == TRUE_FLAG) && (rightEvalResult == TRUE_FLAG);
            } else if (op.equals(SQLBinaryOperator.BooleanOr)) {
                long leftEvalResult = left.value().toLong();
                long rightEvalResult = right.value().toLong();
                cmp = (leftEvalResult == TRUE_FLAG) || (rightEvalResult == TRUE_FLAG);
            } else if (op.equals(SQLBinaryOperator.BooleanXor)) {
                long leftEvalResult = left.value().toLong();
                long rightEvalResult = right.value().toLong();
                cmp = leftEvalResult != rightEvalResult;
            } else {
                throw new SQLFeatureNotSupportedException(op.name);
            }
            long value = 0;
            if (cmp) {
                value = TRUE_FLAG;
            }
            return new EvalResult(value, Query.Type.INT16);
        }

        @Override
        public Query.Type type(Query.Type left) {
            return null;
        }

        @Override
        public String string() {
            return op.name;
        }
    }

    public static class IsExpr implements EvalEngine.BinaryExpr {
        boolean isNot;

        SQLExpr right;

        public IsExpr(boolean isNot) {
            this.isNot = isNot;
        }

        @Override
        public EvalResult evaluate(EvalResult left, EvalResult right) throws SQLException {
            //todo
         /*   Boolean cmp = false;
            SQLExpr expr = right.getExpr();
            if (expr instanceof SQLNullExpr) {
                cmp = left == null;
            }
            if (expr instanceof SQLNotNullConstraint) {
                cmp = left != null;
            }

            if (expr instanceof SQLBooleanExpr) {
                long leftEvalResult = left.value().toLong();
                if (this.isNot && ((SQLBooleanExpr) expr).getBooleanValue()) { // not true
                    cmp = leftEvalResult != TRUE_FLAG;
                } else if (this.isNot) {  // not false
                    cmp = leftEvalResult == TRUE_FLAG;
                } else if (((SQLBooleanExpr) expr).getBooleanValue()) { // true
                    cmp = leftEvalResult == TRUE_FLAG;
                } else { // false
                    cmp = leftEvalResult != TRUE_FLAG;
                }
            }

            long value = 0;
            if (cmp) {
                value = TRUE_FLAG;
            }
            return new EvalResult(value, Query.Type.INT16);*/
            return null;
        }

        @Override
        public Query.Type type(Query.Type left) {
            return Query.Type.INT64;
        }

        @Override
        public String string() {
            return "is " + (isNot ? "not " : "") + right.toString();
        }
    }

}
