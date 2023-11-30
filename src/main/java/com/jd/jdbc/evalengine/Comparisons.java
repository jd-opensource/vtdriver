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
import com.jd.jdbc.srvtopo.BindVariable;
import io.vitess.proto.Query;
import static io.vitess.proto.Query.Type.NULL_TYPE;
import java.sql.SQLException;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

public class Comparisons {

    public interface ComparisonOp {
        /**
         * @param left
         * @param right
         * @return
         * @throws SQLException
         */
        Boolean compare(EvalResult left, EvalResult right) throws SQLException;


        /**
         * @return
         */
        String string();
    }

    @Getter
    @AllArgsConstructor
    public static class ComparisonExpr implements EvalEngine.Expr {

        private final ComparisonOp op;

        private final EvalEngine.Expr left;

        private final EvalEngine.Expr right;

        @Override
        public EvalResult evaluate(EvalEngine.ExpressionEnv env) throws SQLException {
            EvalResult lval;
            if (this.left != null) {
                lval = this.left.evaluate(env);
            } else {
                lval = new EvalResult(NULL_TYPE);
            }
            EvalResult rval;
            if (this.right != null) {
                rval = this.right.evaluate(env);
            } else {
                rval = new EvalResult(NULL_TYPE);
            }
            Boolean cmp = op.compare(lval, rval);
            long value = 0;
            if (cmp) {
                value = TRUE_FLAG;
            }
            return new EvalResult(value, Query.Type.INT16);
        }

        @Override
        public EvalResult eval(EvalEngine.ExpressionEnv env, EvalResult result) throws SQLException {
            EvalResult lval;
            if (this.left != null) {
                lval = this.left.evaluate(env);
            } else {
                lval = new EvalResult(NULL_TYPE);
            }
            EvalResult rval;
            if (this.right != null) {
                rval = this.right.evaluate(env);
            } else {
                rval = new EvalResult(NULL_TYPE);
            }
            Boolean cmp = op.compare(lval, rval);
            long value = 0;
            if (cmp) {
                value = TRUE_FLAG;
            }
            return new EvalResult(value, Query.Type.INT16);
        }

        @Override
        public Query.Type type(EvalEngine.ExpressionEnv env) throws SQLException {
            // return null;
            return Query.Type.INT16;
        }

        @Override
        public int getFlags() {
            return 0;
        }

        @Override
        public String string() {
            return left.string() + op.string() + right.string();
        }

        @Override
        public boolean constant() {
            return false;
        }

        @Override
        public void output(StringBuilder builder, boolean wrap, Map<String, BindVariable> bindVariableMap) throws SQLException {

        }
    }

    public static class CompareEQ implements ComparisonOp {
        @Override
        public Boolean compare(EvalResult left, EvalResult right) throws SQLException {
            return EvalEngine.allCompare(left.resultValue(), right.resultValue()) == 0;
        }

        @Override
        public String string() {
            return "=";
        }
    }

    public static class CompareNE implements ComparisonOp {
        @Override
        public Boolean compare(EvalResult left, EvalResult right) throws SQLException {
            return EvalEngine.allCompare(left.resultValue(), right.resultValue()) != 0;
        }

        @Override
        public String string() {
            return "!=";
        }
    }

    public static class CompareLT implements ComparisonOp {
        @Override
        public Boolean compare(EvalResult left, EvalResult right) throws SQLException {
            // left < right
            return EvalEngine.allCompare(left.resultValue(), right.resultValue()) < 0;
        }

        @Override
        public String string() {
            return "<";
        }
    }

    public static class CompareLE implements ComparisonOp {
        @Override
        public Boolean compare(EvalResult left, EvalResult right) throws SQLException {
            // left <= right
            return EvalEngine.allCompare(left.resultValue(), right.resultValue()) <= 0;
        }

        @Override
        public String string() {
            return "<=";
        }
    }

    public static class CompareGT implements ComparisonOp {
        @Override
        public Boolean compare(EvalResult left, EvalResult right) throws SQLException {
            // left > right
            return EvalEngine.allCompare(left.resultValue(), right.resultValue()) > 0;
        }

        @Override
        public String string() {
            return ">";
        }
    }

    public static class CompareGE implements ComparisonOp {
        @Override
        public Boolean compare(EvalResult left, EvalResult right) throws SQLException {
            // left >= right
            return EvalEngine.allCompare(left.resultValue(), right.resultValue()) >= 0;
        }

        @Override
        public String string() {
            return ">=";
        }
    }

    public static class CompareNullSafeEQ implements ComparisonOp {
        @Override
        public Boolean compare(EvalResult left, EvalResult right) throws SQLException {
            return EvalEngine.nullSafeCompare(left.resultValue(), right.resultValue()) == 0;
        }

        @Override
        public String string() {
            return "<=>";
        }
    }


}
