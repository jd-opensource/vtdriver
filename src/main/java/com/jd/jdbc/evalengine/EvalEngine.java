/*
Copyright 2021 JD Project Authors. Licensed under Apache-2.0.

Copyright 2019 The Vitess Authors.

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

import com.jd.jdbc.key.Bytes;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.vitess.resultset.ResultSetUtil;
import io.vitess.proto.Query;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

public class EvalEngine {

    /**
     * Cast converts a Value to the target type.
     *
     * @param value
     * @param type
     * @return
     * @throws SQLException
     */
    public static VtResultValue cast(VtResultValue value, Query.Type type) throws SQLException {
        if (value.getVtType() == type || value.isNull()) {
            return value;
        }
        if (type.equals(value.getVtType())) {
            return value;
        }
        if (value.getVtType() == Query.Type.EXPRESSION) {
            throw new SQLException(value.getVtType() + "(" + value.getValue() + ") cannot be cast to " + type.name());
        }
        return VtResultValue.newVtResultValue(type, value.getValue());
    }

    public static Integer nullSafeCompare(final VtResultValue value1, final VtResultValue value2) throws SQLException {
        if (value1.isNull()) {
            if (value2.isNull()) {
                return 0;
            }
            return -1;
        }
        if (value2.isNull()) {
            return 1;
        }
        if (value1.getValue().getClass().equals(value2.getValue().getClass())) {
            if (value1.getValue() instanceof Comparable && value2.getValue() instanceof Comparable) {
                Comparable c1 = (Comparable) value1.getValue();
                Comparable c2 = (Comparable) value2.getValue();
                return c1.compareTo(c2);
            }
        } else if (value1.getValue() instanceof Number && value2.getValue() instanceof Number) {
            long v1 = ((Number) value1.getValue()).longValue();
            long v2 = ((Number) value2.getValue()).longValue();
            return Objects.compare(v1, v2, Long::compareTo);
        }
        if (isByteComparable(value1) && isByteComparable(value2)) {
            if (value1.getValue() instanceof byte[] && value2.getValue() instanceof byte[]) {
                // 按字节排序, 以下这种情况不能正确处理:
                // 111:00:00 12:00:00
                return Bytes.compare((byte[]) value1.getValue(), (byte[]) value2.getValue());
            }
        }
        throw new SQLException("types are not comparable: " + value1.getVtType() + " vs " + value2.getVtType());
    }

    private static Boolean isByteComparable(final VtResultValue value) {
        switch (value.getVtType()) {
            case BLOB:
            case VARBINARY:
            case BINARY:
            case TIMESTAMP:
            case DATE:
            case TIME:
            case DATETIME:
            case YEAR:
                return true;
            default:
                return false;
        }
    }

    public static VtResultValue nullSafeAdd(VtResultValue value1, VtResultValue value2, Query.Type resultType) throws SQLException {
        if (value1.isNull() && value2.isNull()) {
            return VtResultValue.newVtResultValue(resultType, 0);
        }
        if (value1.isNull()) {
            return VtResultValue.newVtResultValue(resultType, value2.getValue());
        }
        if (value2.isNull()) {
            return VtResultValue.newVtResultValue(resultType, value1.getValue());
        }
        if (Query.Type.INT64.equals(resultType)) {
            if (value1.getValue() instanceof Number && value2.getValue() instanceof Number) {
                long v1 = ((Number) value1.getValue()).longValue();
                long v2 = ((Number) value2.getValue()).longValue();
                long result = v1 + v2;
                if (result < 0) {
                    BigInteger b1 = BigInteger.valueOf(v1);
                    BigInteger b2 = BigInteger.valueOf(v2);
                    BigInteger bResult = b1.add(b2);
                    return VtResultValue.newVtResultValue(Query.Type.DECIMAL, bResult);
                }
                return VtResultValue.newVtResultValue(Query.Type.INT64, result);
            }
        }
        if (Query.Type.DECIMAL.equals(resultType)) {
            BigDecimal b1 = (BigDecimal) ResultSetUtil.convertValue(value1, BigDecimal.class);
            BigDecimal b2 = (BigDecimal) ResultSetUtil.convertValue(value2, BigDecimal.class);
            BigDecimal bResult = b1.add(b2);
            return VtResultValue.newVtResultValue(Query.Type.DECIMAL, bResult);
        }
        throw new SQLException("nullSafeAdd error");
    }

    /**
     * Min returns the minimum of v1 and v2. If one of the
     * Min returns the minimum of v1 and v2. If one of the
     * are NULL, it returns NULL
     *
     * @param value1
     * @param value2
     * @return
     * @throws SQLException
     */
    public static VtResultValue min(VtResultValue value1, VtResultValue value2) throws SQLException {
        return Arithmetic.minmax(value1, value2, true);
    }

    /**
     * Max returns the maximum of v1 and v2. If one of the
     * values is NULL, it returns the other value. If both
     * are NULL, it returns NULL.
     *
     * @param value1
     * @param value2
     * @return
     * @throws SQLException
     */
    public static VtResultValue max(VtResultValue value1, VtResultValue value2) throws SQLException {
        return Arithmetic.minmax(value1, value2, false);
    }

    public static BigInteger toUint64(VtValue v) throws SQLException {
        EvalResult num = Arithmetic.newIntegralNumeric(v);
        switch (num.type) {
            case INT64:
                if (num.ival < 0) {
                    throw new SQLException("negative number cannot be converted to unsigned: " + num.ival);
                }
                return BigInteger.valueOf(num.ival);
            case UINT64:
                return num.uval;
            default:
                break;
        }
        throw new SQLException("unreachable");
    }

    public static Expr newLiteralIntFromBytes(byte[] val) {
        Long ival = Long.parseLong(new String(val));
        return newLiteralInt(ival);
    }

    public static Expr newLiteralInt(Long i) {
        return new Literal(new EvalResult(i, Query.Type.INT64));
    }

    public static Expr newLiteralFloat(byte[] val) {
        Double fval = Double.parseDouble(new String(val));
        return new Literal(new EvalResult(fval, Query.Type.FLOAT64));
    }

    public static Expr newLiteralString(byte[] val) {
        return new Literal(new EvalResult(val, Query.Type.VARBINARY));
    }

    private static EvalResult evaluateByType(VtValue val) throws SQLException {
        switch (val.getVtType()) {
            case INT64:
                Long lval = Long.valueOf(new String(val.getVtValue()));
                return new EvalResult(lval, Query.Type.INT64);
            case INT32:
                Long ival = Long.valueOf(new String(val.getVtValue()));
                return new EvalResult(ival, Query.Type.INT32);
            case UINT64:
                BigInteger uval = new BigInteger(new String(val.getVtValue()));
                return new EvalResult(uval, Query.Type.UINT64);
            case FLOAT64:
                Double fval = Double.valueOf(new String(val.getVtValue()));
                return new EvalResult(fval, Query.Type.FLOAT64);
            case VARCHAR:
            case TEXT:
            case VARBINARY:
                return new EvalResult(val.getVtValue(), Query.Type.VARBINARY);
            case NULL_TYPE:
                return new EvalResult(Query.Type.NULL_TYPE);
        }
        throw new SQLException("Type is not supported: " + val.getVtType());
    }

    private static Query.Type mergeNumericalTypes(Query.Type ltype, Query.Type rtype) {
        switch (ltype) {
            case INT64:
                if (rtype == Query.Type.UINT64 || rtype == Query.Type.FLOAT64) {
                    return rtype;
                }
            case UINT64:
                if (rtype == Query.Type.FLOAT64) {
                    return rtype;
                }
        }
        return ltype;
    }

    /**
     * Expr is the interface that all evaluating expressions must implement
     */
    public interface Expr {
        /**
         * @param env
         * @return
         */
        EvalResult evaluate(ExpressionEnv env) throws SQLException;

        /**
         * @param env
         * @return
         */
        Query.Type type(ExpressionEnv env) throws SQLException;

        /**
         * @return
         */
        String string();
    }

    /**
     * inaryExpr allows binary expressions to not have to evaluate child expressions - this is done by the BinaryOp
     */
    public interface BinaryExpr {
        /**
         * @param left
         * @param right
         * @return
         * @throws SQLException
         */
        EvalResult evaluate(EvalResult left, EvalResult right) throws SQLException;

        /**
         * @param left
         * @return
         */
        Query.Type type(Query.Type left);

        /**
         * @return
         */
        String string();
    }

    @Getter
    @Setter
    public static class EvalResult {
        private Query.Type type;

        private Long ival;

        private BigInteger uval;

        private Double fval;

        private byte[] bytes;

        public EvalResult() {
        }

        public EvalResult(byte[] bytes, Query.Type type) {
            this.bytes = bytes;
            this.type = type;
        }

        public EvalResult(Long ival, Query.Type type) {
            this.ival = ival;
            this.type = type;
        }

        public EvalResult(BigInteger uval, Query.Type type) {
            this.uval = uval;
            this.type = type;
        }

        public EvalResult(Double fval, Query.Type type) {
            this.fval = fval;
            this.type = type;
        }

        public EvalResult(Query.Type type) {
            this.type = type;
        }

        @Override
        public String toString() {
            switch (this.type) {
                case INT64:
                    return "Type: " + this.type + ", Value: " + this.ival;
                case UINT64:
                    return "Type: " + this.type + ", Value: " + this.uval.toString();
                case FLOAT64:
                    return "Type: " + this.type + ", Value: " + this.fval.toString();
                default:
                    return "Type: " + this.type + ", Value: " + this.bytes;
            }
        }

        /**
         * Value allows for retrieval of the value we expose for public consumption
         *
         * @return
         */
        public VtValue value() throws SQLException {
            if (this.type == Query.Type.INT32 || this.type == Query.Type.INT24 || this.type == Query.Type.INT16) {
                this.type = Query.Type.INT64;
            }
            return Arithmetic.castFromNumeric(this, this.type);
        }

        public VtResultValue resultValue() throws SQLException {
            if (this.type == Query.Type.INT32 || this.type == Query.Type.INT24 || this.type == Query.Type.INT16) {
                this.type = Query.Type.INT64;
            }
            return Arithmetic.castFromNum(this, this.type);
        }
    }

    /**
     * ExpressionEnv contains the `environment that the expression
     * evaluates in, such as the current row and bindvars
     */
    @AllArgsConstructor
    @Setter
    @Getter
    public static class ExpressionEnv {
        private Map<String, com.jd.jdbc.srvtopo.BindVariable> bindVariableMap;

        private List<VtResultValue> row;

        public ExpressionEnv(Map<String, com.jd.jdbc.srvtopo.BindVariable> bindVariableMap) {
            this.bindVariableMap = bindVariableMap;
            this.row = new ArrayList<>();
        }
    }

    public static class BindVariable implements Expr {
        private final String key;

        public BindVariable(String key) {
            this.key = key;
        }

        @Override
        public EvalResult evaluate(ExpressionEnv env) throws SQLException {
            if (!env.bindVariableMap.containsKey(this.key)) {
                throw new SQLException("Bind variable not found");
            }
            return evaluateByType(VtValue.newVtValue(env.bindVariableMap.get(this.key)));
        }

        @Override
        public Query.Type type(ExpressionEnv env) throws SQLException {
            if (!env.bindVariableMap.containsKey(this.key)) {
                throw new SQLException("query arguments missing for index: " + this.key);
            }
            return env.bindVariableMap.get(this.key).getType();
        }

        @Override
        public String string() {
            return ":" + this.key;
        }
    }

    public static class Literal implements Expr {
        private static final Log logger = LogFactory.getLog(Literal.class);

        @Getter
        private final EvalResult val;

        public Literal(EvalResult val) {
            this.val = val;
        }

        @Override
        public EvalResult evaluate(ExpressionEnv env) {
            return this.val;
        }

        @Override
        public Query.Type type(ExpressionEnv env) {
            return this.val.getType();
        }

        @Override
        public String string() {
            try {
                return this.val.value().toString();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                return "";
            }
        }
    }

    @Getter
    @AllArgsConstructor
    public static class BinaryOp implements Expr {

        private final BinaryExpr expr;

        private final Expr left;

        private final Expr right;

        @Override
        public EvalResult evaluate(ExpressionEnv env) throws SQLException {
            EvalResult lval = this.left.evaluate(env);
            EvalResult rval = this.right.evaluate(env);

            if (lval.type == Query.Type.INT32 || lval.type == Query.Type.INT24 || lval.type == Query.Type.INT16 || lval.type == Query.Type.INT8) {
                lval.type = Query.Type.INT64;
            }

            if (rval.type == Query.Type.INT32 || rval.type == Query.Type.INT24 || rval.type == Query.Type.INT16 || rval.type == Query.Type.INT8) {
                rval.type = Query.Type.INT64;
            }

            return this.expr.evaluate(lval, rval);
        }

        @Override
        public Query.Type type(ExpressionEnv env) throws SQLException {
            Query.Type ltype = this.left.type(env);
            Query.Type rtype = this.right.type(env);
            Query.Type type = mergeNumericalTypes(ltype, rtype);
            return this.expr.type(type);
        }

        @Override
        public String string() {
            return null;
        }
    }

    public static class Addition implements BinaryExpr {

        @Override
        public EvalResult evaluate(EvalResult left, EvalResult right) throws SQLException {
            return Arithmetic.addNumericWithError(left, right);
        }

        @Override
        public Query.Type type(Query.Type left) {
            return left;
        }

        @Override
        public String string() {
            return "+";
        }
    }

    public static class Subtraction implements BinaryExpr {

        @Override
        public EvalResult evaluate(EvalResult left, EvalResult right) throws SQLException {
            return Arithmetic.subtractNumericWithError(left, right);
        }

        @Override
        public Query.Type type(Query.Type left) {
            return left;
        }

        @Override
        public String string() {
            return "-";
        }
    }

    public static class Multiplication implements BinaryExpr {

        @Override
        public EvalResult evaluate(EvalResult left, EvalResult right) throws SQLException {
            return Arithmetic.multiplyNumericWithError(left, right);
        }

        @Override
        public Query.Type type(Query.Type left) {
            return left;
        }

        @Override
        public String string() {
            return "*";
        }
    }

    public static class Division implements BinaryExpr {

        @Override
        public EvalResult evaluate(EvalResult left, EvalResult right) throws SQLException {
            return Arithmetic.divideNumericWithError(left, right);
        }

        @Override
        public Query.Type type(Query.Type left) {
            return Query.Type.FLOAT64;
        }

        @Override
        public String string() {
            return "/";
        }
    }
}
