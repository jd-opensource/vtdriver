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

import com.jd.jdbc.common.util.CollectionUtils;
import com.jd.jdbc.key.Bytes;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLName;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOpExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLBinaryOperator;
import com.jd.jdbc.sqlparser.ast.expr.SQLCharExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIdentifierExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLPropertyExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLVariantRefExpr;
import com.jd.jdbc.sqlparser.ast.expr.VtOffset;
import com.jd.jdbc.sqltypes.VtNumberRange;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.vitess.resultset.ResultSetUtil;
import io.vitess.proto.Query;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

public class EvalEngine {

    public static final int TRUE_FLAG = 1;

    private static final Literal NULL_EXPR = new Literal(new EvalResult());

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

    public static Integer allCompare(final VtResultValue value1, final VtResultValue value2) throws SQLException {
        if (value1.isNull() || value2.isNull()) {
            return 0;
        }
        return nullSafeCompare(value1, value2);
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
        if (Query.Type.FLOAT64.equals(resultType)) {
            BigDecimal f1 = (BigDecimal) ResultSetUtil.convertValue(value1, BigDecimal.class);
            BigDecimal f2 = (BigDecimal) ResultSetUtil.convertValue(value2, BigDecimal.class);
            BigDecimal fResult = f1.add(f2);
            return VtResultValue.newVtResultValue(Query.Type.FLOAT64, fResult);
        }
        throw new SQLException("nullSafeAdd error");
    }

    public static VtResultValue divideNumericWithError(VtResultValue value1, VtResultValue value2) throws SQLException {
        if (value2.isNull()) {
            return VtResultValue.NULL;
        }
        if (value1.isNull()) {
            return VtResultValue.NULL;
        }

        BigDecimal b1 = (BigDecimal) ResultSetUtil.convertValue(value1, BigDecimal.class);
        BigDecimal b2 = (BigDecimal) ResultSetUtil.convertValue(value2, BigDecimal.class);

        if (b2.equals(BigDecimal.ZERO)) {
            return VtResultValue.NULL;
        }

        BigDecimal bResult = b1.divide(b2, 4, BigDecimal.ROUND_HALF_UP);
        return VtResultValue.newVtResultValue(Query.Type.DECIMAL, bResult);
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
        BigInteger uval = new BigInteger(val);
        if (uval.compareTo(new BigInteger(String.valueOf(VtNumberRange.INT64_MAX))) <= 0) {
            Long ival = Long.parseLong(new String(val));
            return newLiteralInt(ival);
        }
        return newLiteralUint(uval);
    }

    public static Expr newLiteralUint(BigInteger i) {
        return new Literal(new EvalResult(i, Query.Type.UINT64));
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
            case DECIMAL:
                BigDecimal bigDecimal = new BigDecimal(new String(val.getVtValue()));
                return new EvalResult(bigDecimal, Query.Type.DECIMAL);
            case VARCHAR:
            case TEXT:
            case VARBINARY:
                return new EvalResult(val.getVtValue(), Query.Type.VARBINARY);
            case NULL_TYPE:
                return new EvalResult(Query.Type.NULL_TYPE);
            default:
                throw new SQLException("Type is not supported: " + val.getVtType());
        }
    }

    private static EvalResult evaluateByType(VtResultValue val) throws SQLException {
        switch (val.getVtType()) {
            case INT64:
                Long lval = Long.valueOf(val.toString());
                return new EvalResult(lval, Query.Type.INT64);
            case INT32:
                Long ival = Long.valueOf(val.toString());
                return new EvalResult(ival, Query.Type.INT32);
            case UINT64:
                BigInteger uval = new BigInteger(val.toString());
                return new EvalResult(uval, Query.Type.UINT64);
            case FLOAT64:
                Double fval = Double.valueOf(val.toString());
                return new EvalResult(fval, Query.Type.FLOAT64);
            case DECIMAL:
                BigDecimal bigDecimal = new BigDecimal(val.toString());
                return new EvalResult(bigDecimal, Query.Type.DECIMAL);
            case VARCHAR:
            case TEXT:
            case VARBINARY:
                return new EvalResult(val.toBytes(), Query.Type.VARBINARY);
            case NULL_TYPE:
                return new EvalResult(Query.Type.NULL_TYPE);
            default:
                throw new SQLException("Type is not supported: " + val.getVtType());
        }
    }

    private static Query.Type mergeNumericalTypes(Query.Type ltype, Query.Type rtype) {
        switch (ltype) {
            case INT32:
                if (rtype == Query.Type.UINT32 || rtype == Query.Type.INT64 || rtype == Query.Type.UINT64 || rtype == Query.Type.FLOAT64 || rtype == Query.Type.DECIMAL) {
                    return rtype;
                }
                break;
            case INT64:
                if (rtype == Query.Type.UINT64 || rtype == Query.Type.FLOAT64 || rtype == Query.Type.DECIMAL) {
                    return rtype;
                }
                break;
            case UINT64:
                if (rtype == Query.Type.FLOAT64 || rtype == Query.Type.DECIMAL) {
                    return rtype;
                }
                break;
            case DECIMAL:
                break;
            default:
                throw new RuntimeException();
        }
        return ltype;
    }

    public static Expr translate(SQLExpr e, TranslationLookup translationLookup) throws SQLException {
        return translateEx(e, translationLookup, true);
    }

    public static Expr translate(List<SQLExpr> targetList, TranslationLookup lookup) throws SQLException {
        List<Expr> exprList = new ArrayList<>();
        for (SQLExpr expr : targetList) {
            Expr translate = translate(expr, lookup);
            if (translate == null) {
                return null;
            }
            exprList.add(translate);
        }
        return new TupleExpr(exprList);
    }

    public static Expr translateEx(SQLExpr e, TranslationLookup translationLookup, boolean simplify) throws SQLException {
        Expr expr = translateExpr(e, translationLookup);
        if (expr == null) {
            return expr;
        }
        if (simplify) {
            expr = Simplify.simplifyExpr(new EvalEngine.ExpressionEnv(), expr);
        }
        return expr;
    }

    public static Expr translateEx(List<SQLExpr> targetList, TranslationLookup lookup, boolean simplify) throws SQLException {
        Expr expr = translate(targetList, lookup);
        if (expr == null) {
            return expr;
        }
        if (simplify) {
            expr = Simplify.simplifyExpr(new EvalEngine.ExpressionEnv(), expr);
        }
        return expr;
    }

    private static Expr translateExpr(SQLExpr e, TranslationLookup translationLookup) throws SQLException {
        if (e instanceof SQLIdentifierExpr || e instanceof SQLPropertyExpr) {
            try {
                int idx = translationLookup.columnLookup((SQLName) e);
                if (idx >= 0) {
                    return new Column(idx);
                }
                return null;
            } catch (SQLException exception) {
                return null;
            }
        } else if (e instanceof SQLBinaryOpExpr) {
            return translateComparisonExpr(e, translationLookup);
        } else if (e instanceof VtOffset) {
            return new Column(((VtOffset) e).getValue());
        } else if (e instanceof SQLVariantRefExpr) {
            int index = ((SQLVariantRefExpr) e).getIndex();
            if (Integer.valueOf(-1).equals(index)) {
                // default value
                return new EvalEngine.BindVariable(((SQLVariantRefExpr) e).getName());
            }
            return new EvalEngine.BindVariable(String.valueOf(index));
        } else if (e instanceof SQLNullExpr) {
            return NULL_EXPR;
        } else if (e instanceof SQLIntegerExpr) {
            return EvalEngine.newLiteralIntFromBytes(((SQLIntegerExpr) e).getNumber().toString().getBytes());
        } else if (e instanceof SQLCharExpr) {
            return new Literal(new EvalResult(((SQLCharExpr) e).getText().getBytes(), Query.Type.VARCHAR));
        } else {
            // todo
            return new AnyExpr(e);
        }
    }

    private static Expr translateComparisonExpr(SQLExpr e, TranslationLookup translationLookup) throws SQLException {
        SQLExpr left = ((SQLBinaryOpExpr) e).getLeft();
        SQLExpr right = ((SQLBinaryOpExpr) e).getRight();
        Expr leftE = translateExpr(left, translationLookup);
        Expr rightE = translateExpr(right, translationLookup);
        BinaryExpr bExpr;
        SQLBinaryOperator op = ((SQLBinaryOpExpr) e).getOperator();
        switch (op) {
            case Equality:
                return new Comparisons.ComparisonExpr(new Comparisons.CompareEQ(), leftE, rightE);
            case NotEqual:
                return new Comparisons.ComparisonExpr(new Comparisons.CompareNE(), leftE, rightE);
            case LessThan:
                return new Comparisons.ComparisonExpr(new Comparisons.CompareLT(), leftE, rightE);
            case LessThanOrEqual:
                return new Comparisons.ComparisonExpr(new Comparisons.CompareLE(), leftE, rightE);
            case GreaterThan:
                return new Comparisons.ComparisonExpr(new Comparisons.CompareGT(), leftE, rightE);
            case GreaterThanOrEqual:
                return new Comparisons.ComparisonExpr(new Comparisons.CompareGE(), leftE, rightE);
            case LessThanOrEqualOrGreaterThan:
                return new Comparisons.ComparisonExpr(new Comparisons.CompareNullSafeEQ(), leftE, rightE);
            case BooleanAnd:
            case BooleanXor:
            case BooleanOr:
                bExpr = new Logical.LogicalExpr(op);
                return new BinaryOp(bExpr, leftE, rightE);
            case Add:
                bExpr = new EvalEngine.Addition();
                return new BinaryOp(bExpr, leftE, rightE);
            case Subtract:
                bExpr = new EvalEngine.Subtraction();
                return new BinaryOp(bExpr, leftE, rightE);
            case Multiply:
                bExpr = new EvalEngine.Multiplication();
                return new BinaryOp(bExpr, leftE, rightE);
            case Divide:
                bExpr = new EvalEngine.Division();
                return new BinaryOp(bExpr, leftE, rightE);
            case Is:
                bExpr = new Logical.IsExpr(false);
                return new BinaryOp(bExpr, leftE, rightE);
            case IsNot:
                bExpr = new Logical.IsExpr(true);
                return new BinaryOp(bExpr, leftE, rightE);
            default:
                throw new SQLFeatureNotSupportedException(op.name);
        }
    }

    /**
     * NullsafeHashcode returns an int64 hashcode that is guaranteed to be the same
     * for two values that are considered equal by `NullsafeCompare`.
     *
     * @param v
     * @param collation
     * @param coerceType
     * @return
     */
    public static long nullsafeHashcode(VtResultValue v, int collation, Query.Type coerceType) throws SQLException {
        EvalResult cast = new EvalResult();
        cast.setValueCast(v, coerceType);
//        cast.setCollation(collation);
        return cast.nullSafeHashcode();
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

        EvalResult eval(ExpressionEnv env, EvalResult result) throws SQLException;

        /**
         * @param env
         * @return
         */
        Query.Type type(ExpressionEnv env) throws SQLException;

        int getFlags();

        /**
         * @return
         */
        String string();

        boolean constant();

        void output(StringBuilder builder, boolean wrap, Map<String, com.jd.jdbc.srvtopo.BindVariable> bindVariableMap) throws SQLException;
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

        public ExpressionEnv() {
        }

        public EvalResult evaluate(Expr expr) throws SQLException {
            typecheck(expr);
            return expr.eval(this, new EvalResult());
        }

        public void typecheck(Expr expr) {
            //todo
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
        public EvalResult eval(ExpressionEnv env, EvalResult result) throws SQLException {
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
        public int getFlags() {
            return 0;
        }

        @Override
        public String string() {
            return this.key;
        }

        @Override
        public boolean constant() {
            return false;
        }

        @Override
        public void output(final StringBuilder builder, final boolean wrap, final Map<String, com.jd.jdbc.srvtopo.BindVariable> bindVariableMap) {
            builder.append(new String(bindVariableMap.get(key).getValue()));
        }
    }

    public static class NullExpr implements Expr {

        @Override
        public EvalResult evaluate(ExpressionEnv env) throws SQLException {
            return null;
        }

        @Override
        public EvalResult eval(ExpressionEnv env, EvalResult result) throws SQLException {
            return null;
        }

        @Override
        public Query.Type type(ExpressionEnv env) throws SQLException {
            return Query.Type.NULL_TYPE;
        }

        @Override
        public int getFlags() {
            return 0;
        }

        @Override
        public String string() {
            return "null";
        }

        @Override
        public boolean constant() {
            return false;
        }

        @Override
        public void output(StringBuilder builder, boolean wrap, Map<String, com.jd.jdbc.srvtopo.BindVariable> bindVariableMap) throws SQLException {

        }
    }

    public static class Literal implements Expr {
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
        public EvalResult eval(ExpressionEnv env, EvalResult result) throws SQLException {
            return this.val;
        }

        @Override
        public Query.Type type(ExpressionEnv env) {
            return this.val.getType();
        }

        @Override
        public int getFlags() {
            return 0;
        }

        @Override
        public String string() {
            try {
                if (CollectionUtils.isNotEmpty(this.val.getTuple())) {
                    List<String> strings = new ArrayList<>();
                    for (VtValue tupleValue : this.val.tupleValues()) {
                        String string = tupleValue.string();
                        strings.add(string);
                    }
                    return "(" + String.join(", ", strings) + ")";
                }

                Query.Type type = this.val.getType();
                String string = this.val.value().string();
                return string;
            } catch (SQLException e) {
                return "";
            }
        }

        @Override
        public boolean constant() {
            return true;
        }

        @Override
        public void output(final StringBuilder builder, final boolean wrap, final Map<String, com.jd.jdbc.srvtopo.BindVariable> bindVariableMap) throws SQLException {
            builder.append(val.value().toString());
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
        public EvalResult eval(ExpressionEnv env, EvalResult result) throws SQLException {
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
        public int getFlags() {
            return 0;
        }

        @Override
        public String string() {
            return null;
        }

        @Override
        public boolean constant() {
            return this.left.constant() && this.right.constant();
        }

        @Override
        public void output(final StringBuilder builder, final boolean wrap, final Map<String, com.jd.jdbc.srvtopo.BindVariable> bindVariableMap) throws SQLException {
            if (wrap) {
                builder.append('(');
            }

            this.left.output(builder, true, bindVariableMap);
            builder.append(" ");
            builder.append(this.expr.string());
            builder.append(" ");
            this.right.output(builder, true, bindVariableMap);

            if (wrap) {
                builder.append(')');
            }
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

    public static class AnyExpr implements Expr {

        private final SQLExpr expr;

        public AnyExpr(SQLExpr expr) {
            this.expr = expr;
        }

        @Override
        public EvalResult evaluate(ExpressionEnv env) throws SQLException {
            return null;
        }

        @Override
        public EvalResult eval(ExpressionEnv env, EvalResult result) throws SQLException {
            return null;
        }

        @Override
        public Query.Type type(ExpressionEnv env) throws SQLException {
            return null;
        }

        @Override
        public int getFlags() {
            return 0;
        }

        @Override
        public String string() {
            return expr.toString();
        }

        @Override
        public boolean constant() {
            return false;
        }

        @Override
        public void output(StringBuilder builder, boolean wrap, Map<String, com.jd.jdbc.srvtopo.BindVariable> bindVariableMap) throws SQLException {

        }
    }

    @Getter
    @AllArgsConstructor
    public static class Column implements Expr {

        private final int offset;

        @Override
        public EvalResult evaluate(ExpressionEnv env) throws SQLException {
            VtResultValue value = env.getRow().get(this.offset);
            if (value.isNull()) {
                return new EvalResult(Query.Type.NULL_TYPE);
            }
            return evaluateByType(value);
        }

        @Override
        public EvalResult eval(ExpressionEnv env, EvalResult result) throws SQLException {
            VtResultValue value = env.getRow().get(this.offset);
            if (value.isNull()) {
                return new EvalResult(Query.Type.NULL_TYPE);
            }
            return evaluateByType(value);
        }

        @Override
        public Query.Type type(ExpressionEnv env) throws SQLException {
            VtResultValue value = env.getRow().get(this.offset);
            return value.getVtType();
        }

        @Override
        public int getFlags() {
            return 0;
        }

        @Override
        public String string() {
            return null;
        }

        @Override
        public boolean constant() {
            return false;
        }

        @Override
        public void output(StringBuilder builder, boolean wrap, Map<String, com.jd.jdbc.srvtopo.BindVariable> bindVariableMap) throws SQLException {

        }
    }

    @Getter
    @AllArgsConstructor
    public static class TupleExpr implements Expr {

        private final List<Expr> tupleExpr;

        @Override
        public EvalResult evaluate(ExpressionEnv env) throws SQLException {
            return null;
        }

        @Override
        public EvalResult eval(ExpressionEnv env, EvalResult result) throws SQLException {
            List<EvalResult> tup = new ArrayList<>();
            for (Expr expr : tupleExpr) {
                tup.add(EvalResult.init(env, expr));
            }
            result.setTuple(tup);
            return result;
        }

        @Override
        public Query.Type type(ExpressionEnv env) throws SQLException {
            return Query.Type.TUPLE;
        }

        @Override
        public int getFlags() {
            return 0;
        }

        @Override
        public String string() {
            List<String> strings = new ArrayList<>();
            for (Expr expr : tupleExpr) {
                strings.add(expr.string());
            }
            return "(" + String.join(", ", strings) + ")";
        }

        @Override
        public boolean constant() {
            for (Expr subexpr : tupleExpr) {
                if (!subexpr.constant()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void output(StringBuilder builder, boolean wrap, Map<String, com.jd.jdbc.srvtopo.BindVariable> bindVariableMap) throws SQLException {

        }
    }
}
