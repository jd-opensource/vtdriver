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

import com.jd.jdbc.sqltypes.VtNumberRange;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtType;
import com.jd.jdbc.sqltypes.VtValue;
import io.vitess.proto.Query;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class EvalResult {

    // flagNull marks that this value is null; implies flagNullable
    private final int flagNull = 1 << 0;

    // flagNullable marks that this value CAN be null
    private final int flagNullable = 1 << 1;

    // flagIntegerUdf marks that this value is math.MinInt64, and will underflow if negated
    private final int flagIntegerUdf = 1 << 5;

    // flagIntegerCap marks that this value is (-math.MinInt64),
    // and should be promoted to flagIntegerUdf if negated
    private final int flagIntegerCap = 1 << 6;

    // flagIntegerOvf marks that this value will overflow if negated
    private final int flagIntegerOvf = 1 << 7;

    // flagHex marks that this value originated from a hex literal
    private final int flagHex = 1 << 8;

    // flagBit marks that this value originated from a bit literal
    private final int flagBit = 1 << 9;

    // flagExplicitCollation marks that this value has an explicit collation
    private final int flagExplicitCollation = 1 << 10;

    private final int flagIntegerRange = flagIntegerOvf | flagIntegerCap | flagIntegerUdf;

    /**
     * // expr is the expression that will be eventually evaluated to fill the other fields.
     * // If expr is set, it means that this EvalResult has not been evaluated yet, and the
     * // remaining fields are not valid. Once the evaluation engine calls EvalResult.resolve(),
     * // the other fields will be set based on the evaluation result of expr and expr will be
     * // set to nil, to mark this result as fully resolved.
     */
    private EvalEngine.Expr expr;

    /**
     * // env is the ExpressionEnv in which the expr is being evaluated
     */
    private EvalEngine.ExpressionEnv env;

    /**
     * // type_ is the SQL type of this result.
     * // Must not be accessed directly: call EvalResult.typeof() instead.
     * // For most expression types, this is known ahead of time and calling typeof() does not require
     * // an evaluation, so the type of an expression can be known without evaluating it.
     */
    protected Query.Type type = Query.Type.NULL_TYPE;

    private int flags;

    protected Long ival;

    protected BigInteger uval;

    private Double fval;

    private BigDecimal bigDecimal;

    private byte[] bytes;

    /**
     * tuple_ is the list of all results contained in this result, if the result is a tuple.
     * It may be uninitialized.
     * Must not be accessed directly: call EvalResult.tuple() instead.
     */
    //nolint
    private List<EvalResult> tuple;

    private final boolean typecheckEval = false;

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

    public EvalResult(BigDecimal bigDecimal, Query.Type type) {
        this.bigDecimal = bigDecimal;
        this.type = type;
    }

    public EvalResult(EvalEngine.Expr expr, Query.Type type) {
        this.expr = expr;
        this.type = type;
    }

    public EvalResult(Query.Type type) {
        this.type = type;
    }

    /**
     * init initializes this EvalResult with the given expr. The actual value of this result will be
     * calculated lazily when required, and will be the output of evaluating the expr.
     *
     * @param env
     * @param expr
     * @return
     */
    public static EvalResult init(EvalEngine.ExpressionEnv env, EvalEngine.Expr expr) throws SQLException {
        EvalResult er = new EvalResult();
        er.expr = expr;
        er.type = expr.type(env);
        er.flags = expr.getFlags();
        return er;
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
            case DECIMAL:
                return "Type: " + this.type + ", Value: " + this.bigDecimal.toString();
            case EXPRESSION:
                return "Type: " + this.type + ", Value: " + this.expr.toString();
            default:
                return "Type: " + this.type + ", Value: " + Arrays.toString(this.bytes);
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

    /**
     * TupleValues allows for retrieval of the value we expose for public consumption
     *
     * @return
     * @throws SQLException
     */
    public List<VtValue> tupleValues() throws SQLException {
        if (expr != null) {
            throw new SQLException("did not resolve EvalResult after evaluation");
        }
        if (tuple == null || tuple.size() == 0) {
            return null;
        }
        List<VtValue> res = new ArrayList<>();
        for (EvalResult vt : tuple) {
            res.add(vt.resolveValue().value());
        }
        return res;
    }

    public void setTuple(List<EvalResult> tuple) {
        this.tuple = tuple;
        this.type = Query.Type.INT16;
    }

    private void setNull() {
        this.flags |= flagNullable | flagNull;
    }

    public void setInt64(Long i) {
        this.type = Query.Type.INT64;
        this.uval = BigInteger.valueOf(i);
        if (i == VtNumberRange.INT64_MIN) {
            flags |= flagIntegerUdf;
        }
    }

    public void setUint64(BigInteger u) {
        this.type = Query.Type.UINT64;
        this.uval = u;
        BigInteger bigInteger = BigInteger.valueOf(VtNumberRange.INT64_MAX + 1);
        if (Objects.equals(u, bigInteger)) {
            flags |= flagIntegerCap;
        }
        if (u.compareTo(bigInteger) > 0) {
            flags |= flagIntegerOvf;
        }
    }

    public void setFloat(double f) {
        this.type = Query.Type.FLOAT64;
        this.uval = BigInteger.valueOf(Double.doubleToLongBits(f));
    }

    public void setRaw(Query.Type type, byte[] raw, Object coll) {
        this.type = type;
        this.bytes = raw;
    }

    /**
     * resolve computes the final value of this EvalResult by evaluating the expr embedded in it.
     * This function should not be called directly: it will be called by the evaluation engine
     * lazily when it needs to know the value of this result and not earlier.
     */
    private void resolve() throws SQLException {
        if (this.expr != null) {
            if (typecheckEval) {
                Query.Type before = this.type;
                expr.eval(env, this);
                if (before != this.type) {
                    throw new SQLException("did not pre-compute the right type: " + before + " before evaluation, " + this.type + " after");
                }
            } else {
                this.expr.eval(this.env, this);
            }
            this.expr = null;
        }
    }

    private EvalResult resolveValue() throws SQLException {
        EvalResult eval = null;
        if (this.expr != null) {
            if (typecheckEval) {
                Query.Type before = this.type;
                expr.eval(env, this);
                if (before != this.type) {
                    throw new SQLException("did not pre-compute the right type: " + before + " before evaluation, " + this.type + " after");
                }
            } else {
                eval = this.expr.eval(this.env, this);
            }
            eval.expr = null;
        }
        return eval;
    }

    public void setValueCast(VtResultValue v, Query.Type coerceType) throws SQLException {
        if (coerceType == Query.Type.NULL_TYPE) {
            this.setNull();
        } else if (VtType.isFloat(coerceType)) {
            if (VtType.isSigned(v.getVtType())) {
                VtValue vtValue = VtValue.newVtValue(v.getVtType(), v.toBytes());
                BigInteger ival = EvalEngine.toUint64(vtValue);
                this.setFloat(ival.floatValue());
            } else if (VtType.isUnsigned(v.getVtType())) {
                VtValue vtValue = VtValue.newVtValue(v.getVtType(), v.toBytes());
                BigInteger ival = EvalEngine.toUint64(vtValue);
                this.setFloat(ival.floatValue());
            } else if (VtType.isFloat(v.getVtType()) || Objects.equals(v.getVtType(), Query.Type.DECIMAL)) {
                VtValue vtValue = VtValue.newVtValue(v.getVtType(), v.toBytes());
                float v1 = vtValue.toFloat();
                this.setFloat(v1);
            } else if (VtType.isText(v.getVtType()) || VtType.isBinary(v.getVtType())) {

            } else {
                throw new SQLException("coercion should not try to coerce this value to a float: " + v);
            }
        } else if (coerceType == Query.Type.DECIMAL) {

        } else if (VtType.isSigned(coerceType)) {
            Query.Type vtType = v.getVtType();
            if (VtType.isSigned(vtType)) {
                VtValue vtValue = VtValue.newVtValue(vtType, v.toBytes());
                BigInteger ival = EvalEngine.toUint64(vtValue);
                this.setInt64(ival.longValue());
            } else if (VtType.isUnsigned(vtType)) {
                VtValue vtValue = VtValue.newVtValue(vtType, v.toBytes());
                BigInteger ival = EvalEngine.toUint64(vtValue);
                this.setInt64(ival.longValue());
            } else {
                throw new SQLException("coercion should not try to coerce this value to a signed int: " + v);
            }
        } else if (VtType.isUnsigned(coerceType)) {
            Query.Type vtType = v.getVtType();
            if (VtType.isSigned(vtType)) {
                VtValue vtValue = VtValue.newVtValue(vtType, v.toBytes());
                BigInteger ival = EvalEngine.toUint64(vtValue);
                this.setUint64(ival);
            } else if (VtType.isUnsigned(vtType)) {
                VtValue vtValue = VtValue.newVtValue(vtType, v.toBytes());
                BigInteger ival = EvalEngine.toUint64(vtValue);
                this.setUint64(ival);
            } else {
                throw new SQLException("coercion should not try to coerce this value to a unsigned int: " + v);
            }
        } else if (VtType.isText(coerceType) || VtType.isBinary(coerceType)) {
            if (VtType.isText(v.getVtType()) || VtType.isBinary(v.getVtType())) {
                VtValue vtValue = VtValue.newVtValue(v.getVtType(), v.toBytes());
                this.setRaw(v.getVtType(), vtValue.raw(), null);
            } else {
                throw new SQLException("coercion should not try to coerce this value to a text: " + v);
            }
        } else {
            throw new SQLException("coercion should not try to coerce this value: " + v);
        }
    }

    public long nullSafeHashcode() throws SQLException {
        this.resolve();

        if (this.isNull()) {
            return Long.MAX_VALUE;
        } else if (isNumeric()) {
            return uint64().longValue();
        } else if (isTextual()) {
            throw new SQLException("text type with an unknown/unsupported collation cannot be hashed");
        } else if (VtType.isDate(type)) {

        } else {
            throw new SQLException("types does not support hashcode yet: " + type);
        }
        return -1;
    }

    public boolean hasFlag(int f) {
        return (flags & f) != 0;
    }

    public boolean isNull() throws SQLException {
        if (!hasFlag(flagNullable)) {
            return false;
        }
        if (hasFlag(flagNull)) {
            return true;
        }
        resolve();
        return hasFlag(flagNull);
    }

    public boolean isNumeric() {
        return VtType.isNumber(type);
    }

    public boolean isTextual() {
        return VtType.isText(type) || VtType.isBinary(type);
    }

    public BigInteger uint64() throws SQLException {
        resolve();
        return uval;
    }

}
