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

import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtType;
import com.jd.jdbc.sqltypes.VtValue;
import io.vitess.proto.Query;
import java.math.BigInteger;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.math.BigDecimal;

import static io.vitess.proto.Query.Type.DECIMAL;
import static io.vitess.proto.Query.Type.FLOAT64;
import static io.vitess.proto.Query.Type.INT64;
import static io.vitess.proto.Query.Type.UINT64;

public class Arithmetic {

    /**
     * @param v
     * @return
     * @throws SQLException
     */
    static EvalEngine.EvalResult newIntegralNumeric(VtValue v) throws SQLException {
        String str = v.toString();
        if (v.isSigned()) {
            long ival = Long.parseLong(str, 10);
            return new EvalEngine.EvalResult(ival, Query.Type.INT64);
        } else if (v.isUnsigned()) {
            long uval = Long.parseUnsignedLong(str, 10);
            return new EvalEngine.EvalResult(uval, Query.Type.UINT64);
        }

        // For other types, do best effort.
        try {
            long ival = Long.parseLong(str, 10);
            return new EvalEngine.EvalResult(ival, Query.Type.INT64);
        } catch (NumberFormatException e) {
            try {
                long uval = Long.parseUnsignedLong(str, 10);
                return new EvalEngine.EvalResult(uval, Query.Type.UINT64);
            } catch (NumberFormatException ee) {
                throw new SQLException("could not parse value: '" + str + "'");
            }
        }
    }

    /**
     * @param value1
     * @param value2
     * @return
     */
    static Integer compareNumeric(EvalEngine.EvalResult value1, EvalEngine.EvalResult value2) {
        // Equalize the types.

        switch (value1.getType()) {
            case INT64:
                switch (value2.getType()) {
                    case UINT64:
                        if (value1.getIval() < 0) {
                            return -1;
                        }
                        value1 = new EvalEngine.EvalResult(BigInteger.valueOf(value1.getIval()), Query.Type.UINT64);
                        break;
                    case FLOAT64:
                        value1 = new EvalEngine.EvalResult(value1.getIval().doubleValue(), Query.Type.FLOAT64);
                        break;
                }
                break;
            case UINT64:
                switch (value2.getType()) {
                    case INT64:
                        if (value2.getIval() < 0) {
                            return 1;
                        }
                        value2 = new EvalEngine.EvalResult(BigInteger.valueOf(value2.getIval()), Query.Type.UINT64);
                        break;
                    case FLOAT64:
                        value1 = new EvalEngine.EvalResult(value1.getUval().doubleValue(), Query.Type.FLOAT64);
                        break;
                }
                break;
            case FLOAT64:
                switch (value2.getType()) {
                    case INT64:
                        value2 = new EvalEngine.EvalResult(value2.getIval().doubleValue(), FLOAT64);
                        break;
                    case UINT64:
                        value2 = new EvalEngine.EvalResult(value2.getUval().doubleValue(), FLOAT64);
                        break;
                }
                break;
        }

        // Both values are of the same type.

        switch (value1.getType()) {
            case INT64:
                return value1.getIval().compareTo(value2.getIval());
            case UINT64:
                return value1.getUval().compareTo(value2.getUval());
            case FLOAT64:
                return value1.getFval().compareTo(value2.getFval());
        }
        return 1;
    }

    /**
     * @param value1
     * @param value2
     * @return
     */
    static EvalEngine.EvalResult addNumeric(EvalEngine.EvalResult value1, EvalEngine.EvalResult value2) throws SQLException {
        List<EvalEngine.EvalResult> resultValues = makeNumericAndPrioritize(value1, value2);
        value1 = resultValues.get(0);
        value2 = resultValues.get(1);
        switch (value1.getType()) {
            case INT64:
                return intPlusInt(value1.getIval(), value2.getIval());
            case UINT64:
                switch (value2.getType()) {
                    case INT64:
                        return uintPlusInt(value1.getUval(), value2.getIval());
                    case UINT64:
                        return uintPlusUint(value1.getUval(), value2.getUval());
                }
                break;
            case FLOAT64:
                return floatPlusAny(value1.getFval(), value2);
        }
        throw new SQLException("unreachable");
    }

    static EvalEngine.EvalResult addNumericWithError(EvalEngine.EvalResult value1, EvalEngine.EvalResult value2) throws SQLException {
        List<EvalEngine.EvalResult> resultValues = makeNumericAndPrioritize(value1, value2);
        value1 = resultValues.get(0);
        value2 = resultValues.get(1);
        switch (value1.getType()) {
            case INT64:
                return intPlusIntWithError(value1.getIval(), value2.getIval());
            case UINT64:
                switch (value2.getType()) {
                    case INT64:
                        return uintPlusIntWithError(value1.getUval(), value2.getIval());
                    case UINT64:
                        return uintPlusUintWithError(value1.getUval(), value2.getUval());
                }
                break;
            case FLOAT64:
                return floatPlusAny(value1.getFval(), value2);
            case DECIMAL:
                return decimalPlusAny(value1.getBigDecimal(), value2);
        }
        throw new SQLException("invalid arithmetic between:" + value1.value().toString() + " " + value2.value().toString());
    }

    static EvalEngine.EvalResult subtractNumericWithError(EvalEngine.EvalResult i1, EvalEngine.EvalResult i2) throws SQLException {
        EvalEngine.EvalResult v1 = makeNumeric(i1);
        EvalEngine.EvalResult v2 = makeNumeric(i2);

        switch (v1.getType()) {
            case INT64:
                switch (v2.getType()) {
                    case INT64:
                        return intMinusIntWithError(i1.getIval(), i2.getIval());
                    case UINT64:
                        return intMinusUintWithError(i1.getIval(), i2.getUval());
                    case FLOAT64:
                        return anyMinusFloat(v1, v2.getFval());
                }
                break;
            case UINT64:
                switch (v2.getType()) {
                    case INT64:
                        return uintMinusIntWithError(v1.getUval(), v2.getIval());
                    case UINT64:
                        return uintMinusUintWithError(v1.getUval(), v2.getUval());
                    case FLOAT64:
                        return anyMinusFloat(v1, v2.getFval());
                }
                break;
            case FLOAT64:
                return floatMinusAny(v1.getFval(), v2);
        }
        throw new SQLException("invalid arithmetic between:" + v1.value().toString() + " " + v2.value().toString());
    }

    static EvalEngine.EvalResult multiplyNumericWithError(EvalEngine.EvalResult value1, EvalEngine.EvalResult value2) throws SQLException {
        List<EvalEngine.EvalResult> resultValues = makeNumericAndPrioritize(value1, value2);
        value1 = resultValues.get(0);
        value2 = resultValues.get(1);
        switch (value1.getType()) {
            case INT64:
                return intTimesIntWithError(value1.getIval(), value2.getIval());
            case UINT64:
                switch (value2.getType()) {
                    case INT64:
                        return uintTimesIntWithError(value1.getUval(), value2.getIval());
                    case UINT64:
                        return uintTimesUintWithError(value1.getUval(), value2.getUval());
                }
                break;
            case FLOAT64:
                return floatTimesAny(value1.getFval(), value2);
        }
        throw new SQLException("invalid arithmetic between:" + value1.value().toString() + " " + value2.value().toString());
    }

    static EvalEngine.EvalResult divideNumericWithError(EvalEngine.EvalResult value1, EvalEngine.EvalResult value2) throws SQLException {
        List<EvalEngine.EvalResult> resultValues = makeNumericAndPrioritize(value1, value2);
        value1 = resultValues.get(0);
        value2 = resultValues.get(1);
        switch (value1.getType()) {
            case INT64:
                return floatDivideAnyWithError(Double.valueOf(value1.getIval().toString()), value2);
            case UINT64:
                return floatDivideAnyWithError(Double.valueOf(value1.getUval().toString()), value2);
            case FLOAT64:
                return floatDivideAnyWithError(value1.getFval(), value2);
        }
        throw new SQLException("invalid arithmetic between: " + value1.value().toString() + " " + value2.value().toString());
    }

    static EvalEngine.EvalResult intPlusIntWithError(Long v1, Long v2) throws SQLException {
        Long result = v1 + v2;
        if ((result > v1) != (v2 > 0)) {
            throw new SQLException("BIGINT value is out of range in " + v1 + " + " + v2);
        }
        return new EvalEngine.EvalResult(result, INT64);
    }

    static EvalEngine.EvalResult intMinusIntWithError(Long v1, Long v2) throws SQLException {
        Long result = v1 - v2;
        if ((result < v1) != (v2 > 0)) {
            throw new SQLException("BIGINT value is out of range in " + v1 + " - " + v2);
        }
        return new EvalEngine.EvalResult(result, INT64);
    }

    static EvalEngine.EvalResult intTimesIntWithError(Long v1, Long v2) throws SQLException {
        Long result = v1 * v2;
        if (v1 != 0 && result / v1 != v2) {
            throw new SQLException("BIGINT value is out of range in " + v1 + " * " + v2);
        }
        return new EvalEngine.EvalResult(result, INT64);
    }

    static EvalEngine.EvalResult intMinusUintWithError(Long v1, BigInteger v2) throws SQLException {
        if (v1 < 0 && v1 < v2.longValue()) {
            throw new SQLException("BIGINT value is out of range in " + v1 + " * " + v2);
        }
        return uintMinusUintWithError(BigInteger.valueOf(v1), v2);
    }

    static EvalEngine.EvalResult uintMinusIntWithError(BigInteger v1, Long v2) throws SQLException {
        if (v1.longValue() < v2 && v2 > 0) {
            throw new SQLException("BIGINT UNSIGNED value is out of range in " + v1 + " - " + v2);
        }
        if (v2 < 0) {
            return uintPlusIntWithError(v1, -v2);
        }
        return uintMinusUintWithError(v1, BigInteger.valueOf(v2));
    }

    static EvalEngine.EvalResult uintPlusIntWithError(BigInteger v1, Long v2) throws SQLException {
        if (v2 < 0 && v1.compareTo(BigInteger.valueOf(v2)) == -1) {
            throw new SQLException("BIGINT value is out of range in " + v1 + " + " + v2);
        }
        return uintPlusUintWithError(v1, BigInteger.valueOf(v2));
    }

    static EvalEngine.EvalResult uintMinusUintWithError(BigInteger v1, BigInteger v2) throws SQLException {
        BigInteger result = v1.subtract(v2);
        if (v2.compareTo(v1) == 1) {
            throw new SQLException("BIGINT UNSIGNED value is out of range in " + v1 + " - " + v2);
        }
        return new EvalEngine.EvalResult(result, UINT64);
    }

    static EvalEngine.EvalResult uintPlusUintWithError(BigInteger v1, BigInteger v2) throws SQLException {
        BigInteger result = v1.add(v2);
        if (result.compareTo(v2) == -1) {
            throw new SQLException("BIGINT value is out of range in " + v1 + " + " + v2);
        }
        return new EvalEngine.EvalResult(result, UINT64);
    }

    static EvalEngine.EvalResult uintTimesIntWithError(BigInteger v1, Long v2) throws SQLException {
        if (v2 < 0 || v1.longValue() < 0) {
            throw new SQLException("BIGINT value is out of range in " + v1 + " * " + v2);
        }
        return uintTimesUintWithError(v1, BigInteger.valueOf(v2));
    }

    static EvalEngine.EvalResult uintTimesUintWithError(BigInteger v1, BigInteger v2) throws SQLException {
        BigInteger result = v1.multiply(v2);
        if (result.compareTo(v2) == -1 || result.compareTo(v1) == -1) {
            throw new SQLException("BIGINT value is out of range in " + v1 + " * " + v2);
        }
        return new EvalEngine.EvalResult(result, UINT64);
    }

    static EvalEngine.EvalResult anyMinusFloat(EvalEngine.EvalResult v1, Double v2) {
        switch (v1.getType()) {
            case INT64:
                v1.setFval(Double.parseDouble(v1.getIval().toString()));
                break;
            case UINT64:
                v1.setFval(Double.parseDouble(v1.getUval().toString()));
                break;
        }
        return new EvalEngine.EvalResult(v1.getFval() - v2, FLOAT64);
    }

    static EvalEngine.EvalResult floatMinusAny(Double v1, EvalEngine.EvalResult v2) {
        switch (v2.getType()) {
            case INT64:
                v2.setFval(Double.valueOf(v2.getIval().toString()));
                break;
            case UINT64:
                v2.setFval(Double.valueOf(v2.getUval().toString()));
                break;
        }
        return new EvalEngine.EvalResult(v1 - v2.getFval(), FLOAT64);
    }

    static EvalEngine.EvalResult floatTimesAny(Double v1, EvalEngine.EvalResult v2) {
        switch (v2.getType()) {
            case INT64:
                v2.setFval(Double.valueOf(v2.getIval().toString()));
                break;
            case UINT64:
                v2.setFval(Double.valueOf(v2.getUval().toString()));
                break;
        }
        return new EvalEngine.EvalResult(v1 * v2.getFval(), FLOAT64);
    }

    static EvalEngine.EvalResult floatDivideAnyWithError(Double value1, EvalEngine.EvalResult value2) throws SQLException {
        switch (value2.getType()) {
            case INT64:
                value2.setFval(Double.valueOf(value2.getIval().toString()));
                break;
            case UINT64:
                value2.setFval(Double.valueOf(value2.getUval().toString()));
                break;
        }
        Double result = value1 / value2.getFval();
        boolean devisionLessTHanOne = value2.getFval() < 1;
        boolean resultMismatch = value2.getFval() * result != value1;

        if (devisionLessTHanOne || resultMismatch) {
            throw new SQLException("BIGINT is out of range in " + value1 + " " + value2.getFval());
        }

        return new EvalEngine.EvalResult(value1 / value2.getFval(), FLOAT64);
    }

    /**
     * makeNumericAndprioritize reorders the input parameters
     * to be Float64, Uint64, Int64.
     *
     * @param inputValue1
     * @param inputValue2
     * @return
     */
    private static List<EvalEngine.EvalResult> makeNumericAndPrioritize(EvalEngine.EvalResult inputValue1, EvalEngine.EvalResult inputValue2) {
        EvalEngine.EvalResult value1 = makeNumeric(inputValue1);
        EvalEngine.EvalResult value2 = makeNumeric(inputValue2);
        List<EvalEngine.EvalResult> resultValues = new ArrayList<>();
        switch (value1.getType()) {
            case INT64:
                if (value2.getType() == UINT64 || value2.getType() == FLOAT64 || value2.getType() == DECIMAL) {
                    resultValues.add(value2);
                    resultValues.add(value1);
                    return resultValues;
                }
                break;
            case UINT64:
                if (value2.getType() == FLOAT64 || value2.getType() == DECIMAL) {
                    resultValues.add(value2);
                    resultValues.add(value1);
                    return resultValues;
                }
                break;
            case DECIMAL:
                break;
        }
        resultValues.add(value1);
        resultValues.add(value2);
        return resultValues;
    }

    /**
     * @param value
     * @return
     */
    private static EvalEngine.EvalResult makeNumeric(EvalEngine.EvalResult value) {
        if (VtType.isNumber(value.getType())) {
            return value;
        }

        try {
            Long ival = Long.parseLong(new String(value.getBytes()));
            return new EvalEngine.EvalResult(ival, INT64);
        } catch (Exception e) {
        }

        try {
            Double fval = Double.parseDouble(new String(value.getBytes()));
            return new EvalEngine.EvalResult(fval, FLOAT64);
        } catch (Exception e) {
        }

        return new EvalEngine.EvalResult(new Long(0), Query.Type.INT64);
    }

    /**
     * isByteComparable returns true if the type is binary or date/time.
     *
     * @param value
     * @return
     */
    static Boolean isByteComparable(VtValue value) {
        if (value.isBinary() || value.isText()) {
            return true;
        }
        switch (value.getVtType()) {
            case TIMESTAMP:
            case DATE:
            case TIME:
            case DATETIME:
                return true;
            default:
                return false;
        }
    }

    /**
     * @param value1
     * @param value2
     * @return
     */
    private static EvalEngine.EvalResult intPlusInt(Long value1, Long value2) {
        long result = value1 + value2;
        if (value1 > 0 && value2 > 0 && result < 0) {
            Double dv1 = value1.doubleValue();
            Double dv2 = value2.doubleValue();
            return new EvalEngine.EvalResult(dv1 + dv2, FLOAT64);
        }
        if (value1 < 0 && value2 < 0 && result > 0) {
            Double dv1 = value1.doubleValue();
            Double dv2 = value2.doubleValue();
            return new EvalEngine.EvalResult(dv1 + dv2, FLOAT64);
        }

        return new EvalEngine.EvalResult(result, INT64);
    }

    /**
     * @param value1
     * @param value2
     * @return
     */
    private static EvalEngine.EvalResult uintPlusInt(BigInteger value1, Long value2) {
        return uintPlusUint(value1, new BigInteger(String.valueOf(value2)));
    }

    /**
     * @param value1
     * @param value2
     * @return
     */
    private static EvalEngine.EvalResult uintPlusUint(BigInteger value1, BigInteger value2) {
        BigInteger result = value1.add(value2);
        //if result < v2
        if (result.compareTo(value2) == -1) {
            Double v1 = value1.doubleValue();
            Double v2 = value2.doubleValue();
            return new EvalEngine.EvalResult(v1 + v2, FLOAT64);
        }
        return new EvalEngine.EvalResult(result, UINT64);
    }

    /**
     * @param value1
     * @param value2
     * @return
     */
    private static EvalEngine.EvalResult floatPlusAny(Double value1, EvalEngine.EvalResult value2) {
        switch (value2.getType()) {
            case INT64:
                value2.setFval(value2.getIval().doubleValue());
                break;
            case UINT64:
                value2.setFval(value2.getUval().doubleValue());
                break;
        }
        return new EvalEngine.EvalResult(value1 + value2.getFval(), FLOAT64);
    }

    private static EvalEngine.EvalResult decimalPlusAny(BigDecimal value1, EvalEngine.EvalResult value2) {
        switch (value2.getType()) {
            case INT64:
                value2.setBigDecimal(BigDecimal.valueOf(value2.getIval()));
                break;
            case UINT64:
                value2.setBigDecimal(new BigDecimal(value2.getUval().toString()));
                break;
        }
        return new EvalEngine.EvalResult(value1.add(value2.getBigDecimal()), DECIMAL);
    }

    /**
     * @param value
     * @param resultType
     * @return
     */
    public static VtValue castFromNumeric(EvalEngine.EvalResult value, Query.Type resultType) throws SQLException {
        if (VtType.isSigned(resultType)) {
            switch (value.getType()) {
                case INT64:
                    return VtValue.newVtValue(resultType, value.getIval().toString().getBytes());
                case UINT64:
                    return VtValue.newVtValue(resultType, Long.valueOf(value.getUval().longValue()).toString().getBytes());
                case FLOAT64:
                    return VtValue.newVtValue(resultType, Long.valueOf(value.getFval().longValue()).toString().getBytes());
//                case INT32:
//                    return VtValue.newVtValue(resultType, value.getIval().toString().getBytes());
            }
        } else if (VtType.isUnsigned(resultType)) {
            switch (value.getType()) {
                case UINT64:
                    return VtValue.newVtValue(resultType, value.getUval().toString().getBytes());
                case INT64:
                    return VtValue.newVtValue(resultType, BigInteger.valueOf(value.getIval()).toString().getBytes());
                case FLOAT64:
                    return VtValue.newVtValue(resultType, BigInteger.valueOf(value.getFval().longValue()).toString().getBytes());
            }
        } else if (VtType.isFloat(resultType) || resultType == DECIMAL) {
            switch (value.getType()) {
                case INT64:
                    return VtValue.newVtValue(resultType, value.getIval().toString().getBytes());
                case UINT64:
                    return VtValue.newVtValue(resultType, value.getUval().toString().getBytes());
                case FLOAT64:
//                    byte format = (byte)'g';
//                    if (resultType == DECIMAL) {
//                        format = (byte)'f';
//                    }
                    DecimalFormat decimalFormat = new DecimalFormat("0.################");
                    decimalFormat.setDecimalSeparatorAlwaysShown(false);
                    return VtValue.newVtValue(resultType, decimalFormat.format(value.getFval()).getBytes());
            }
        } else {
            return VtValue.newVtValue(resultType, value.getBytes());
        }
        // NULL represents the NULL value.
        return VtValue.NULL;
    }

    public static VtResultValue castFromNum(EvalEngine.EvalResult value, Query.Type resultType) throws SQLException {
        if (VtType.isSigned(resultType)) {
            switch (value.getType()) {
                case INT64:
                    return VtResultValue.newVtResultValue(resultType, value.getIval());
                case UINT64:
                    return VtResultValue.newVtResultValue(resultType, Long.valueOf(value.getUval().longValue()));
                case FLOAT64:
                    return VtResultValue.newVtResultValue(resultType, Long.valueOf(value.getFval().longValue()));
//                case INT32:
//                    return VtValue.newVtValue(resultType, value.getIval().toString().getBytes());
            }
        } else if (VtType.isUnsigned(resultType)) {
            switch (value.getType()) {
                case UINT64:
                    return VtResultValue.newVtResultValue(resultType, value.getUval());
                case INT64:
                    return VtResultValue.newVtResultValue(resultType, BigInteger.valueOf(value.getIval()));
                case FLOAT64:
                    return VtResultValue.newVtResultValue(resultType, BigInteger.valueOf(value.getFval().longValue()));
            }
        } else if (VtType.isFloat(resultType) || resultType == DECIMAL) {
            switch (value.getType()) {
                case INT64:
                    return VtResultValue.newVtResultValue(resultType, value.getIval());
                case UINT64:
                    return VtResultValue.newVtResultValue(resultType, value.getUval());
                case FLOAT64:
                    return VtResultValue.newVtResultValue(resultType, value.getFval());
                case DECIMAL:
                    return VtResultValue.newVtResultValue(resultType, value.getBigDecimal());
            }
        } else {
            return VtResultValue.newVtResultValue(resultType, value);
        }
        // NULL represents the NULL value.
        return VtResultValue.NULL;
    }


    public static VtResultValue minmax(VtResultValue value1, VtResultValue value2, boolean min) throws SQLException {
        if (value1.isNull()) {
            return value2;
        }
        if (value2.isNull()) {
            return value1;
        }
        int n = EvalEngine.nullSafeCompare(value1, value2);

        boolean value1IsSmaller = n < 0;
        if (min == value1IsSmaller) {
            return value1;
        }
        return value2;
    }
}
