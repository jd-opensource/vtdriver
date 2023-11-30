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

package com.jd.jdbc.sqltypes;

import com.jd.jdbc.vitess.resultset.ResultSetUtil;
import io.vitess.proto.Query;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class VtResultValue {
    public static VtResultValue NULL = new VtResultValue();

    private Object value;

    private Query.Type vtType;

    private VtResultValue() {
        vtType = Query.Type.NULL_TYPE;
        value = null;
    }

    public VtResultValue(Object value, Query.Type vtType) {
        this.value = value;
        this.vtType = vtType;
    }

    public static VtResultValue newVtResultValue(Query.Type typ, Object value) throws SQLException {
        VtResultValue vtResultValue = new VtResultValue();
        vtResultValue.vtType = typ;
        if (value == null) {
            return VtResultValue.NULL;
        }
        switch (typ.getNumber()) {
            case Query.Type.NULL_TYPE_VALUE: {
                return VtResultValue.NULL;
            }
            case Query.Type.INT8_VALUE: {
                int x = (int) ResultSetUtil.convertValue(value, int.class);
                if (x < VtNumberRange.INT8_MIN || x > VtNumberRange.INT8_MAX) {
                    throw new SQLException("wrong data type " + typ + " for " + value);
                }
                vtResultValue.value = x;
                return vtResultValue;
            }
            case Query.Type.UINT8_VALUE: {
                int x = (int) ResultSetUtil.convertValue(value, int.class);
                if (x < VtNumberRange.UINT8_MIN || x > VtNumberRange.UINT8_MAX) {
                    throw new SQLException("wrong data type " + typ + " for " + value);
                }
                vtResultValue.value = x;
                return vtResultValue;
            }
            case Query.Type.INT16_VALUE: {
                int x = (int) ResultSetUtil.convertValue(value, int.class);
                if (x < VtNumberRange.INT16_MIN || x > VtNumberRange.INT16_MAX) {
                    throw new SQLException("wrong data type " + typ + " for " + value);
                }
                vtResultValue.value = x;
                return vtResultValue;
            }
            case Query.Type.UINT16_VALUE: {
                int x = (int) ResultSetUtil.convertValue(value, int.class);
                if (x < VtNumberRange.UINT16_MIN || x > VtNumberRange.UINT16_MAX) {
                    throw new SQLException("wrong data type " + typ + " for " + value);
                }
                vtResultValue.value = x;
                return vtResultValue;
            }
            case Query.Type.INT24_VALUE: {
                int x = (int) ResultSetUtil.convertValue(value, int.class);
                if (x < VtNumberRange.INT24_MIN || x > VtNumberRange.INT24_MAX) {
                    throw new SQLException("wrong data type " + typ + " for " + value);
                }
                vtResultValue.value = x;
                return vtResultValue;
            }
            case Query.Type.UINT24_VALUE: {
                int x = (int) ResultSetUtil.convertValue(value, int.class);
                if (x < VtNumberRange.UINT24_MIN || x > VtNumberRange.UINT24_MAX) {
                    throw new SQLException("wrong data type " + typ + " for " + value);
                }
                vtResultValue.value = x;
                return vtResultValue;
            }
            case Query.Type.INT32_VALUE: {
                vtResultValue.value = ResultSetUtil.convertValue(value, Integer.class);
                return vtResultValue;
            }
            case Query.Type.UINT32_VALUE: {
                long x = (long) ResultSetUtil.convertValue(value, long.class);
                if (x < VtNumberRange.UINT32_MIN || x > VtNumberRange.UINT32_MAX) {
                    throw new SQLException("wrong data type " + typ + " for " + value);
                }
                vtResultValue.value = x;
                return vtResultValue;
            }
            case Query.Type.INT64_VALUE: {
                vtResultValue.value = ResultSetUtil.convertValue(value, Long.class);
                return vtResultValue;
            }
            case Query.Type.UINT64_VALUE: {
                BigInteger x = (BigInteger) ResultSetUtil.convertValue(value, BigInteger.class);
                if (x.compareTo(new BigInteger(String.valueOf(VtNumberRange.UINT64_MIN))) < 0 ||
                    x.compareTo(new BigInteger(String.valueOf(VtNumberRange.UINT64_MAX))) > 0) {
                    throw new SQLException("wrong data type " + typ + " for " + value);
                }
                vtResultValue.value = x;
                return vtResultValue;
            }
            case Query.Type.FLOAT32_VALUE: {
                vtResultValue.value = ResultSetUtil.convertValue(value, Float.class);
                return vtResultValue;
            }
            case Query.Type.FLOAT64_VALUE: {
                vtResultValue.value = ResultSetUtil.convertValue(value, Double.class);
                return vtResultValue;
            }
            case Query.Type.DECIMAL_VALUE: {
                vtResultValue.value = ResultSetUtil.convertValue(value, BigDecimal.class);
                return vtResultValue;
            }
            case Query.Type.VARCHAR_VALUE: {
                vtResultValue.value = ResultSetUtil.convertValue(value, String.class);
                return vtResultValue;
            }
            default: {
                vtResultValue.value = value;
                return vtResultValue;
            }
        }
    }

    private static byte[] toBytes(Object value) {
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        String s;
        if (value instanceof String) {
            s = (String) value;
        } else {
            s = String.valueOf(value);
        }
        return s.getBytes();
    }

    public boolean isNull() {
        return vtType == null || vtType.equals(Query.Type.NULL_TYPE) || value == null;
    }

    /**
     * IsComparable returns true if the Value is null safe comparable without collation information.
     */
    public boolean isComparable() {

        if (vtType == null || VtType.isNumber(vtType) || VtType.isBinary(vtType)) {
            return true;
        }
        switch (vtType){
            case TIMESTAMP:
            case DATE:
            case TIME:
            case DATETIME:
            case ENUM:
            case SET:
            case BIT:
                return true;
        }
        return false;
    }

    public byte[] toBytes() {
        if (vtType == Query.Type.EXPRESSION) {
            return null;
        }
        return toBytes(value);
    }

    @Override
    public String toString() {
        if (isNull() || vtType == Query.Type.EXPRESSION) {
            return null;
        }
        if (value instanceof byte[]) {
            return new String((byte[]) value);
        }
        if (value instanceof String) {
            return (String) value;
        } else {
            return String.valueOf(value);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VtResultValue value1 = (VtResultValue) o;
        return Objects.equals(value, value1.value) && vtType == value1.vtType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, vtType);
    }
}
