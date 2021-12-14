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

import com.google.protobuf.ByteString;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLCharExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLDateExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLHexExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLIntegerExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLMethodInvokeExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNullExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLNumberExpr;
import com.jd.jdbc.sqlparser.ast.expr.SQLTimestampExpr;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.srvtopo.BindVariable;
import com.mysql.cj.util.DataTypeUtil;
import io.vitess.proto.Query;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.codec.binary.Hex;

@Getter
@Setter
public class VtValue {
    public static VtValue NULL = new VtValue();

    private static Log log = LogFactory.getLog(VtValue.class);

    private Query.Type vtType;

    private byte[] vtValue;

    private String charEncoding;

    private VtValue() {
        vtType = Query.Type.NULL_TYPE;
        vtValue = null;
    }

    public VtValue(Query.Value v) {
        this.vtType = v.getType();
        this.vtValue = v.getValue().toByteArray();
    }

    public static VtValue newVtValue(SQLExpr node) throws SQLException {
        if (node instanceof SQLIntegerExpr) {
            return VtValue.newVtValue(Query.Type.INT64, ((SQLIntegerExpr) node).getValue().toString().getBytes());
        } else if (node instanceof SQLNumberExpr) {
            return VtValue.newVtValue(Query.Type.FLOAT64, String.valueOf(((SQLNumberExpr) node).getValue()).getBytes());
        } else if (node instanceof SQLHexExpr) {
            return VtValue.newVtValue(Query.Type.VARBINARY, ((SQLHexExpr) node).getValue());
        } else if (node instanceof SQLCharExpr) {
            String text = ((SQLCharExpr) node).getText();
            return VtValue.newVtValue(Query.Type.VARCHAR, text.getBytes());
        } else if (node instanceof SQLDateExpr) {
            return VtValue.newVtValue(Query.Type.DATE, ((SQLDateExpr) node).getValue().getBytes());
        } else if (node instanceof SQLTimestampExpr) {
            return VtValue.newVtValue(Query.Type.TIMESTAMP, ((SQLTimestampExpr) node).getValue().getBytes());
        } else if (node instanceof SQLMethodInvokeExpr) {
            return VtValue.NULL;
        } else if (node instanceof SQLNullExpr) {
            return VtValue.NULL;
        } else {
            throw new SQLException("un-supported AST node " + node);
        }
    }

    public static VtValue newVtValue(BindVariable bindVariable) throws SQLException {
        if (bindVariable.getType() == Query.Type.TUPLE) {
            throw new SQLException("cannot transfer bind variables to single vtvalue, as its tuple type");
        }
        return newVtValue(bindVariable.getType(), bindVariable.getValue());
    }

    public static List<VtValue> newVtValueList(BindVariable bindVariable) throws SQLException {
        if (bindVariable.getType() != Query.Type.TUPLE) {
            throw new SQLException("cannot transfer bind variables to vtvalue list, as it's not tuple type");
        }
        List<VtValue> vtValueList = new ArrayList<>();
        for (Query.Value value : bindVariable.getValuesList()) {
            VtValue vtValue = newVtValue(value.getType(), value.getValue().toByteArray());
            vtValueList.add(vtValue);
        }
        return vtValueList;
    }

    public static VtValue newVtValue(Query.Type typ, byte[] val) throws SQLException {
        VtValue vtValue = new VtValue();
        vtValue.vtType = typ;
        switch (typ.getNumber()) {
            case Query.Type.NULL_TYPE_VALUE: {
                vtValue.vtValue = null;
                return vtValue;
            }
            case Query.Type.INT8_VALUE: {
                int x = Integer.parseInt(new String(val));
                if (x < VtNumberRange.INT8_MIN || x > VtNumberRange.INT8_MAX) {
                    throw new SQLException(String.format("wrong data type %s for %s", typ, Arrays.toString(val)));
                }
                vtValue.vtValue = val;
                return vtValue;
            }
            case Query.Type.UINT8_VALUE: {
                int x = Integer.parseInt(new String(val));
                if (x < VtNumberRange.UINT8_MIN || x > VtNumberRange.UINT8_MAX) {
                    throw new SQLException(String.format("wrong data type %s for %s", typ, Arrays.toString(val)));
                }
                vtValue.vtValue = val;
                return vtValue;
            }
            case Query.Type.INT16_VALUE: {
                int x = Integer.parseInt(new String(val));
                if (x < VtNumberRange.INT16_MIN || x > VtNumberRange.INT16_MAX) {
                    throw new SQLException(String.format("wrong data type %s for %s", typ, Arrays.toString(val)));
                }
                vtValue.vtValue = val;
                return vtValue;
            }
            case Query.Type.UINT16_VALUE: {
                int x = Integer.parseInt(new String(val));
                if (x < VtNumberRange.UINT16_MIN || x > VtNumberRange.UINT16_MAX) {
                    throw new SQLException(String.format("wrong data type %s for %s", typ, Arrays.toString(val)));
                }
                vtValue.vtValue = val;
                return vtValue;
            }
            case Query.Type.INT24_VALUE: {
                int x = Integer.parseInt(new String(val));
                if (x < VtNumberRange.INT24_MIN || x > VtNumberRange.INT24_MAX) {
                    throw new SQLException(String.format("wrong data type %s for %s", typ, Arrays.toString(val)));
                }
                vtValue.vtValue = val;
                return vtValue;
            }
            case Query.Type.UINT24_VALUE: {
                int x = Integer.parseInt(new String(val));
                if (x < VtNumberRange.UINT24_MIN || x > VtNumberRange.UINT24_MAX) {
                    throw new SQLException(String.format("wrong data type %s for %s", typ, Arrays.toString(val)));
                }
                vtValue.vtValue = val;
                return vtValue;
            }
            case Query.Type.INT32_VALUE: {
                int x = Integer.parseInt(new String(val));
                if (x < VtNumberRange.INT32_MIN || x > VtNumberRange.INT32_MAX) {
                    throw new SQLException(String.format("wrong data type %s for %s", typ.toString(), Arrays.toString(val)));
                }
                vtValue.vtValue = val;
                return vtValue;
            }
            case Query.Type.UINT32_VALUE: {
                long x = Long.parseLong(new String(val));
                if (x < VtNumberRange.UINT32_MIN || x > VtNumberRange.UINT32_MAX) {
                    throw new SQLException(String.format("wrong data type %s for %s", typ, Arrays.toString(val)));
                }
                vtValue.vtValue = val;
                return vtValue;
            }
            case Query.Type.INT64_VALUE: {
                try {
                    long x = Long.parseLong(new String(val));
                    if (x < VtNumberRange.INT64_MIN || x > VtNumberRange.INT64_MAX) {
                        throw new SQLException(String.format("wrong data type %s for %s", typ.toString(), Arrays.toString(val)));
                    }
                } catch (NumberFormatException e) {
                    BigInteger x = new BigInteger(new String(val));
                    if (x.compareTo(new BigInteger(String.valueOf(VtNumberRange.UINT64_MIN))) < 0 ||
                        x.compareTo(new BigInteger(String.valueOf(VtNumberRange.UINT64_MAX))) > 0) {
                        throw new SQLException(String.format("wrong data type %s for %s", typ, Arrays.toString(val)));
                    }
                    vtValue.vtType = Query.Type.UINT64;
                }
                vtValue.vtValue = val;
                return vtValue;
            }
            case Query.Type.UINT64_VALUE: {
                BigInteger x = new BigInteger(new String(val));
                if (x.compareTo(new BigInteger(String.valueOf(VtNumberRange.UINT64_MIN))) < 0 ||
                    x.compareTo(new BigInteger(String.valueOf(VtNumberRange.UINT64_MAX))) > 0) {
                    throw new SQLException(String.format("wrong data type %s for %s", typ, Arrays.toString(val)));
                }
                vtValue.vtValue = val;
                return vtValue;
            }
            case Query.Type.FLOAT32_VALUE: {
                float x = Float.parseFloat(new String(val));
                vtValue.vtValue = val;
                return vtValue;
            }
            case Query.Type.FLOAT64_VALUE: {
                double x = Double.parseDouble(new String(val));
                vtValue.vtValue = val;
                return vtValue;
            }
            default:
                vtValue.vtValue = val;
                return vtValue;
        }
    }

    public static String toSqlReadable(Query.Type typ, byte[] val) {
        if (typ == Query.Type.BINARY ||
            typ == Query.Type.VARBINARY ||
            typ == Query.Type.BLOB) {

            /* e.g.:
                byte[] buffer = new byte[]{0x4D, 0x79, 0x53, 0x51,0x4C, ...};
                to X'4D7953514C...'
            */
            return "X'" + Hex.encodeHexString(val) + "'";
        } else {
            if (VtType.isQuoted(typ)) {
                return "'" + new String(val) + "'";
            } else {
                return new String(val);
            }
        }
    }

    public static boolean isQuoted(Query.Type vtType) {
        return vtType != null && VtType.isQuoted(vtType);
    }

    public static boolean isNull(Query.Type vtType) {
        return vtType == null || vtType.equals(Query.Type.NULL_TYPE);
    }

    public static VtValue toVtValue(final Object object) throws SQLException {
        if (object == null) {
            return VtValue.NULL;
        }
        String className = object.getClass().getName();
        switch (className) {
            case "java.lang.String":
                return VtValue.newVtValue(Query.Type.VARBINARY, ((String) object).getBytes());
            case "java.lang.Boolean":
                return VtValue.newVtValue(Query.Type.BIT, (Boolean) object ? new byte[] {1} : new byte[] {0});
            case "java.math.BigDecimal":
                return VtValue.newVtValue(Query.Type.DECIMAL, (((BigDecimal) object).toEngineeringString()).getBytes());
            case "java.math.BigInteger":
                if (((BigInteger) object).compareTo(new BigInteger(String.valueOf(VtNumberRange.INT64_MIN))) >= 0
                    && ((BigInteger) object).compareTo(new BigInteger(String.valueOf(VtNumberRange.INT64_MAX))) <= 0) {
                    return VtValue.newVtValue(Query.Type.INT64, ((BigInteger) object).toString(10).getBytes());
                } else {
                    return VtValue.newVtValue(Query.Type.UINT64, ((BigInteger) object).toString(10).getBytes());
                }
            case "java.lang.Byte":
                return VtValue.newVtValue(Query.Type.INT8, String.valueOf(object).getBytes());
            case "java.lang.Double":
                return VtValue.newVtValue(Query.Type.FLOAT64, String.valueOf(object).getBytes());
            case "java.lang.Float":
                return VtValue.newVtValue(Query.Type.FLOAT32, String.valueOf(object).getBytes());
            case "java.lang.Integer":
                return VtValue.newVtValue(Query.Type.INT32, String.valueOf(object).getBytes());
            case "java.lang.Long":
                return VtValue.newVtValue(Query.Type.INT64, String.valueOf(object).getBytes());
            case "java.lang.Short":
                return VtValue.newVtValue(Query.Type.INT16, String.valueOf(object).getBytes());
            default:
                throw new RuntimeException("unknown class " + className);
        }
    }

    public VtValue copy() {
        VtValue out = new VtValue();
        out.vtType = this.vtType;
        out.vtValue = new byte[this.vtValue.length];
        System.arraycopy(this.vtValue, 0, out.vtValue, 0, this.vtValue.length);
        return out;
    }

    public byte[] raw() {
        return vtValue;
    }

    public boolean isIntegral() {
        return vtType != null && VtType.isIntegral(vtType);
    }

    public boolean isFloat() {
        return vtType != null && VtType.isFloat(vtType);
    }

    public boolean isSigned() {
        return vtType != null && VtType.isSigned(vtType);
    }

    public boolean isUnsigned() {
        return vtType != null && VtType.isUnsigned(vtType);
    }

    public boolean isNumber() {
        return vtType != null && VtType.isNumber(vtType);
    }

    public boolean isQuoted() {
        return vtType != null && VtType.isQuoted(vtType);
    }

    public boolean isNull() {
        return vtType == null || vtType.equals(Query.Type.NULL_TYPE);
    }

    public boolean isBinary() {
        return vtType != null && VtType.isBinary(vtType);
    }

    public boolean isText() {
        return vtType != null && VtType.isText(vtType);
    }

    @Override
    public String toString() {
        if (isNull() || vtType == Query.Type.EXPRESSION) {
            return null;
        }
        return StringUtils.toString(vtValue, charEncoding);
    }

    public String toSqlReadable() {
        return toSqlReadable(vtType, vtValue);
    }

    public boolean toBoolean() {
        if (isNull()) {
            return false;
        }

        if (isIntegral() && getVtType() != Query.Type.UINT64) {
            long l = Long.parseLong(new String(vtValue, 0, vtValue.length));
            // Goes back to ODBC driver compatibility, and VB/Automation Languages/COM, where in Windows "-1" can mean true as well.
            return (l == -1 || l > 0);
        }

        if (getVtType() == Query.Type.UINT64) {
            BigInteger bigInteger = new BigInteger(new String(vtValue, 0, vtValue.length));
            return bigInteger.compareTo(BigInteger.valueOf(0)) > 0;
        }

        if (isFloat()) {
            // this means that 0.1 or -1 will be TRUE
            double d = Double.parseDouble(new String(vtValue, 0, vtValue.length));
            return d > 0 || d == -1.0d;
        }

        if (getVtType() == Query.Type.DECIMAL) {
            // this means that 0.1 or -1 will be TRUE
            BigDecimal d = new BigDecimal(new String(vtValue, 0, vtValue.length));
            return d.compareTo(BigDecimal.valueOf(0)) > 0 || d.compareTo(BigDecimal.valueOf(-1)) == 0;
        }

        if (getVtType() == Query.Type.BIT) {
            long l = DataTypeUtil.bitToLong(getVtValue(), 0, getVtValue().length);
            return (l == -1 || l > 0);
        }

        String s = new String(vtValue, 0, vtValue.length);
        if ("y".equalsIgnoreCase(s) || "true".equalsIgnoreCase(s)) {
            return true;
        }

        if ("n".equalsIgnoreCase(s) || "false".equalsIgnoreCase(s)) {
            return false;
        }

        log.warn("undetermined value to boolean: %s" + s);
        return false;
    }

    public int toInt() throws SQLException {
        if (isNull()) {
            return 0;
        }

        if (isIntegral() && getVtType() != Query.Type.UINT64) {
            long l = Long.parseLong(new String(vtValue, 0, vtValue.length));
            if ((l < Integer.MIN_VALUE || l > Integer.MAX_VALUE)) {
                throw new SQLException("number out of range: " + Long.valueOf(l).toString());
            }
            return (int) l;
        }

        if (getVtType() == Query.Type.UINT64) {
            BigInteger i = new BigInteger(new String(vtValue, 0, vtValue.length));
            if ((i.compareTo(Constants.BIG_INTEGER_MIN_INTEGER_VALUE) < 0 || i.compareTo(Constants.BIG_INTEGER_MAX_INTEGER_VALUE) > 0)) {
                throw new SQLException("number out of range: " + i);
            }
            return i.intValue();
        }

        if (isFloat()) {
            double d = Double.parseDouble(new String(vtValue, 0, vtValue.length));
            if ((d < Integer.MIN_VALUE || d > Integer.MAX_VALUE)) {
                throw new SQLException("number out of range: " + Double.valueOf(d).toString());
            }
            return (int) d;
        }

        if (getVtType() == Query.Type.DECIMAL) {
            BigDecimal d = new BigDecimal(new String(vtValue, 0, vtValue.length));
            if ((d.compareTo(Constants.BIG_DECIMAL_MIN_INTEGER_VALUE) < 0 || d.compareTo(Constants.BIG_DECIMAL_MAX_INTEGER_VALUE) > 0)) {
                throw new SQLException("number out of range: " + d);
            }
            return (int) d.longValue();
        }

        if (getVtType() == Query.Type.BIT) {
            long l = DataTypeUtil.bitToLong(vtValue, 0, vtValue.length);
            if (l >> 32 != 0) {
                throw new SQLException("number out of range: " + Long.valueOf(l).toString());
            }
            return (int) l;
        }

        String s = new String(vtValue, 0, vtValue.length);
        return Integer.parseInt(s);
    }

    public long toLong() throws SQLException {
        if (isNull()) {
            return 0;
        }

        if (isIntegral() && getVtType() != Query.Type.UINT64) {
            return Long.parseLong(new String(vtValue, 0, vtValue.length));
        }

        if (getVtType() == Query.Type.UINT64) {
            BigInteger i = new BigInteger(new String(vtValue, 0, vtValue.length));
            if ((i.compareTo(Constants.BIG_INTEGER_MIN_LONG_VALUE) < 0 || i.compareTo(Constants.BIG_INTEGER_MAX_LONG_VALUE) > 0)) {
                throw new SQLException("number out of range: " + i);
            }
            return i.longValue();
        }

        if (isFloat()) {
            double d = Double.parseDouble(new String(vtValue, 0, vtValue.length));
            if ((d < Long.MIN_VALUE || d > Long.MAX_VALUE)) {
                throw new SQLException("number out of range: " + Double.valueOf(d).toString());
            }
            return (long) d;
        }

        if (getVtType() == Query.Type.DECIMAL) {
            BigDecimal d = new BigDecimal(new String(vtValue, 0, vtValue.length));
            if ((d.compareTo(Constants.BIG_DECIMAL_MIN_LONG_VALUE) < 0 || d.compareTo(Constants.BIG_DECIMAL_MAX_LONG_VALUE) > 0)) {
                throw new SQLException("number out of range: " + d);
            }
            return d.longValue();
        }

        if (getVtType() == Query.Type.BIT) {
            return DataTypeUtil.bitToLong(vtValue, 0, vtValue.length);
        }

        String s = new String(vtValue, 0, vtValue.length);
        return Long.parseLong(s);
    }

    public BigDecimal toDecimal() {
        if (isNull()) {
            return null;
        }

        if (getVtType() == Query.Type.BIT) {
            return new BigDecimal(new BigInteger(ByteBuffer.allocate(vtValue.length + 1).put((byte) 0).put(vtValue, 0, vtValue.length).array()));
        }

        return new BigDecimal(new String(vtValue, 0, vtValue.length));
    }

    public byte[] toBytes() {
        if (vtType == Query.Type.EXPRESSION) {
            return null;
        }
        return vtValue;
    }

    public Query.Value toQueryValue() {
        if (this.isNull()) {
            return Query.Value.newBuilder().build();
        }
        return Query.Value.newBuilder().setType(this.getVtType()).setValue(ByteString.copyFrom(this.getVtValue())).build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VtValue)) {
            return false;
        }

        VtValue vtValue1 = (VtValue) o;

        if (vtType != vtValue1.vtType) {
            return false;
        }
        return Arrays.equals(vtValue, vtValue1.vtValue);
    }

    @Override
    public int hashCode() {
        int result = vtType != null ? vtType.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(vtValue);
        return result;
    }
}
