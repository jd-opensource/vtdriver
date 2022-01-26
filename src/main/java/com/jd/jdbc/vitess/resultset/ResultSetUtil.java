/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jd.jdbc.vitess.resultset;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.mysql.cj.conf.PropertyDefinitions;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;
import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

public final class ResultSetUtil {
    private ResultSetUtil() {
    }

    public static Object convertValue(final Object value, final Class<?> convertType) {
        if (null == value) {
            return convertNullValue(convertType);
        }
        if (value.getClass() == convertType) {
            return value;
        }
        if (value instanceof Number) {
            return convertNumberValue(value, convertType);
        }
        if (value instanceof byte[]) {
            return convertByteArrayValue(value, convertType);
        }
        if (String.class.equals(convertType)) {
            return value.toString();
        } else {
            if (value instanceof String) {
                String s = (String) value;
                if (boolean.class == convertType || Boolean.class == convertType) {
                    return Boolean.valueOf(s);
                }
                if (byte.class == convertType || Byte.class == convertType) {
                    return Byte.valueOf(s);
                }
                if (short.class == convertType || Short.class == convertType) {
                    return Short.valueOf(s);
                }
                if (int.class == convertType || Integer.class == convertType) {
                    return Integer.valueOf(s);
                }
                if (long.class == convertType || Long.class == convertType) {
                    return Long.valueOf(s);
                }
                if (float.class == convertType || Float.class == convertType) {
                    return Float.valueOf(s);
                }
                if (double.class == convertType || Double.class == convertType) {
                    return Double.valueOf(s);
                }
                if (BigInteger.class == convertType) {
                    return new BigInteger(s);
                }
                if (BigDecimal.class == convertType) {
                    return new BigDecimal(s);
                }
            }
            return value;
        }
    }

    public static Object convertValue(final VtResultValue vtResultValue, final Class<?> convertType, final TimeZone srcTz, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior)
        throws SQLException {
        Object value = vtResultValue.getValue();
        if (null == value) {
            return convertNullValue(convertType);
        }
        if (Timestamp.class == convertType) {
            return convertToTimestamp(vtResultValue, srcTz, zeroDatetimeBehavior);
        }
        if (Time.class == convertType) {
            return convertToTime(vtResultValue, srcTz, zeroDatetimeBehavior);
        }
        if (java.sql.Date.class == convertType) {
            return converToDate(vtResultValue, srcTz, zeroDatetimeBehavior);
        }
        if (LocalDate.class.equals(convertType)) {
            return convertToLocalDate(vtResultValue, zeroDatetimeBehavior);
        }
        if (LocalTime.class.equals(convertType)) {
            return convertToLocalTime(vtResultValue, zeroDatetimeBehavior);
        }
        if (LocalDateTime.class.equals(convertType)) {
            return convertToLocalDateTime(vtResultValue, zeroDatetimeBehavior);
        }
        throw new SQLException("Conversion not supported for type " + convertType.getName());
    }

    public static Object convertValue(final VtResultValue vtResultValue, final Class<?> convertType) throws SQLException {
        Object value = vtResultValue.getValue();
        if (null == value) {
            return convertNullValue(convertType);
        }
        if (String.class.equals(convertType)) {
            return convertToString(vtResultValue);
        }
        if (BigDecimal.class.equals(convertType)) {
            return convertToBigDecimal(vtResultValue);
        }
        if (BigInteger.class.equals(convertType)) {
            return convertToBigInteger(vtResultValue);
        }
        if (Boolean.class.equals(convertType) || boolean.class.equals(convertType)) {
            return convertToBoolean(vtResultValue);
        }
        if (Byte.class.equals(convertType) || byte.class.equals(convertType)) {
            return convertToByte(vtResultValue);
        }
        if (Short.class.equals(convertType) || short.class.equals(convertType)) {
            return convertToShort(vtResultValue);
        }
        if (Integer.class.equals(convertType) || int.class.equals(convertType)) {
            return convertToInteger(vtResultValue);
        }
        if (Long.class.equals(convertType) || long.class.equals(convertType)) {
            return convertToLong(vtResultValue);
        }
        if (Float.class.equals(convertType) || float.class.equals(convertType)) {
            return convertToFloat(vtResultValue);
        }
        if (Double.class.equals(convertType) || double.class.equals(convertType)) {
            return convertToDouble(vtResultValue);
        }
        if (byte[].class.equals(convertType)) {
            return convertToByteArray(vtResultValue);
        }
        if (URL.class.equals(convertType)) {
            return convertToURL(vtResultValue);
        }
        if (Clob.class.equals(convertType)) {
            return new SerialClob(convertToString(vtResultValue).toCharArray());
        }
        if (Blob.class.equals(convertType)) {
            return new SerialBlob(vtResultValue.toBytes());
        }
        if (InputStreamReader.class.equals(convertType)) {
            return new InputStreamReader(new ByteArrayInputStream(vtResultValue.toBytes()));
        }
        if (OffsetDateTime.class.equals(convertType)) {
            try {
                String odt = convertToString(vtResultValue);
                return OffsetDateTime.parse(odt);
            } catch (DateTimeParseException e) {
                // Let it continue and try by object deserialization.
            }
        }
        if (OffsetTime.class.equals(convertType)) {
            try {
                String ot = convertToString(vtResultValue);
                return OffsetTime.parse(ot);
            } catch (DateTimeParseException e) {
                // Let it continue and try by object deserialization.
            }
        }

        throw new SQLException("Conversion not supported for type " + convertType.getName());
    }

    private static byte[] convertToByteArray(final VtResultValue vtResultValue) throws SQLDataException {
        Object value = vtResultValue.getValue();
        switch (vtResultValue.getVtType()) {
            case BIT:
                if (value instanceof Integer) {
                    return Ints.toByteArray((Integer) value);
                }
                return Longs.toByteArray((Long) value);
            case BINARY:
            case VARBINARY:
                if (value instanceof byte[]) {
                    return (byte[]) value;
                }
                break;
            case BLOB:
            case YEAR:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
                return (byte[]) value;
        }
        return convertToString(vtResultValue).getBytes();
    }

    private static Double convertToDouble(final VtResultValue vtResultValue) throws SQLDataException {
        Object value = vtResultValue.getValue();
        switch (vtResultValue.getVtType()) {
            case INT8:
            case UINT8:
            case INT16:
            case UINT16:
            case INT24:
            case UINT24:
            case INT32:
                return Double.valueOf((Integer) value);
            case UINT32:
            case INT64:
                return Double.valueOf((Long) value);
            case UINT64:
                BigInteger bi = (BigInteger) value;
                return bi.doubleValue();
            case FLOAT32:
                return Double.valueOf(value.toString());
            case FLOAT64:
                return (Double) value;
            case DECIMAL:
                BigDecimal bd = (BigDecimal) value;
                if (bd.compareTo(BigDecimal.valueOf(-Double.MAX_VALUE)) < 0 || bd.compareTo(BigDecimal.valueOf(Double.MAX_VALUE)) > 0) {
                    throw new SQLDataException("java.sql.SQLDataException: Value '" + bd + "' is outside of valid range for type java.lang.Double");
                }
                return bd.doubleValue();
            case VARCHAR:
            case CHAR:
            case TEXT:
            case JSON:
                String str = (String) value;
                double d;
                try {
                    d = Double.parseDouble(str);
                } catch (NumberFormatException e) {
                    throw new SQLDataException("Value '" + str + "' is outside of valid range for type java.lang.Double");
                }
                return d;
            case BIT:
                if (value instanceof Boolean) {
                    return (Boolean) value ? 1D : 0D;
                }
                if (value instanceof Integer) {
                    return ((Integer) value).doubleValue();
                }
                return ((Long) value).doubleValue();
            case YEAR:
                return new Double(new String((byte[]) value));
            default:
                throw new SQLDataException("Unsupported conversion from " + vtResultValue.getVtType() + " to java.lang.Double");
        }
    }

    private static Float convertToFloat(final VtResultValue vtResultValue) throws SQLDataException {
        Object value = vtResultValue.getValue();
        switch (vtResultValue.getVtType()) {
            case INT8:
            case UINT8:
            case INT16:
            case UINT16:
            case INT24:
            case UINT24:
            case INT32:
                return Float.valueOf((Integer) value);
            case UINT32:
            case INT64:
                return Float.valueOf((Long) value);
            case UINT64:
                BigInteger bi = (BigInteger) value;
                return bi.floatValue();
            case FLOAT32:
                return (Float) value;
            case FLOAT64:
                Double d = (Double) value;
                if (d < -Float.MAX_VALUE || d > Float.MAX_VALUE) {
                    throw new SQLDataException("Value '" + d + "' is outside of valid range for type java.lang.Float");
                }
                return d.floatValue();
            case DECIMAL:
                BigDecimal bd = (BigDecimal) value;
                if (bd.compareTo(BigDecimal.valueOf(-Float.MAX_VALUE)) < 0 || bd.compareTo(BigDecimal.valueOf(Float.MAX_VALUE)) > 0) {
                    throw new SQLDataException("java.sql.SQLDataException: Value '" + bd + "' is outside of valid range for type java.lang.Float");
                }
                return bd.floatValue();
            case VARCHAR:
            case CHAR:
            case TEXT:
            case JSON:
                String str = (String) value;
                float f;
                try {
                    f = Float.parseFloat(str);
                } catch (NumberFormatException e) {
                    throw new SQLDataException("Value '" + str + "' is outside of valid range for type java.lang.Float");
                }
                return f;
            case BIT:
                if (value instanceof Boolean) {
                    return (Boolean) value ? 1F : 0F;
                }
                if (value instanceof Integer) {
                    return Float.valueOf((Integer) value);
                }
                return Float.valueOf((Long) value);
            case YEAR:
                return new Float(new String((byte[]) value));
            default:
                throw new SQLDataException("Unsupported conversion from " + vtResultValue.getVtType() + " to java.lang.Float");
        }
    }

    private static Short convertToShort(final VtResultValue vtResultValue) throws SQLDataException {
        Object value = vtResultValue.getValue();
        switch (vtResultValue.getVtType()) {
            case INT8:
            case UINT8:
            case INT16:
            case UINT16:
            case INT24:
            case UINT24:
            case INT32:
                Integer i = (Integer) value;
                if (i < Short.MIN_VALUE || i > Short.MAX_VALUE) {
                    throw new SQLDataException("Value '" + i + "' is outside of valid range for type java.lang.Short");
                }
                return ((Integer) value).shortValue();
            case UINT32:
            case INT64:
                Long li = (Long) value;
                if (li < Short.MIN_VALUE || li > Short.MAX_VALUE) {
                    throw new SQLDataException("Value '" + li + "' is outside of valid range for type java.lang.Short");
                }
                return li.shortValue();
            case UINT64:
                BigInteger bi = (BigInteger) value;
                if (bi.compareTo(BigInteger.valueOf(Short.MIN_VALUE)) < 0 ||
                    bi.compareTo(BigInteger.valueOf(Short.MAX_VALUE)) > 0) {
                    throw new SQLDataException("Value '" + bi + "' is outside of valid range for type java.lang.Short");
                }
                return bi.shortValue();
            case FLOAT32:
                Float f = (Float) value;
                if (f < Short.MIN_VALUE || f > Short.MAX_VALUE) {
                    throw new SQLDataException("Value '" + f + "' is outside of valid range for type java.lang.Short");
                }
                return f.shortValue();
            case FLOAT64:
                Double d = (Double) value;
                if (d < Short.MIN_VALUE || d > Short.MAX_VALUE) {
                    throw new SQLDataException("Value '" + d + "' is outside of valid range for type java.lang.Short");
                }
                return d.shortValue();
            case DECIMAL:
                BigDecimal bd = (BigDecimal) value;
                if (bd.compareTo(BigDecimal.valueOf(Short.MIN_VALUE)) < 0 || bd.compareTo(BigDecimal.valueOf(Short.MAX_VALUE)) > 0) {
                    throw new SQLDataException("java.sql.SQLDataException: Value '" + bd + "' is outside of valid range for type java.lang.Short");
                }
                return bd.shortValue();
            case VARCHAR:
            case CHAR:
            case TEXT:
            case JSON:
                String str = (String) value;
                short si;
                try {
                    si = Short.parseShort(str);
                } catch (NumberFormatException e) {
                    throw new SQLDataException("Value '" + str + "' is outside of valid range for type java.lang.Short");
                }
                return si;
            case BIT:
                if (value instanceof Boolean) {
                    return (Boolean) value ? (short) 1 : (short) 0;
                }
                if (value instanceof Integer) {
                    Integer li2 = (Integer) value;
                    if (li2 < Short.MIN_VALUE || li2 > Short.MAX_VALUE) {
                        throw new SQLDataException("Value '" + li2 + "' is outside of valid range for type java.lang.Short");
                    }
                    return li2.shortValue();
                }
                Long li2 = (Long) value;
                if (li2 < Short.MIN_VALUE || li2 > Short.MAX_VALUE) {
                    throw new SQLDataException("Value '" + li2 + "' is outside of valid range for type java.lang.Short");
                }
                return li2.shortValue();
            case YEAR:
                return new Short(new String((byte[]) value));
            default:
                throw new SQLDataException("Unsupported conversion from " + vtResultValue.getVtType() + " to java.lang.Short");
        }
    }

    private static Byte convertToByte(final VtResultValue vtResultValue) throws SQLDataException {
        Object value = vtResultValue.getValue();
        switch (vtResultValue.getVtType()) {
            case VARCHAR:
            case CHAR:
            case TEXT:
            case JSON:
                String str = (String) value;
                byte[] bytes = str.getBytes();
                if (bytes.length > 1) {
                    throw new SQLDataException("Value '" + str + "' is outside of valid range for type java.lang.Byte");
                } else {
                    return bytes.length == 0 ? (byte) 0 : bytes[0];
                }
            default:
                Integer i = convertToInteger(vtResultValue);
                if (i < Byte.MIN_VALUE || i > Byte.MAX_VALUE) {
                    throw new SQLDataException("Value '" + i + "' is outside of valid range for type java.lang.Byte");
                }
                return i.byteValue();
        }
    }

    private static Boolean parseBooleanFromString(final String str) throws SQLDataException {
        if ("Y".equalsIgnoreCase(str) || "true".equalsIgnoreCase(str)) {
            return true;
        } else if ("N".equalsIgnoreCase(str) || "false".equalsIgnoreCase(str)) {
            return false;
        }
        if (str.matches("-?\\d+")) {
            int i = Integer.parseInt(str);
            return i > 0 || i == -1;
        } else if (str.contains("e") || str.contains("E") || str.matches("-?\\d*\\.\\d*")) {
            double d2 = Double.parseDouble(str);
            return d2 > 0 || d2 == -1;
        }
        throw new SQLDataException("Cannot determine value type from string '" + str + "'");
    }

    private static Boolean convertToBoolean(final VtResultValue vtResultValue) throws SQLDataException {
        Object value = vtResultValue.getValue();
        switch (vtResultValue.getVtType()) {
            case INT8:
            case UINT8:
            case INT16:
            case UINT16:
            case INT24:
            case UINT24:
            case INT32:
                return (Integer) value > 0 || (Integer) value == -1;
            case UINT32:
            case INT64:
                return (Long) value > 0 || (Long) value == -1;
            case UINT64:
                BigInteger bi = (BigInteger) value;
                return bi.compareTo(BigInteger.valueOf(0)) > 0 || bi.compareTo(BigInteger.valueOf(-1)) == 0;
            case FLOAT32:
                Float f = (Float) value;
                return f > 0 || f == -1;
            case FLOAT64:
                Double d = (Double) value;
                return d > 0 || d == -1;
            case VARCHAR:
            case CHAR:
            case TEXT:
            case JSON:
                return parseBooleanFromString((String) value);
            case BIT:
                if (value instanceof Integer) {
                    int i = (Integer) value;
                    return i > 0 || i == -1;
                }
                Long li = (Long) value;
                return li > 0 || li == -1;
            case YEAR:
                int y = Integer.parseInt(new String((byte[]) value));
                return y > 0 || y == -1;
            case DECIMAL:
                BigDecimal bd = (BigDecimal) value;
                return bd.compareTo(BigDecimal.valueOf(0)) > 0 || bd.compareTo(BigDecimal.valueOf(-1)) == 0;
            default:
                throw new SQLDataException("Unsupported conversion from " + vtResultValue.getVtType() + " to java.math.Boolean");
        }
    }

    private static String convertToString(final VtResultValue vtResultValue) {
        Object value = vtResultValue.getValue();
        switch (vtResultValue.getVtType()) {
            case YEAR:
                return new String((byte[]) value) + "-01-01";
            case DATE:
            case BINARY:
            case VARBINARY:
                if (value instanceof byte[]) {
                    return new String((byte[]) value);
                }
                break;
            case BLOB:
                return new String((byte[]) value);
            case TIME:
            case TIMESTAMP:
            case DATETIME:
                String str = new String((byte[]) value);
                int idx = str.indexOf(".");
                if (idx != -1 && Integer.parseInt(str.substring(idx + 1)) == 0) {
                    str = str.substring(0, idx);
                }
                return str;
        }
        return value.toString();
    }

    private static BigInteger convertToBigInteger(final VtResultValue vtResultValue) throws SQLException {
        String stringVal = convertToString(vtResultValue);
        try {
            return new BigInteger(stringVal);
        } catch (NumberFormatException nfe) {
            throw new SQLException("Bad format for BigInteger '" + stringVal + "'");
        }
    }

    private static BigDecimal convertToBigDecimal(final VtResultValue vtResultValue) {
        Object value = vtResultValue.getValue();
        switch (vtResultValue.getVtType()) {
            case YEAR:
                return new BigDecimal(new String((byte[]) value));
            default:
                return new BigDecimal(convertToString(vtResultValue));
        }
    }

    private static Long convertToLong(final VtResultValue vtResultValue) throws SQLDataException {
        Object value = vtResultValue.getValue();
        switch (vtResultValue.getVtType()) {
            case INT8:
            case UINT8:
            case INT16:
            case UINT16:
            case INT24:
            case UINT24:
            case INT32:
                return Long.valueOf((Integer) value);
            case UINT32:
            case INT64:
                return (Long) value;
            case UINT64:
                BigInteger bi = (BigInteger) value;
                if (bi.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0
                    || bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
                    throw new SQLDataException("Value '" + bi + "' is outside of valid range for type java.lang.Long");
                }
                return bi.longValue();
            case FLOAT32:
                Float f = (Float) value;
                if (f < Long.MIN_VALUE || f > Long.MAX_VALUE) {
                    throw new SQLDataException("Value '" + f + "' is outside of valid range for type java.lang.Long");
                }
                return f.longValue();
            case FLOAT64:
                Double d = (Double) value;
                if (d < Long.MIN_VALUE || d > Long.MAX_VALUE) {
                    throw new SQLDataException("Value '" + d + "' is outside of valid range for type java.lang.Long");
                }
                return d.longValue();
            case VARCHAR:
            case CHAR:
            case TEXT:
            case JSON:
                String str = (String) value;
                long li;
                try {
                    li = Long.parseLong(str);
                } catch (NumberFormatException e) {
                    throw new SQLDataException("Value '" + str + "' is outside of valid range for type java.lang.Long");
                }
                return li;
            case BIT:
                if (value instanceof Integer) {
                    return ((Integer) value).longValue();
                }
                return (Long) value;
            case YEAR:
                return new Long(new String((byte[]) value));
            case DECIMAL:
                BigDecimal bd = (BigDecimal) value;
                if (bd.compareTo(BigDecimal.valueOf(Long.MIN_VALUE)) < 0 || bd.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
                    throw new SQLDataException("Value '" + bd + "' is outside of valid range for type java.lang.Long");
                }
                return ((BigDecimal) value).longValue();
            default:
                throw new SQLDataException("Unsupported conversion from " + vtResultValue.getVtType() + " to java.lang.Long");
        }
    }

    private static Integer convertToInteger(final VtResultValue vtResultValue) throws SQLDataException {
        Object value = vtResultValue.getValue();
        switch (vtResultValue.getVtType()) {
            case INT8:
            case UINT8:
            case INT16:
            case UINT16:
            case INT24:
            case UINT24:
            case INT32:
                return (Integer) value;
            case UINT32:
            case INT64:
                Long li = (Long) value;
                if (li < Integer.MIN_VALUE || li > Integer.MAX_VALUE) {
                    throw new SQLDataException("Value '" + li + "' is outside of valid range for type java.lang.Integer");
                }
                return li.intValue();
            case UINT64:
                BigInteger bi = (BigInteger) value;
                if (bi.compareTo(BigInteger.valueOf(Integer.MIN_VALUE)) < 0 ||
                    bi.compareTo(BigInteger.valueOf(Integer.MAX_VALUE)) > 0) {
                    throw new SQLDataException("Value '" + bi + "' is outside of valid range for type java.lang.Integer");
                }
                return bi.intValue();
            case FLOAT32:
                Float f = (Float) value;
                if (f < Integer.MIN_VALUE || f > Integer.MAX_VALUE) {
                    throw new SQLDataException("Value '" + f + "' is outside of valid range for type java.lang.Integer");
                }
                return f.intValue();
            case FLOAT64:
                Double d = (Double) value;
                if (d < Integer.MIN_VALUE || d > Integer.MAX_VALUE) {
                    throw new SQLDataException("Value '" + d + "' is outside of valid range for type java.lang.Integer");
                }
                return d.intValue();
            case VARCHAR:
            case CHAR:
            case TEXT:
            case JSON:
                String str = (String) value;
                int i;
                try {
                    i = Integer.parseInt(str);
                } catch (NumberFormatException e) {
                    throw new SQLDataException("Value '" + str + "' is outside of valid range for type java.lang.Integer");
                }
                return i;
            case BIT:
                if (value instanceof Integer) {
                    return (Integer) value;
                }
                Long li2 = (Long) value;
                if (li2 < Integer.MIN_VALUE || li2 > Integer.MAX_VALUE) {
                    throw new SQLDataException("Value '" + li2 + "' is outside of valid range for type java.lang.Integer");
                }
                return li2.intValue();
            case YEAR:
                return Integer.parseInt(new String((byte[]) value));
            case DECIMAL:
                BigDecimal bd = (BigDecimal) value;
                return bd.intValue();
            default:
                throw new SQLDataException("Unsupported conversion from " + vtResultValue.getVtType() + " to java.lang.Integer");
        }
    }

    private static java.sql.Date converToDate(final VtResultValue vtResultValue, final TimeZone srcTz, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        Object value = vtResultValue.getValue();
        switch (vtResultValue.getVtType()) {
            case YEAR:
                return DateTimeUtil.getDateFromYearString(new String((byte[]) value), srcTz, zeroDatetimeBehavior);
            case DATE:
                return DateTimeUtil.getDateFromDateString(new String((byte[]) value), srcTz, zeroDatetimeBehavior);
            case DATETIME:
            case TIMESTAMP:
                return DateTimeUtil.getDateFromTimestampString(new String((byte[]) value), srcTz, zeroDatetimeBehavior);
            case TIME:
                Time time = DateTimeUtil.getTimeFromTimeString(new String((byte[]) value), TimeZone.getTimeZone("UTC"));
                return new java.sql.Date(time.getTime());
            default:
                throw new SQLDataException("Unsupported conversion from " + vtResultValue.getVtType() + " to java.sql.Date");
        }
    }

    private static Object convertToTimestamp(final VtResultValue vtResultValue, final TimeZone srcTz, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        Object value = vtResultValue.getValue();
        switch (vtResultValue.getVtType()) {
            case YEAR:
                return DateTimeUtil.getTimestampFromYearString(new String((byte[]) value), srcTz, zeroDatetimeBehavior);
            case DATE:
                return DateTimeUtil.getTimestampFromDateString(new String((byte[]) value), srcTz, zeroDatetimeBehavior);
            case DATETIME:
            case TIMESTAMP:
                return DateTimeUtil.getTimestampFromTimestampString(new String((byte[]) value), srcTz, zeroDatetimeBehavior);
            case TIME:
                return DateTimeUtil.getTimeStampFromTimeString(new String((byte[]) value), srcTz, zeroDatetimeBehavior);
            default:
                throw new SQLDataException("Unsupported conversion from " + vtResultValue.getVtType() + " to java.sql.Timestamp");
        }
    }

    private static Object convertToTime(final VtResultValue vtResultValue, final TimeZone srcTz, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        Object value = vtResultValue.getValue();
        switch (vtResultValue.getVtType()) {
            case DATETIME:
            case TIMESTAMP:
                return DateTimeUtil.getTimeFromTimestampString(new String((byte[]) value), srcTz, zeroDatetimeBehavior);
            case TIME:
                return DateTimeUtil.getTimeFromTimeString(new String((byte[]) value), srcTz);
            default:
                throw new SQLDataException("Unsupported conversion from " + vtResultValue.getVtType() + " to java.sql.Time");
        }
    }

    protected static Object convertToURL(final VtResultValue vtResultValue) {
        String val = vtResultValue.getValue().toString();
        try {
            return new URL(val);
        } catch (final MalformedURLException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    private static BigDecimal adjustBigDecimalResult(final BigDecimal value, final boolean needScale, final int scale) {
        if (needScale) {
            try {
                return value.setScale(scale);
            } catch (final ArithmeticException ex) {
                return value.setScale(scale, BigDecimal.ROUND_HALF_UP);
            }
        }
        return value;
    }

    private static Object convertToLocalDateTime(final VtResultValue vtResultValue, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        Object value = vtResultValue.getValue();
        switch (vtResultValue.getVtType()) {
            case YEAR:
                return DateTimeUtil.getLocalDateTimeFromYearString(new String((byte[]) value), zeroDatetimeBehavior);
            case DATE:
                return DateTimeUtil.getLocalDateTimeFromDateString(new String((byte[]) value), zeroDatetimeBehavior);
            case TIME:
                return DateTimeUtil.getLocalDateTimeFromTimeString(new String((byte[]) value), zeroDatetimeBehavior);
            case DATETIME:
            case TIMESTAMP:
                return DateTimeUtil.getLocalDateTimeFromTimestampString(new String((byte[]) value), zeroDatetimeBehavior);
            default:
                throw new SQLDataException("Unsupported conversion from " + vtResultValue.getVtType() + " to java.time.LocalDateTime");
        }
    }

    private static Object convertToLocalDate(final VtResultValue vtResultValue, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        Object value = vtResultValue.getValue();
        switch (vtResultValue.getVtType()) {
            case YEAR:
                return DateTimeUtil.getLocalDateFromYearString(new String((byte[]) value), zeroDatetimeBehavior);
            case DATE:
                return DateTimeUtil.getLocalDateFromDateString(new String((byte[]) value), zeroDatetimeBehavior);
            case DATETIME:
            case TIMESTAMP:
                return DateTimeUtil.getLocalDateFromTimestampString(new String((byte[]) value), zeroDatetimeBehavior);
            default:
                throw new SQLDataException("Unsupported conversion from " + vtResultValue.getVtType() + " to java.time.LocalDate");
        }
    }

    private static Object convertToLocalTime(final VtResultValue vtResultValue, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        Object value = vtResultValue.getValue();
        switch (vtResultValue.getVtType()) {
            case TIME:
                return DateTimeUtil.getLocalTimeFromTimeString(new String((byte[]) value), zeroDatetimeBehavior);
            case DATETIME:
            case TIMESTAMP:
                return DateTimeUtil.getLocalTimeFromTimestampString(new String((byte[]) value), zeroDatetimeBehavior);
            default:
                throw new SQLDataException("Unsupported conversion from " + vtResultValue.getVtType() + " to java.time.LocalTime");
        }
    }

    protected static Object convertNullValue(final Class<?> convertType) {
        switch (convertType.getName()) {
            case "boolean":
                return false;
            case "byte":
                return (byte) 0;
            case "short":
                return (short) 0;
            case "int":
                return 0;
            case "long":
                return 0L;
            case "float":
                return 0.0F;
            case "double":
                return 0.0D;
            default:
                return null;
        }
    }

    protected static Object convertNumberValue(final Object value, final Class<?> convertType) {
        Number number = (Number) value;

        if (Boolean.class.equals(convertType) || boolean.class.equals(convertType)) {
            long l = number.longValue();
            return l == -1L || l > 0L;
        }
        if (Byte.class.equals(convertType) || byte.class.equals(convertType)) {
            return number.byteValue();
        }
        if (Short.class.equals(convertType) || short.class.equals(convertType)) {
            return number.shortValue();
        }
        if (Integer.class.equals(convertType) || int.class.equals(convertType)) {
            return number.intValue();
        }
        if (Long.class.equals(convertType) || long.class.equals(convertType)) {
            return number.longValue();
        }
        if (Float.class.equals(convertType) || float.class.equals(convertType)) {
            return number.floatValue();
        }
        if (Double.class.equals(convertType) || double.class.equals(convertType)) {
            return number.doubleValue();
        }
        if (BigDecimal.class.equals(convertType)) {
            return new BigDecimal(number.toString());
        }
        if (BigInteger.class.equals(convertType)) {
            return new BigInteger(number.toString());
        }
        if (String.class.equals(convertType)) {
            return value.toString();
        }
        if (Object.class.equals(convertType)) {
            return value;
        }
        if (byte[].class.equals(convertType)) {
            if (value instanceof Long) {
                return Longs.toByteArray((Long) value);
            }
            if (value instanceof Integer) {
                return Ints.toByteArray((Integer) value);
            }
            if (value instanceof Short) {
                return Shorts.toByteArray((Short) value);
            }
            throw new RuntimeException("Unsupported data type");
        }
        throw new RuntimeException("Unsupported data type");
    }

    protected static Object convertByteArrayValue(final Object value, final Class<?> convertType) {
        byte[] bytesValue = (byte[]) value;
        switch (bytesValue.length) {
            case 1:
                return convertNumberValue(bytesValue[0], convertType);
            case Shorts.BYTES:
                return convertNumberValue(Shorts.fromByteArray(bytesValue), convertType);
            case Ints.BYTES:
                return convertNumberValue(Ints.fromByteArray(bytesValue), convertType);
            case Longs.BYTES:
                return convertNumberValue(Longs.fromByteArray(bytesValue), convertType);
            default:
                return value;
        }
    }
}
