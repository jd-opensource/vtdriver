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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;

public final class MetaDataResultSetUtil {
    private MetaDataResultSetUtil() {
    }

    public static Object convertValue(final Object value, final Class<?> convertType) {
        if (null == value) {
            return convertNullValue(convertType);
        }
        if (value.getClass() == convertType) {
            return value;
        }
        if (LocalDateTime.class.equals(convertType)) {
            return convertLocalDateTimeValue(value);
        }
        if (LocalDate.class.equals(convertType)) {
            return convertLocalDateValue(value);
        }
        if (LocalTime.class.equals(convertType)) {
            return convertLocalTimeValue(value);
        }
        if (URL.class.equals(convertType)) {
            return convertURL(value);
        }
        if (value instanceof Number) {
            return convertNumberValue(value, convertType);
        }
        if (value instanceof Date) {
            return convertDateValue(value, convertType);
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

    private static Object convertURL(final Object value) {
        String val = value.toString();
        try {
            return new URL(val);
        } catch (final MalformedURLException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    public static Object convertBigDecimalValue(final Object value, final boolean needScale, final int scale) {
        if (null == value) {
            return convertNullValue(BigDecimal.class);
        }
        if (value.getClass() == BigDecimal.class) {
            return adjustBigDecimalResult((BigDecimal) value, needScale, scale);
        }
        if (value instanceof Number || value instanceof String) {
            BigDecimal bigDecimal = new BigDecimal(value.toString());
            return adjustBigDecimalResult(bigDecimal, needScale, scale);
        }
        throw new RuntimeException("Unsupported Date type");
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

    private static Object convertLocalDateTimeValue(final Object value) {
        Timestamp timestamp = (Timestamp) value;
        return timestamp.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

    private static Object convertLocalDateValue(final Object value) {
        Timestamp timestamp = (Timestamp) value;
        return timestamp.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
    }

    private static Object convertLocalTimeValue(final Object value) {
        Timestamp timestamp = (Timestamp) value;
        return timestamp.toInstant().atZone(ZoneId.systemDefault()).toLocalTime();
    }

    private static Object convertNullValue(final Class<?> convertType) {
        return ResultSetUtil.convertNullValue(convertType);
    }

    private static Object convertNumberValue(final Object value, final Class<?> convertType) {
        return ResultSetUtil.convertNumberValue(value, convertType);
    }

    private static Object convertDateValue(final Object value, final Class<?> convertType) {
        Date date = (Date) value;
        switch (convertType.getName()) {
            case "java.sql.Date":
                return new java.sql.Date(date.getTime());
            case "java.sql.Time":
                return new Time(date.getTime());
            case "java.sql.Timestamp":
                return new Timestamp(date.getTime());
            case "java.lang.String":
                return date.toString();
            default:
                throw new RuntimeException("Unsupported Date type");
        }
    }

    private static Object convertByteArrayValue(final Object value, final Class<?> convertType) {
        return ResultSetUtil.convertByteArrayValue(value, convertType);
    }
}

