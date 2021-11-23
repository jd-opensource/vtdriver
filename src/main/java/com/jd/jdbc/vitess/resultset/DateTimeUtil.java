/*
Copyright 2021 JD Project Authors.

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

package com.jd.jdbc.vitess.resultset;

import com.mysql.cj.conf.PropertyDefinitions;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;
import lombok.AllArgsConstructor;
import lombok.Getter;

public final class DateTimeUtil {
    private DateTimeUtil() {
    }

    public static Date getDateFromYearString(final String timeStr, final TimeZone srcTz, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        return getDateFromDateString(timeStr + "-01-01", srcTz, zeroDatetimeBehavior);
    }

    public static Date getDateFromDateString(final String timestr, final TimeZone srcTz, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        InternalDate id = InternalDate.parseInternalDate(timestr);
        if (id.isZero()) {
            switch (zeroDatetimeBehavior) {
                case CONVERT_TO_NULL:
                    return null;
                case ROUND:
                    return java.sql.Date.valueOf("0001-01-01");
                case EXCEPTION:
                default:
                    throw new SQLException("Zero date value prohibited");
            }
        }
        Calendar cal = Calendar.getInstance(srcTz, Locale.US);
        cal.set(id.getYear(), id.getMonth() - 1, id.getDay(), 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Date(cal.getTimeInMillis());
    }

    public static Date getDateFromTimestampString(final String timestr, final TimeZone srcTz, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        InternalTimestamp its = InternalTimestamp.parseInternalTimestamp(timestr);
        if (its.isZero()) {
            switch (zeroDatetimeBehavior) {
                case CONVERT_TO_NULL:
                    return null;
                case ROUND:
                    return Date.valueOf("0001-01-01");
                case EXCEPTION:
                default:
                    throw new SQLException("Zero date value prohibited");
            }
        }

        int mill = 0;
        Calendar cal = Calendar.getInstance(srcTz, Locale.US);
        cal.set(Calendar.MILLISECOND, mill);
        // month = 0 是1月
        cal.set(its.getYear(), its.getMonth() - 1, its.getDay(), 0, 0, 0);
        return new Date(cal.getTimeInMillis());
    }

    // 从指定时区的Time String转换为本地时区的Time对象
    public static Time getTimeFromTimeString(final String timestr, final TimeZone srcTz) throws SQLException {
        InternalTime it = InternalTime.parseInternalTime(timestr);

        int mill = it.getNano() / 1000_000;
        Calendar cal = Calendar.getInstance(srcTz, Locale.US);
        cal.set(Calendar.MILLISECOND, mill);
        cal.set(1970, Calendar.JANUARY, 1, it.getHour(), it.getMinute(), it.getSecond());
        return new Time(cal.getTimeInMillis());
    }

    // 从字符串timestamp(时区为srcTz), 获得本地时区的Time对象
    public static Time getTimeFromTimestampString(final String timestamp, final TimeZone srcTz, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) {
        InternalTimestamp its = InternalTimestamp.parseInternalTimestamp(timestamp);
        if (its.isZero()) {
            switch (zeroDatetimeBehavior) {
                case CONVERT_TO_NULL:
                    return null;
                case ROUND:
                case EXCEPTION:
                default:
                    // 返回本地时间 0001-01-01 00:00:00
                    return Time.valueOf("00:00:00");
            }
        }

        int mill = its.getNano() / 1000_000;
        Calendar cal = Calendar.getInstance(srcTz, Locale.US);
        cal.set(Calendar.MILLISECOND, mill);
        cal.set(1970, Calendar.JANUARY, 1, its.getHour(), its.getMinute(), its.getSecond());
        return new Time(cal.getTimeInMillis());
    }

    // 从字符串timestamp(时区为srcTz), 获得本地时区的Timestamp对象
    public static Timestamp getTimestampFromTimestampString(final String timestamp, final TimeZone srcTz, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior)
        throws SQLException {
        InternalTimestamp its = InternalTimestamp.parseInternalTimestamp(timestamp);
        if (its.isZero()) {
            switch (zeroDatetimeBehavior) {
                case CONVERT_TO_NULL:
                    return null;
                case ROUND:
                    // 返回本地时间 0001-01-01 00:00:00
                    return Timestamp.valueOf("0001-01-01 00:00:00");
                case EXCEPTION:
                default:
                    throw new SQLException("Zero date value prohibited");
            }
        }

        Calendar cal = Calendar.getInstance(srcTz, Locale.US);
        cal.set(Calendar.MILLISECOND, 0);
        // month = 0 是1月
        cal.set(its.getYear(), its.getMonth() - 1, its.getDay(), its.getHour(), its.getMinute(), its.getSecond());
        Timestamp ts = new Timestamp(cal.getTimeInMillis());
        ts.setNanos(its.getNano());
        return ts;
    }

    public static Timestamp getTimeStampFromTimeString(final String time, final TimeZone srcTz, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        InternalTime it = InternalTime.parseInternalTime(time);

        Calendar cal = Calendar.getInstance(srcTz, Locale.US);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(1970, Calendar.JANUARY, 1, it.getHour(), it.getMinute(), it.getSecond());
        Timestamp ts = new Timestamp(cal.getTimeInMillis());
        ts.setNanos(it.getNano());
        return ts;
    }

    public static Timestamp getTimestampFromYearString(final String date, final TimeZone srcTz, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        return getTimestampFromDateString(date + "-01-01", srcTz, zeroDatetimeBehavior);
    }

    public static Timestamp getTimestampFromDateString(final String date, final TimeZone srcTz, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        InternalDate id = InternalDate.parseInternalDate(date);
        if (id.isZero()) {
            switch (zeroDatetimeBehavior) {
                case CONVERT_TO_NULL:
                    return null;
                case ROUND:
                    // 返回本地时间 0001-01-01 00:00:00
                    return Timestamp.valueOf("0001-01-01 00:00:00");
                case EXCEPTION:
                default:
                    throw new SQLException("Zero date value prohibited");
            }
        }

        Calendar cal = Calendar.getInstance(srcTz, Locale.US);
        cal.set(Calendar.MILLISECOND, 0);
        // month = 0 是1月
        cal.set(id.getYear(), id.getMonth() - 1, id.getDay(), 0, 0, 0);
        Timestamp ts = new Timestamp(cal.getTimeInMillis());
        return ts;
    }

    public static LocalDateTime getLocalDateTimeFromYearString(final String year, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        return getLocalDateTimeFromDateString(year + "-01-01", zeroDatetimeBehavior);
    }

    public static LocalDateTime getLocalDateTimeFromDateString(final String date, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        InternalDate id = InternalDate.parseInternalDate(date);
        if (id.isZero()) {
            switch (zeroDatetimeBehavior) {
                case CONVERT_TO_NULL:
                    return null;
                case ROUND:
                    return LocalDateTime.of(1, 1, 1, 0, 0, 0);
                case EXCEPTION:
                default:
                    throw new SQLException("Zero date value prohibited");
            }
        }
        return LocalDateTime.of(id.year, id.month, id.day, 0, 0, 0);
    }

    public static LocalDateTime getLocalDateTimeFromTimeString(final String time, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        InternalTime it = InternalTime.parseInternalTime(time);
        return LocalDateTime.of(1970, 1, 1, it.hour, it.minute, it.second, it.nano);
    }

    public static LocalDateTime getLocalDateTimeFromTimestampString(final String timestamp, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        InternalTimestamp its = InternalTimestamp.parseInternalTimestamp(timestamp);
        if (its.isZero()) {
            switch (zeroDatetimeBehavior) {
                case CONVERT_TO_NULL:
                    return null;
                case ROUND:
                    return LocalDateTime.of(1, 1, 1, 0, 0, 0);
                case EXCEPTION:
                default:
                    throw new SQLException("Zero date value prohibited");
            }
        }
        return LocalDateTime.of(its.year, its.month, its.day, its.hour, its.minute, its.second, its.nano);
    }

    public static LocalDate getLocalDateFromYearString(final String year, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        return getLocalDateFromDateString(year + "-01-01", zeroDatetimeBehavior);
    }

    public static LocalDate getLocalDateFromDateString(final String date, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        InternalDate id = InternalDate.parseInternalDate(date);
        if (id.isZero()) {
            switch (zeroDatetimeBehavior) {
                case CONVERT_TO_NULL:
                    return null;
                case ROUND:
                    return LocalDate.of(1, 1, 1);
                case EXCEPTION:
                default:
                    throw new SQLException("Zero date value prohibited");
            }
        }
        return LocalDate.of(id.year, id.month, id.day);
    }

    public static LocalDate getLocalDateFromTimestampString(final String timestamp, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        InternalTimestamp its = InternalTimestamp.parseInternalTimestamp(timestamp);
        if (its.isZero()) {
            switch (zeroDatetimeBehavior) {
                case CONVERT_TO_NULL:
                    return null;
                case ROUND:
                    return LocalDate.of(1, 1, 1);
                case EXCEPTION:
                default:
                    throw new SQLException("Zero date value prohibited");
            }
        }
        return LocalDate.of(its.year, its.month, its.day);
    }

    public static LocalTime getLocalTimeFromTimeString(final String time, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        InternalTime it = InternalTime.parseInternalTime(time);
        return LocalTime.of(it.hour, it.minute, it.second, it.nano);
    }

    public static LocalTime getLocalTimeFromTimestampString(final String timestamp, final PropertyDefinitions.ZeroDatetimeBehavior zeroDatetimeBehavior) throws SQLException {
        InternalTimestamp its = InternalTimestamp.parseInternalTimestamp(timestamp);
        return LocalTime.of(its.hour, its.minute, its.second, its.nano);
    }

    // 不带时区的Timestamp
    @Getter
    @AllArgsConstructor
    public static class InternalTimestamp {
        private final int year;

        // 1月 month = 1
        private final int month;

        private final int day;

        private final int hour;

        private final int minute;

        private final int second;

        private final int nano;

        // yyyy-MM-dd HH:mm:ss(.fffffffff)
        public static InternalTimestamp parseInternalTimestamp(final String timestamp) {
            int year = Integer.parseInt(timestamp.substring(0, 4));
            int month = Integer.parseInt(timestamp.substring(5, 7));
            int day = Integer.parseInt(timestamp.substring(8, 10));
            int hour = Integer.parseInt(timestamp.substring(11, 13));
            int minute = Integer.parseInt(timestamp.substring(14, 16));
            int second = Integer.parseInt(timestamp.substring(17, 19));
            int nano;
            if (timestamp.length() > 19) {
                StringBuilder nanoStr = new StringBuilder(timestamp.substring(20));
                while (nanoStr.length() < 9) {
                    nanoStr.append("0");
                }
                nano = Integer.parseInt(nanoStr.toString());
            } else {
                nano = 0;
            }

            return new InternalTimestamp(year, month, day, hour, minute, second, nano);
        }

        public boolean isZero() {
            return year == 0 && month == 0 && day == 0 && hour == 0 && minute == 0 && second == 0 && nano == 0;
        }
    }

    // 不带时区的Date
    @Getter
    @AllArgsConstructor
    public static class InternalDate {
        private final int year;

        // 1月 month = 1
        private final int month;

        private final int day;

        // yyyy-MM-dd
        public static InternalDate parseInternalDate(final String date) {
            int year = Integer.parseInt(date.substring(0, 4));
            int month = Integer.parseInt(date.substring(5, 7));
            int day = Integer.parseInt(date.substring(8, 10));
            return new InternalDate(year, month, day);
        }

        public boolean isZero() {
            return year == 0 && month == 0 && day == 0;
        }
    }

    // 不带时区的Time
    @Getter
    @AllArgsConstructor
    public static class InternalTime {
        private final int hour;

        private final int minute;

        private final int second;

        private final int nano;

        // hh:mm:ss(.fffffffff)
        public static InternalTime parseInternalTime(final String time) throws SQLException {
            // mysql TIME(x) 类型 -838:00:00.x ~ 838:59:59.x
            // java.sql.Time 类型 00:00:00.000 ~ 23:59:59.999
            if (time.startsWith("-") || !Objects.equals(time.charAt(2), ':')) {
                throw new SQLException("The value '" + time + "' is an invalid TIME value. "
                    + "JDBC Time objects represent a wall-clock time and not a duration as MySQL treats them. "
                    + "If you are treating this type as a duration, consider retrieving this value as a string and dealing with it according to your requirements.");
            }

            int hour = Integer.parseInt(time.substring(0, 2));
            int minute = Integer.parseInt(time.substring(3, 5));
            int second = Integer.parseInt(time.substring(6, 8));
            int nano;
            if (time.length() > 8) {
                StringBuilder nanoStr = new StringBuilder(time.substring(9));
                while (nanoStr.length() < 9) {
                    nanoStr.append("0");
                }
                nano = Integer.parseInt(nanoStr.toString());
            } else {
                nano = 0;
            }

            return new InternalTime(hour, minute, second, nano);
        }

        public boolean isZero() {
            return hour == 0 && minute == 0 && second == 0 && nano == 0;
        }
    }
}
