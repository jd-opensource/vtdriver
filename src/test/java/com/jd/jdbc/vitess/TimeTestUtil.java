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

package com.jd.jdbc.vitess;

import com.jd.jdbc.vitess.resultset.DateTimeUtil;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;

public class TimeTestUtil {
    private static final TimeZone TZ;

    static {
        String[] allTimeZone = TimeZone.getAvailableIDs();
        int randomIndex = RandomUtils.nextInt(0, allTimeZone.length);

        while (allTimeZone[randomIndex].startsWith("Etc/GMT+") || allTimeZone[randomIndex].equals("America/Hermosillo")) {
            randomIndex = RandomUtils.nextInt(0, allTimeZone.length);
        }

        TZ = TimeZone.getTimeZone(allTimeZone[randomIndex]);
    }

    public static TimeZone getRandomTimeZone() {
        return TZ;
    }

    public static String getRandomDateString() {
        long min = Date.valueOf("1000-01-01").getTime();
        long max = Date.valueOf("9999-12-31").getTime();
        long random = RandomUtils.nextLong(0, max - min) + min;
        return (new Date(random)).toString();
    }

    public static String getRandomYearString() {
        int random = RandomUtils.nextInt(1901, 2156);
        return String.valueOf(random);
    }

    // 对应mysql TIME(x) 类型 -838:00:00.x ~ 838:59:59.x
    public static String getRandomTimeString(final int p, final boolean normalRange) {
        int h;
        if (normalRange) {
            h = RandomUtils.nextInt(0, 24);
        } else {
            h = RandomUtils.nextInt(0, 839);
            if (RandomUtils.nextBoolean()) {
                h = -h;
            }
        }

        int m = RandomUtils.nextInt(0, 60);
        int s = RandomUtils.nextInt(0, 60);
        if (p == 0) {
            return String.format("%02d:%02d:%02d", h, m, s);
        }

        int f = RandomUtils.nextInt(0, (int) Math.pow(10, p));
        return String.format("%02d:%02d:%02d.%0" + p + "d", h, m, s, f);
    }

    // 1000-01-01 ~ 9999-12-31
    public static String getRandomDateTimeString(final int p) {
        Date date = getRandomDate();
        Time time = getRandomTime();
        String datetime = date + " " + time;

        if (p == 0) {
            return datetime;
        }

        int f = RandomUtils.nextInt(0, (int) Math.pow(10, p));
        return String.format("%s %s.%0" + p + "d", date.toString(), time, f);
    }

    // mysql Timestamp范围 '1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC
    // getRandomTimestampString 返回 1970-01-01 00:00:01.f ~ 2038-01-18 23:59:59.f
    public static String getRandomTimestampString(final int p) {
        long min = Date.valueOf("1970-01-01").getTime();
        long max = Date.valueOf("2038-01-18").getTime();
        long random = RandomUtils.nextLong(0, max - min) + min;
        Date date = Date.valueOf(new Date(random).toString());

        int h = RandomUtils.nextInt(0, 24);
        int m = RandomUtils.nextInt(0, 60);
        int s = RandomUtils.nextInt(0, 60);
        // 1970-01-01时, 秒不能是1
        if (date.getTime() == 0) {
            s = RandomUtils.nextInt(1, 60);
        }

        Time time = Time.valueOf(String.format("%02d:%02d:%02d", h, m, s));

        String datetime = date + " " + time;
        if (p == 0) {
            return datetime;
        }

        int f = RandomUtils.nextInt(0, (int) Math.pow(10, p));
        return String.format("%s.%0" + p + "d", datetime, f);
    }

    public static Date getRandomDate() {
        long min = Date.valueOf("1000-01-01").getTime();
        long max = Date.valueOf("9999-12-31").getTime();
        long random = RandomUtils.nextLong(0, max - min) + min;
        return Date.valueOf(new Date(random).toString());
    }

    public static Date getRandomYear() {
        int random = RandomUtils.nextInt(1901, 2156);
        return Date.valueOf(random + "-01-01");
    }

    // 对应java.sql.Time类型 00:00:00 ~ 23:59:59
    public static Time getRandomTime() {
        int h = RandomUtils.nextInt(0, 24);
        int m = RandomUtils.nextInt(0, 60);
        int s = RandomUtils.nextInt(0, 60);
        return Time.valueOf(String.format("%02d:%02d:%02d", h, m, s));
    }

    // 1000-01-01 ~ 9999-12-31
    public static Timestamp getRandomDateTime(final int p) {
        return Timestamp.valueOf(getRandomDateTimeString(p));
    }

    // 1970-01-01 ~ 2038-01-18
    public static Timestamp getRandomTimestamp(final int p) {
        return Timestamp.valueOf(getRandomTimestampString(p));
    }

    public static Date getDateFromMysqlDate(final String date, final TimeZone tz) {
        Assert.assertNotNull(tz);
        DateTimeUtil.InternalDate idate = DateTimeUtil.InternalDate.parseInternalDate(date);
        Calendar cal = Calendar.getInstance(tz, Locale.US);
        cal.set(idate.getYear(), idate.getMonth() - 1, idate.getDay(), 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);

        return new Date(cal.getTimeInMillis());
    }

    public static Date getDateFromMysqlDateUseDefaultTimeZone(final String date) {
        return getDateFromMysqlDate(date, TimeZone.getDefault());
    }

    public static Date getDateFromMysqlTimestamp(final String timestamp, final TimeZone tz) {
        Assert.assertNotNull(tz);
        DateTimeUtil.InternalTimestamp its = DateTimeUtil.InternalTimestamp.parseInternalTimestamp(timestamp);
        Calendar cal = Calendar.getInstance(tz, Locale.US);
        cal.set(its.getYear(), its.getMonth() - 1, its.getDay(), 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Date(cal.getTimeInMillis());
    }

    public static Date getDateFromMysqlTimestampUseDefaultTimeZone(final String timestamp) {
        return getDateFromMysqlTimestamp(timestamp, TimeZone.getDefault());
    }

    public static Date getDateFromMysqlDatetime(final String datetime, final TimeZone tz) {
        return getDateFromMysqlTimestamp(datetime, tz);
    }

    public static Date getDateFromMysqlYearUseDefaultTimeZone(final String year) {
        return getDateFromMysqlDate(year + "-01-01", TimeZone.getDefault());
    }

    public static Date getDateFromMysqlDatetimeUseDefaultTimeZone(final String datetime) {
        return getDateFromMysqlDatetime(datetime, TimeZone.getDefault());
    }

    public static Date getDateFromMysqlTime(final String time, final TimeZone tz) throws SQLException {
        Assert.assertNotNull(tz);
        DateTimeUtil.InternalTime it = DateTimeUtil.InternalTime.parseInternalTime(time);
        Calendar cal = Calendar.getInstance(tz, Locale.US);
        cal.set(1970, Calendar.JANUARY, 1, it.getHour(), it.getMinute(), it.getSecond());
        cal.set(Calendar.MILLISECOND, it.getNano() / 1000_000);
        return new Date(cal.getTimeInMillis());
    }

    public static Date getDateFromMysqlTimeUseUTCTimeZone(final String time) throws SQLException {
        return getDateFromMysqlTime(time, TimeZone.getTimeZone("UTC"));
    }

    public static Time getTimeFromMysqlTime(final String time, final TimeZone tz) throws SQLException {
        Assert.assertNotNull(tz);
        DateTimeUtil.InternalTime it = DateTimeUtil.InternalTime.parseInternalTime(time);
        Calendar cal = Calendar.getInstance(tz, Locale.US);
        cal.set(1970, Calendar.JANUARY, 1, it.getHour(), it.getMinute(), it.getSecond());
        cal.set(Calendar.MILLISECOND, it.getNano() / 1000_000);
        return new Time(cal.getTimeInMillis());
    }

    public static Time getTimeFromMysqlTimeUseServerTimeZone(final String time, final TimeZone serverTimeZone) throws SQLException {
        return getTimeFromMysqlTime(time, serverTimeZone);
    }

    public static Time getTimeFromMysqlTimestamp(final String timestamp, final TimeZone tz) {
        Assert.assertNotNull(tz);
        DateTimeUtil.InternalTimestamp its = DateTimeUtil.InternalTimestamp.parseInternalTimestamp(timestamp);
        Calendar cal = Calendar.getInstance(tz, Locale.US);
        cal.set(1970, Calendar.JANUARY, 1, its.getHour(), its.getMinute(), its.getSecond());
        cal.set(Calendar.MILLISECOND, its.getNano() / 1000_000);
        return new Time(cal.getTimeInMillis());
    }

    public static Time getTimeFromMysqlTimestampUseServerTimeZone(final String timestamp, final TimeZone serverTimeZone) {
        return getTimeFromMysqlTimestamp(timestamp, serverTimeZone);
    }

    public static Time getTimeFromMysqlDatetime(final String datetime, final TimeZone tz) {
        return getTimeFromMysqlTimestamp(datetime, tz);
    }

    public static Time getTimeFromMysqlDatetimeUseServerTimeZone(final String datetime, final TimeZone serverTimeZone) {
        return getTimeFromMysqlDatetime(datetime, serverTimeZone);
    }

    public static Timestamp getTimestampFromMysqlTimestamp(final String timestamp, final TimeZone tz) {
        Assert.assertNotNull(tz);
        DateTimeUtil.InternalTimestamp its = DateTimeUtil.InternalTimestamp.parseInternalTimestamp(timestamp);
        Calendar cal = Calendar.getInstance(tz, Locale.US);
        cal.set(its.getYear(), its.getMonth() - 1, its.getDay(), its.getHour(), its.getMinute(), its.getSecond());
        cal.set(Calendar.MILLISECOND, 0);
        Timestamp ts = new Timestamp(cal.getTimeInMillis());
        ts.setNanos(its.getNano());
        return ts;
    }

    public static Timestamp getTimestampFromMysqlTimestampUseServerTimeZone(final String timestamp, final TimeZone serverTimeZone) {
        return getTimestampFromMysqlTimestamp(timestamp, serverTimeZone);
    }

    public static Timestamp getTimestampFromMysqlDatetime(final String datetime, final TimeZone tz) {
        return getTimestampFromMysqlTimestamp(datetime, tz);
    }

    public static Timestamp getTimestampFromMysqlDatetimeUseServerTimeZone(final String datetime, final TimeZone serverTimeZone) {
        return getTimestampFromMysqlDatetime(datetime, serverTimeZone);
    }

    public static Timestamp getTimestampFromMysqlDate(final String date, final TimeZone tz) {
        Assert.assertNotNull(tz);
        DateTimeUtil.InternalDate idate = DateTimeUtil.InternalDate.parseInternalDate(date);
        Calendar cal = Calendar.getInstance(tz, Locale.US);
        cal.set(idate.getYear(), idate.getMonth() - 1, idate.getDay(), 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Timestamp(cal.getTimeInMillis());
    }

    public static Timestamp getTimestampFromMysqlYearUseServerTimeZone(final String date, final TimeZone serverTimeZone) {
        return getTimestampFromMysqlDate(date + "-01-01", serverTimeZone);
    }

    public static Timestamp getTimestampFromMysqlDateUseServerTimeZone(final String date, final TimeZone serverTimeZone) {
        return getTimestampFromMysqlDate(date, serverTimeZone);
    }

    public static Timestamp getTimestampFromMysqlTime(final String time, final TimeZone tz) throws SQLException {
        Assert.assertNotNull(tz);
        DateTimeUtil.InternalTime it = DateTimeUtil.InternalTime.parseInternalTime(time);
        Calendar cal = Calendar.getInstance(tz, Locale.US);
        cal.set(1970, Calendar.JANUARY, 1, it.getHour(), it.getMinute(), it.getSecond());
        cal.set(Calendar.MILLISECOND, 0);
        Timestamp ts = new Timestamp(cal.getTimeInMillis());
        ts.setNanos(it.getNano());
        return ts;
    }

    public static Timestamp getTimestampFromMysqlTimeUseServerTimeZone(final String time, final TimeZone serverTimeZone) throws SQLException {
        return getTimestampFromMysqlTime(time, serverTimeZone);
    }

    public static void printAll(final Object... a) {
        for (Object o : a) {
            System.out.println(o);
        }
        System.out.println();
    }

    public static String removeTrailingZeros(final String timeString) {
        int idx = timeString.indexOf(".");
        if (idx != -1 && Integer.parseInt(timeString.substring(idx + 1)) == 0) {
            return timeString.substring(0, idx);
        }
        return timeString;
    }
}
