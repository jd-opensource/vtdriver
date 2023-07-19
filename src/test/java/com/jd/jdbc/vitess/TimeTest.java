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

import com.jd.jdbc.util.InnerConnectionPoolUtil;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import testsuite.TestSuite;

public class TimeTest extends TestSuite {
    private static final int TEST_SIZE = 10;

    private static final String TABLE_NAME = "time_test";

    protected static TimeZone serverTimezone;

    protected Connection conn;

    @BeforeClass
    public static void beforeClass() throws NoSuchFieldException, IllegalAccessException {
        InnerConnectionPoolUtil.clearAll();
    }

    private static void checkResult(final GetTimeObjectFunction f1, final GetTimeObjectFunction f2) {
        Object ret1 = null;
        Object ret2 = null;
        Exception e1 = null;
        Exception e2 = null;

        try {
            ret1 = f1.exec();
        } catch (Exception e) {
            e1 = e;
        }

        try {
            ret2 = f2.exec();
        } catch (Exception e) {
            e2 = e;
        }

        Assert.assertEquals("expected: " + ret1 + ", actual: " + ret2, ret1, ret2);
        Assert.assertTrue(e1 == null && e2 == null || e1 != null && e2 != null);
        if (e1 != null) {
            Assert.assertSame(e1.getClass(), e2.getClass());
            Assert.assertEquals(e1.getMessage(), e2.getMessage());
        }
    }

    @Before
    public void init() throws SQLException {
        serverTimezone = TimeZone.getTimeZone("UTC");
        String url = getConnectionUrl(Driver.of(testsuite.internal.TestSuiteShardSpec.TWO_SHARDS));
        conn = DriverManager.getConnection(url);
        Assert.assertEquals(serverTimezone.getID(), ((VitessConnection) conn).getProperties().getProperty("serverTimezone"));
    }

    @After
    public void clean() throws SQLException, NoSuchFieldException, IllegalAccessException {
        if (null != conn) {
            InnerConnectionPoolUtil.removeInnerConnectionConfig(conn);
            conn.close();
        }
    }

    private List<TestCase> initTestCase(final int size, final String mysqlType, final String fieldName, final int p) throws SQLException {
        List<TestCase> testCaseList = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            String timeString;
            switch (mysqlType) {
                case "YEAR":
                    timeString = TimeTestUtil.getRandomYearString();
                    break;
                case "DATE":
                    timeString = TimeTestUtil.getRandomDateString();
                    break;
                case "TIME":
                    timeString = TimeTestUtil.getRandomTimeString(p, true);
                    break;
                case "DATETIME":
                    timeString = TimeTestUtil.getRandomDateTimeString(p);
                    break;
                case "TIMESTAMP":
                    timeString = TimeTestUtil.getRandomTimestampString(p);
                    break;
                default:
                    throw new SQLException("unknown type");
            }

            TestCase tc;
            String[] initSQL;
            initSQL = new String[] {
                "delete from " + TABLE_NAME,
                "insert into " + TABLE_NAME + " (" + fieldName + ") values ('" + timeString + "')",
            };
            String query = "select " + fieldName + " from " + TABLE_NAME;
            tc = new TestCase(initSQL, query, timeString, mysqlType);

            testCaseList.add(tc);
        }
        return testCaseList;
    }

    private void selectTest(final List<TestCase> testcases, final Calendar cal) throws SQLException {
        int i = 1;
        for (TestCase testcase : testcases) {
            testcase.printAll(i++);
            try (Statement stmt = conn.createStatement()) {
                for (String sql : testcase.getInitSQL()) {
                    stmt.execute(sql);
                }

                String timeString = testcase.getTimeString();
                ResultSet rs = stmt.executeQuery(testcase.getQuery());
                Assert.assertTrue(rs.next());
                switch (testcase.getMysqlType()) {
                    case "YEAR":
                        Assert.assertEquals(timeString + "-01-01", rs.getString(1));
                        Assert.assertEquals(rs.getDate(1), rs.getObject(1));
                        if (null == cal) {
                            Assert.assertEquals(TimeTestUtil.getDateFromMysqlYearUseDefaultTimeZone(timeString), rs.getDate(1));
                            Assert.assertEquals(TimeTestUtil.getDateFromMysqlYearUseDefaultTimeZone(timeString), rs.getObject(1, java.sql.Date.class));
                            Assert.assertEquals(TimeTestUtil.getTimestampFromMysqlYearUseServerTimeZone(timeString, serverTimezone), rs.getTimestamp(1));
                        } else {
                            Assert.assertEquals(TimeTestUtil.getDateFromMysqlDate(timeString, cal.getTimeZone()), rs.getDate(1, cal));
                            Assert.assertEquals(TimeTestUtil.getTimestampFromMysqlDate(timeString, cal.getTimeZone()), rs.getTimestamp(1, cal));
                        }
                        break;
                    case "DATE":
                        Assert.assertEquals(timeString, rs.getString(1));
                        Assert.assertEquals(rs.getDate(1), rs.getObject(1));
                        if (null == cal) {
                            Assert.assertEquals(TimeTestUtil.getDateFromMysqlDateUseDefaultTimeZone(timeString), rs.getDate(1));
                            Assert.assertEquals(TimeTestUtil.getDateFromMysqlDateUseDefaultTimeZone(timeString), rs.getObject(1, java.sql.Date.class));
                            Assert.assertEquals(TimeTestUtil.getTimestampFromMysqlDateUseServerTimeZone(timeString, serverTimezone), rs.getTimestamp(1));
                        } else {
                            Assert.assertEquals(TimeTestUtil.getDateFromMysqlDate(timeString, cal.getTimeZone()), rs.getDate(1, cal));
                            Assert.assertEquals(TimeTestUtil.getTimestampFromMysqlDate(timeString, cal.getTimeZone()), rs.getTimestamp(1, cal));
                        }
                        break;
                    case "TIME":
                        Assert.assertEquals(TimeTestUtil.removeTrailingZeros(timeString), rs.getString(1));
                        checkResult(() -> rs.getObject(1), () -> rs.getTime(1));
                        if (null == cal) {
                            checkResult(() -> TimeTestUtil.getTimeFromMysqlTimeUseServerTimeZone(timeString, serverTimezone), () -> rs.getTime(1));
                            checkResult(() -> TimeTestUtil.getTimeFromMysqlTimeUseServerTimeZone(timeString, serverTimezone), () -> rs.getObject(1, java.sql.Time.class));
                            checkResult(() -> TimeTestUtil.getDateFromMysqlTimeUseUTCTimeZone(timeString), () -> rs.getDate(1));
                            checkResult(() -> TimeTestUtil.getTimestampFromMysqlTimeUseServerTimeZone(timeString, serverTimezone), () -> rs.getTimestamp(1));
                        } else {
                            checkResult(() -> TimeTestUtil.getTimeFromMysqlTime(timeString, cal.getTimeZone()), () -> rs.getTime(1, cal));
                            checkResult(() -> TimeTestUtil.getDateFromMysqlTime(timeString, cal.getTimeZone()), () -> rs.getDate(1, cal));
                            checkResult(() -> TimeTestUtil.getTimestampFromMysqlTime(timeString, cal.getTimeZone()), () -> rs.getTimestamp(1, cal));
                        }
                        break;
                    case "DATETIME":
                        Assert.assertEquals(TimeTestUtil.removeTrailingZeros(timeString), rs.getString(1));
                        Assert.assertEquals(rs.getTimestamp(1), rs.getObject(1));
                        if (null == cal) {
                            Assert.assertEquals(TimeTestUtil.getTimeFromMysqlDatetimeUseServerTimeZone(timeString, serverTimezone), rs.getTime(1));
                            Assert.assertEquals(TimeTestUtil.getDateFromMysqlDatetimeUseDefaultTimeZone(timeString), rs.getDate(1));
                            Assert.assertEquals(TimeTestUtil.getTimestampFromMysqlDatetimeUseServerTimeZone(timeString, serverTimezone), rs.getTimestamp(1));
                            Assert.assertEquals(TimeTestUtil.getTimestampFromMysqlDatetimeUseServerTimeZone(timeString, serverTimezone), rs.getObject(1, java.sql.Timestamp.class));
                        } else {
                            Assert.assertEquals(TimeTestUtil.getTimeFromMysqlDatetime(timeString, cal.getTimeZone()), rs.getTime(1, cal));
                            Assert.assertEquals(TimeTestUtil.getDateFromMysqlDatetime(timeString, cal.getTimeZone()), rs.getDate(1, cal));
                            Assert.assertEquals(TimeTestUtil.getTimestampFromMysqlDatetime(timeString, cal.getTimeZone()), rs.getTimestamp(1, cal));
                        }
                        break;
                    case "TIMESTAMP":
                        Assert.assertEquals(TimeTestUtil.removeTrailingZeros(timeString), rs.getString(1));
                        Assert.assertEquals(rs.getTimestamp(1), rs.getObject(1));
                        if (null == cal) {
                            Assert.assertEquals(TimeTestUtil.getTimeFromMysqlTimestampUseServerTimeZone(timeString, serverTimezone), rs.getTime(1));
                            Assert.assertEquals(TimeTestUtil.getDateFromMysqlTimestampUseDefaultTimeZone(timeString), rs.getDate(1));
                            Assert.assertEquals(TimeTestUtil.getTimestampFromMysqlTimestampUseServerTimeZone(timeString, serverTimezone), rs.getTimestamp(1));
                            Assert.assertEquals(TimeTestUtil.getTimestampFromMysqlTimestampUseServerTimeZone(timeString, serverTimezone), rs.getObject(1, java.sql.Timestamp.class));
                        } else {
                            Assert.assertEquals(TimeTestUtil.getTimeFromMysqlTimestamp(timeString, cal.getTimeZone()), rs.getTime(1, cal));
                            Assert.assertEquals(TimeTestUtil.getDateFromMysqlTimestamp(timeString, cal.getTimeZone()), rs.getDate(1, cal));
                            Assert.assertEquals(TimeTestUtil.getTimestampFromMysqlTimestamp(timeString, cal.getTimeZone()), rs.getTimestamp(1, cal));
                        }
                        break;
                    default:
                        throw new SQLException("unknown type");
                }
                Assert.assertArrayEquals("expected: " + Arrays.toString(timeString.getBytes()) + ", actual: " + Arrays.toString(rs.getBytes(1)), timeString.getBytes(), rs.getBytes(1));
                Assert.assertFalse(rs.next());
                rs.close();
                printOk();
            }
        }
    }

    @Test
    public void yearTest() throws SQLException {
        List<TestCase> testCases = initTestCase(TEST_SIZE, "YEAR", "year", -1);
        selectTest(testCases, null);
    }

    @Test
    public void dateTest() throws SQLException {
        List<TestCase> testCases = initTestCase(TEST_SIZE, "DATE", "date", -1);
        selectTest(testCases, null);
    }

    @Test
    public void timeTest() throws SQLException {
        List<TestCase> testCases = initTestCase(TEST_SIZE, "TIME", "time0", 0);
        testCases.addAll(initTestCase(TEST_SIZE, "TIME", "time1", 1));
        testCases.addAll(initTestCase(TEST_SIZE, "TIME", "time2", 2));
        testCases.addAll(initTestCase(TEST_SIZE, "TIME", "time3", 3));
        testCases.addAll(initTestCase(TEST_SIZE, "TIME", "time4", 4));
        testCases.addAll(initTestCase(TEST_SIZE, "TIME", "time5", 5));
        testCases.addAll(initTestCase(TEST_SIZE, "TIME", "time6", 6));
        selectTest(testCases, null);
    }

    @Test
    public void datetimeTest() throws SQLException {
        List<TestCase> testCases = initTestCase(TEST_SIZE, "DATETIME", "datetime0", 0);
        testCases.addAll(initTestCase(TEST_SIZE, "DATETIME", "datetime1", 1));
        testCases.addAll(initTestCase(TEST_SIZE, "DATETIME", "datetime2", 2));
        testCases.addAll(initTestCase(TEST_SIZE, "DATETIME", "datetime3", 3));
        testCases.addAll(initTestCase(TEST_SIZE, "DATETIME", "datetime4", 4));
        testCases.addAll(initTestCase(TEST_SIZE, "DATETIME", "datetime5", 5));
        testCases.addAll(initTestCase(TEST_SIZE, "DATETIME", "datetime6", 6));
        selectTest(testCases, null);
    }

    @Test
    public void timestampTest() throws SQLException {
        List<TestCase> testCases = initTestCase(TEST_SIZE, "TIMESTAMP", "timestamp0", 0);
        testCases.addAll(initTestCase(TEST_SIZE, "TIMESTAMP", "timestamp1", 1));
        testCases.addAll(initTestCase(TEST_SIZE, "TIMESTAMP", "timestamp2", 2));
        testCases.addAll(initTestCase(TEST_SIZE, "TIMESTAMP", "timestamp3", 3));
        testCases.addAll(initTestCase(TEST_SIZE, "TIMESTAMP", "timestamp4", 4));
        testCases.addAll(initTestCase(TEST_SIZE, "TIMESTAMP", "timestamp5", 5));
        testCases.addAll(initTestCase(TEST_SIZE, "TIMESTAMP", "timestamp6", 6));
        //selectTest(testCases, null);
        selectTest(testCases, Calendar.getInstance(TimeZone.getTimeZone("GMT-3:00"), Locale.US));
    }

    // 依赖sql_mode, 其中这两个选项要关闭: NO_ZERO_IN_DATE,NO_ZERO_DATE
    // 否则会因为不能对时间插入0值导致报错
    @Test
    public void exceptionTest() throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("delete from " + TABLE_NAME);

        // 时间存入0值, 没问题
        stmt.executeUpdate("insert into " + TABLE_NAME + " (timestamp0) values ('0000-00-00 00:00:00.000000')");
        stmt.executeUpdate("insert into " + TABLE_NAME + " (datetime0) values ('0000-00-00 00:00:00.000000')");
        stmt.executeUpdate("insert into " + TABLE_NAME + " (date) values ('0000-00-00')");
        stmt.executeUpdate("insert into " + TABLE_NAME + " (date) values ('2021-00-00')");

        // mysql TIME类型 -838:00:00.x ~ 838:59:59.x
        // getString 正常返回, getTime报错
        stmt.executeUpdate("delete from " + TABLE_NAME);
        stmt.executeUpdate("insert into " + TABLE_NAME + " (time6) values ('-50:23:00.123450')");
        ResultSet rs = stmt.executeQuery("select time6 from " + TABLE_NAME);

        Assert.assertTrue(rs.next());
        Assert.assertEquals("-50:23:00.123450", rs.getString(1));
        try {
            rs.getTime(1);
        } catch (Exception e) {
            Assert.assertEquals("java.sql.SQLException", e.getClass().getName());
            Assert.assertTrue(e.getMessage().equals(
                "The value '-50:23:00.123450' is an invalid TIME value. JDBC Time objects represent a wall-clock time and not a duration as MySQL treats them. If you are treating this type as a duration, consider retrieving this value as a string and dealing with it according to your requirements."));
            printInfo(e.getMessage());
        }
        Assert.assertFalse(rs.next());

        // {{sql, ExceptionName, ExceptionMessage},{},...}
        String[][] testcases = {
            // TIMESTAMP 不存在的日期
            {"insert into " + TABLE_NAME + " (timestamp6) values ('2020-08-00 00:00:00.000456')", "com.mysql.cj.jdbc.exceptions.MysqlDataTruncation", "Incorrect datetime value"},
            // TIMESTAMP 超出范围的时间
            {"insert into " + TABLE_NAME + " (timestamp6) values ('3000-08-20 00:00:00.000456')", "com.mysql.cj.jdbc.exceptions.MysqlDataTruncation", "Incorrect datetime value"},
            // DATETIME 超出范围
            {"insert into " + TABLE_NAME + " (datetime6) values ('12345-08-20 00:00:00.000456')", "com.mysql.cj.jdbc.exceptions.MysqlDataTruncation", "Incorrect datetime value"},
            // DATETIME 不存在的日期
            {"insert into " + TABLE_NAME + " (datetime6) values ('2021-09-31 00:00:00.000456')", "com.mysql.cj.jdbc.exceptions.MysqlDataTruncation", "Incorrect datetime value"},
            // DATE 不存在的日期
            {"insert into " + TABLE_NAME + " (date) values ('2021-13-20')", "com.mysql.cj.jdbc.exceptions.MysqlDataTruncation", "Incorrect date value"},
            // TIME 超出范围
            {"insert into " + TABLE_NAME + " (time6) values ('900:00:00.000456')", "com.mysql.cj.jdbc.exceptions.MysqlDataTruncation", "Incorrect time value"},
        };

        for (String[] test : testcases) {
            printInfo(test[0]);
            boolean flag = false;
            try {
                stmt.executeUpdate(test[0]);
            } catch (Exception e) {
                flag = true;
                Assert.assertEquals(test[1], e.getClass().getName());
                Assert.assertTrue(e.getMessage().contains(test[2]));
            }
            Assert.assertTrue(flag);
        }

        stmt.close();
        conn.close();
    }

    @Test
    public void precisionTest() throws SQLException {
        // 测试时间类型getString小数点后位数是否正确
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("delete from " + TABLE_NAME);

        String[] timelist = {
            "2021-08-18", //date
            "2021", // year

            "13:58:03", // time0
            "13:58:03.1", // time1
            "13:58:03.00", // time2, 小数部分全0时, getString返回不带小数
            "13:58:03.123", // time3
            "13:58:03.1234", // time4
            "13:58:03.12345", // time5
            "13:58:03.123450", // time6, 小数部分非0时, 末位0不能省略

            "2021-08-18 13:58:03", // datetime0
            "2021-08-18 13:58:03.1", // datetime1
            "2021-08-18 13:58:03.12", // datetime2
            "2021-08-18 13:58:03.123", // datetime3
            "2021-08-18 13:58:03.1234", // datetime4
            "2021-08-18 13:58:03.12345", // datetime5
            "2021-08-18 13:58:03.123450", // datetime6

            "2021-08-18 13:58:03", // timestamp0
            "2021-08-18 13:58:03.1", // timestamp1
            "2021-08-18 13:58:03.12", // timestamp2
            "2021-08-18 13:58:03.123", // timestamp3
            "2021-08-18 13:58:03.1234", // timestamp4
            "2021-08-18 13:58:03.12345", // timestamp5
            "2021-08-18 13:58:03.120000" // timestamp6
        };

        PreparedStatement pstmt = conn.prepareStatement("insert into " + TABLE_NAME + " "
            + "(date,year,"
            + "time0,time1,time2,time3,time4,time5,time6,"
            + "datetime0,datetime1,datetime2,datetime3,datetime4,datetime5,datetime6,"
            + "timestamp0,timestamp1,timestamp2,timestamp3,timestamp4,timestamp5,timestamp6) "
            + "values (?,?, ?,?,?,?,?,?,?, ?,?,?,?,?,?,?, ?,?,?,?,?,?,?)");

        for (int i = 1; i <= timelist.length; i++) {
            pstmt.setString(i, timelist[i - 1]);
        }
        pstmt.executeUpdate();

        ResultSet rs = stmt.executeQuery("select "
            + "date,year,"
            + "time0,time1,time2,time3,time4,time5,time6,"
            + "datetime0,datetime1,datetime2,datetime3,datetime4,datetime5,datetime6,"
            + "timestamp0,timestamp1,timestamp2,timestamp3,timestamp4,timestamp5,timestamp6 "
            + "from " + TABLE_NAME);

        Assert.assertTrue(rs.next());
        for (int i = 1; i <= timelist.length; i++) {
            if (i == 2) {
                // YEAR类型比较特殊, getString返回的是 year-01-01
                Assert.assertEquals(timelist[i - 1] + "-01-01", rs.getString(i));
            } else if (i == 5) {
                // "13:58:03.00", 小数部分全0时, getString返回不带小数
                Assert.assertEquals(timelist[i - 1].substring(0, timelist[i - 1].length() - 3), rs.getString(i));
            } else {
                Assert.assertEquals(timelist[i - 1], rs.getString(i));
            }
        }
        Assert.assertFalse(rs.next());

        rs.close();
        stmt.close();
        pstmt.close();
    }

    private interface GetTimeObjectFunction {
        Object exec() throws Exception;
    }

    @Getter
    @AllArgsConstructor
    private static class TestCase {
        private final String[] initSQL;

        private final String query;

        private final String timeString;

        private final String mysqlType;

        public void printAll(final int i) {
            printComment("# TEST CASE " + i);
            printNormal("initSQL:");
            for (String sql : initSQL) {
                printNormal("\t" + sql);
            }
            printNormal("query: " + query);
        }
    }
}
