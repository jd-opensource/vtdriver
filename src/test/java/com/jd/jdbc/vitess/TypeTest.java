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

import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Assert;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.Test;
import testsuite.TestSuite;
import static testsuite.internal.TestSuiteShardSpec.TWO_SHARDS;

/*
CREATE TABLE `type_test`
(
    `id`            bigint(20) unsigned NOT NULL AUTO_INCREMENT,
    `f_key`         char(32)       NOT NULL DEFAULT '' COMMENT '分片键',
    `f_tinyint`     tinyint(4) DEFAULT NULL,
    `f_u_tinyint`   tinyint(3) unsigned DEFAULT NULL,
    `f_smallint`    smallint(6) DEFAULT NULL,
    `f_u_smallint`  smallint(5) unsigned DEFAULT NULL,
    `f_mediumint`   mediumint(9) DEFAULT NULL,
    `f_u_mediumint` mediumint(8) unsigned DEFAULT NULL,
    `f_int`         int(11) DEFAULT NULL,
    `f_u_int`       int(10) unsigned DEFAULT NULL,
    `f_bigint`      bigint(20) DEFAULT NULL,
    `f_u_bigint`    bigint(20) unsigned DEFAULT NULL,
    `f_float`       float                   DEFAULT NULL,
    `f_u_float`     float unsigned DEFAULT NULL,
    `f_double`      double                  DEFAULT NULL,
    `f_u_double`    double unsigned DEFAULT NULL,
    `f_decimal`     decimal(65, 4) NOT NULL,
    `f_u_decimal`   decimal(65, 4) unsigned DEFAULT NULL,
    `f_bit`         bit(64)                 DEFAULT NULL,
    `f_date`        date                    DEFAULT NULL,
    `f_time`        time                    DEFAULT NULL,
    `f_datetime`    datetime(3) DEFAULT NULL,
    `f_timestamp`   timestamp(3) NULL DEFAULT NULL,
    `f_year` year(4) DEFAULT NULL,
    `f_boolean`     tinyint(1) unsigned DEFAULT NULL,
    `f_varchar`     varchar(255)            DEFAULT NULL,
    `f_text`        text,
    `f_ttext`       tinytext,
    `f_mtext`       mediumtext,
    `f_ltext`       longtext,
    `f_varbinary`   varbinary(1000) DEFAULT NULL,
    `f_blob`        blob,
    `f_mblob`       mediumblob,
    `f_tblob`       tinyblob,
    `f_lblob`       longblob,
    `f_binary`      binary(50) DEFAULT NULL,
    `f_enum`        enum('Y','N') DEFAULT NULL,
    `f_set` set('Value A','Value B') DEFAULT NULL,
    `f_ger`         geometry                DEFAULT NULL,
    `f_json`        json                    DEFAULT NULL,
    PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `type_bit_tinyint` (
	`id` INT(11) NOT NULL,
	`f_bit_1` BIT(1) NULL,
	`f_bit_64` BIT(64) NULL,
	`f_u_tinyint_1` TINYINT(1) UNSIGNED NULL,
	`f_tinyint_1` TINYINT(1) NULL,
	`f_u_tinyint_64` TINYINT(128) UNSIGNED NULL,
	`f_tinyint_64` TINYINT(128) NULL,
	PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

* */
public class TypeTest extends TestSuite {
    protected Connection driverConnection;

    protected String sql1;

    protected String sql2;

    protected PreparedStatement stmtUpdate;

    protected PreparedStatement stmtSelect;

    String sql = "INSERT INTO `type_test` (`id`, `f_key`, `f_tinyint`, `f_u_tinyint`, `f_smallint`, `f_u_smallint`, `f_mediumint`, `f_u_mediumint`, " +
        "`f_int`, `f_u_int`, `f_bigint`, `f_u_bigint`, `f_float`, `f_u_float`, `f_double`, `f_u_double`, `f_decimal`, `f_u_decimal`, `f_bit`, `f_date`, " +
        "`f_time`, `f_datetime`, `f_timestamp`, `f_year`, `f_boolean`, `f_varchar`, `f_text`, `f_ttext`, `f_mtext`, `f_ltext`, `f_varbinary`, `f_blob`, " +
        "`f_mblob`, `f_tblob`, `f_lblob`, `f_binary`, `f_enum`, `f_set`, `f_ger`, `f_json`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);\n";

    String selectSql = "select * from type_test";

    public static java.util.Date getCurrYearFirst() {
        Calendar currCal = Calendar.getInstance();
        Calendar calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(Calendar.YEAR, currCal.get(Calendar.YEAR));
        return calendar.getTime();
    }

    @Before
    public void init() throws SQLException {
        driverConnection = getConnection(Driver.of(TWO_SHARDS));
        try (Statement stmt = driverConnection.createStatement()) {
            stmt.executeUpdate("delete from type_test");
        }
    }

    @After
    public void close() throws SQLException {
        if (driverConnection != null) {
            driverConnection.close();
        }
    }

    @Test
    public void a() throws SQLException, IOException {
        byte[] bufffer = new byte[256];
        for (int i = 0; i < 256; i++) {
            bufffer[i] = (byte) (i - 127);
        }
        //stmt.setString(19, "MySQL");
        byte[] mysqlBuffer = new byte[] {0x4D, 0x79, 0x53, 0x51, 0x4C};
        long now = System.currentTimeMillis();

        try (PreparedStatement stmt = driverConnection.prepareStatement(sql)) {
            stmt.setNull(1, Types.NULL);
            stmt.setString(2, "abc");

            stmt.setByte(3, (byte) -128);
            stmt.setShort(4, (short) 255);
            stmt.setShort(5, (short) -32768);
            stmt.setInt(6, 65535);
            stmt.setInt(7, -8388608);
            stmt.setInt(8, 16777215);
            stmt.setInt(9, -2147483648);
            stmt.setLong(10, 4294967295L);
            stmt.setLong(11, -9223372036854775808L);
            stmt.setBigDecimal(12, new BigDecimal("18446744073709551615"));
            stmt.setFloat(13, -123.456f);
            stmt.setFloat(14, 666.456f);
            stmt.setDouble(15, -456.789);
            stmt.setDouble(16, 456.789);
            stmt.setBigDecimal(17, new BigDecimal("-123456789012345.6789"));
            stmt.setBigDecimal(18, new BigDecimal("123456789012345.6789"));

            stmt.setBoolean(19, true);

            stmt.setDate(20, new Date(now));
            stmt.setTime(21, new Time(now));
            stmt.setTimestamp(22, new Timestamp(now));
            stmt.setTimestamp(23, new Timestamp(now));
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy");
            stmt.setString(24, simpleDateFormat.format(new java.util.Date()));

            stmt.setBoolean(25, false);

            stmt.setBytes(26, mysqlBuffer);

            stmt.setString(27, "Mysql-中文支持不text么1");
            stmt.setString(28, "Mysql-中文支持不text么2");
            stmt.setString(29, "Mysql-中文支持不text么3");
            stmt.setString(30, "Mysql-中文支持不text么4");

            stmt.setBinaryStream(31, new ByteArrayInputStream(bufffer));

            stmt.setBlob(32, new ByteArrayInputStream(bufffer));
            stmt.setBlob(33, new ByteArrayInputStream(bufffer));
            stmt.setBlob(34, new ByteArrayInputStream(new byte[] {1}));
            stmt.setBlob(35, new ByteArrayInputStream(bufffer));

            stmt.setBytes(36, mysqlBuffer);
            stmt.setString(37, null);
            stmt.setString(38, null);
            stmt.setString(39, null);
            stmt.setString(40, null);

            stmt.execute();
        }

        try (Statement statement = driverConnection.createStatement()) {
            ResultSet resultSet = statement.executeQuery(selectSql);

            assertTrue(resultSet.next());

            assertEquals(-128, resultSet.getByte(3));
            assertEquals(-128, resultSet.getObject(3));
            assertEquals(255, resultSet.getShort(4));
            assertEquals(255, resultSet.getObject(4));
            assertEquals(-32768, resultSet.getShort(5));
            assertEquals(-32768, resultSet.getObject(5));
            assertEquals(65535, resultSet.getInt(6));
            assertEquals(65535, resultSet.getObject(6));
            assertEquals(-8388608, resultSet.getInt(7));
            assertEquals(-8388608, resultSet.getObject(7));
            assertEquals(16777215, resultSet.getInt(8));
            assertEquals(16777215, resultSet.getObject(8));
            assertEquals(-2147483648, resultSet.getInt(9));
            assertEquals(-2147483648, resultSet.getObject(9));
            assertEquals(4294967295L, resultSet.getLong(10));
            assertEquals(4294967295L, resultSet.getObject(10));
            assertEquals(-9223372036854775808L, resultSet.getLong(11));
            assertEquals(-9223372036854775808L, resultSet.getObject(11));
            assertEquals(new BigDecimal("18446744073709551615"), resultSet.getBigDecimal(12));
            assertEquals(new BigInteger("18446744073709551615"), resultSet.getObject(12));
            assertEquals(-123.456f, resultSet.getFloat(13), 0.0001);
            assertEquals(-123.456f, resultSet.getObject(13));
            assertEquals(666.456f, resultSet.getFloat(14), 0.0001);
            assertEquals(666.456f, resultSet.getObject(14));
            assertEquals(-456.789, resultSet.getDouble(15), 0.0001);
            assertEquals(-456.789, resultSet.getObject(15));
            assertEquals(456.789, resultSet.getDouble(16), 0.0001);
            assertEquals(456.789, resultSet.getObject(16));

            assertEquals(new BigDecimal("-123456789012345.6789"), resultSet.getBigDecimal(17));
            assertEquals(new BigDecimal("-123456789012345.6789"), resultSet.getObject(17));
            assertEquals(new BigDecimal("123456789012345.6789"), resultSet.getBigDecimal(18));
            assertEquals(new BigDecimal("123456789012345.6789"), resultSet.getObject(18));

            assertTrue(resultSet.getBoolean(19));
            assertEquals(new Date(now).toString(), resultSet.getDate(20).toString());
            assertEquals(new Date(now).toString(), resultSet.getObject(20).toString());
            assertEquals(new Time(now).toString(), resultSet.getTime(21).toString());
            assertEquals(new Time(now).toString(), resultSet.getObject(21).toString());
            assertEquals(new Timestamp(now).toString(), resultSet.getTimestamp(22).toString());
            assertEquals(new Timestamp(now).toString(), resultSet.getObject(22).toString());
            assertEquals(new Timestamp(now).toString(), resultSet.getTimestamp(23).toString());
            assertEquals(new Timestamp(now).toString(), resultSet.getObject(23).toString());
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-01-01");
            assertEquals(simpleDateFormat.format(new java.util.Date()), resultSet.getString(24));
            assertEquals(new Date(getCurrYearFirst().getTime()), resultSet.getObject(24));

            assertFalse(resultSet.getBoolean(25));
            assertFalse((Integer) resultSet.getObject(25) == 1);
            assertEquals("MySQL", resultSet.getString(26));
            assertEquals("MySQL", resultSet.getObject(26));

            assertEquals("Mysql-中文支持不text么1", resultSet.getString(27));
            assertEquals("Mysql-中文支持不text么1", resultSet.getObject(27));
            assertEquals("Mysql-中文支持不text么2", resultSet.getString(28));
            assertEquals("Mysql-中文支持不text么2", resultSet.getObject(28));
            assertEquals("Mysql-中文支持不text么3", resultSet.getString(29));
            assertEquals("Mysql-中文支持不text么3", resultSet.getObject(29));
            assertEquals("Mysql-中文支持不text么4", resultSet.getString(30));
            assertEquals("Mysql-中文支持不text么4", resultSet.getObject(30));


            byte[] returnBytes = new byte[256];
            resultSet.getBinaryStream(31).read(returnBytes);
            assertArrayEquals(bufffer, returnBytes);
            assertArrayEquals(bufffer, (byte[]) resultSet.getObject(31));


            returnBytes = new byte[256];
            resultSet.getBlob(32).getBinaryStream().read(returnBytes);
            assertArrayEquals(bufffer, returnBytes);
            assertArrayEquals(bufffer, (byte[]) resultSet.getObject(32));


            returnBytes = new byte[256];
            resultSet.getBlob(33).getBinaryStream().read(returnBytes);
            assertArrayEquals(bufffer, returnBytes);
            assertArrayEquals(bufffer, (byte[]) resultSet.getObject(33));

            returnBytes = new byte[256];
            resultSet.getBlob(34).getBinaryStream().read(returnBytes);
            byte[] bufffer1 = new byte[256];
            for (int i = 0; i < 256; i++) {
                if (i == 0) {
                    bufffer1[0] = 1;
                } else {
                    bufffer1[i] = 0;
                }
            }
            assertArrayEquals(bufffer1, returnBytes);
            assertArrayEquals(new byte[] {1}, (byte[]) resultSet.getObject(34));

            returnBytes = new byte[256];
            resultSet.getBlob(35).getBinaryStream().read(returnBytes);
            assertArrayEquals(bufffer, returnBytes);
            assertArrayEquals(bufffer, (byte[]) resultSet.getObject(35));

            byte[] mysqlBuffer50 = new byte[50];
            mysqlBuffer50[0] = 0x4D;
            mysqlBuffer50[1] = 0x79;
            mysqlBuffer50[2] = 0x53;
            mysqlBuffer50[3] = 0x51;
            mysqlBuffer50[4] = 0x4C;
            for (int i = 5; i < 50; i++) {
                mysqlBuffer50[i] = 0;
            }
            assertArrayEquals(mysqlBuffer50, resultSet.getBytes(36));
            assertArrayEquals(mysqlBuffer50, (byte[]) resultSet.getObject(36));
        }
    }

    @Test
    public void test0() throws SQLException {
        sql1 = "INSERT INTO `type_test` (`id`, `f_key`, `f_tinyint`, `f_u_tinyint`, `f_smallint`, `f_u_smallint`, `f_mediumint`, `f_u_mediumint`, " +
            "`f_int`, `f_u_int`, `f_bigint`, `f_u_bigint`, `f_float`, `f_u_float`, `f_double`, `f_u_double`, `f_decimal`, `f_u_decimal`) " +
            "VALUES (1, 'f_keykkk', ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);\n";
        sql2 = "SELECT `f_tinyint`, `f_u_tinyint`, `f_smallint`, `f_u_smallint`, `f_mediumint`, `f_u_mediumint`, " +
            "`f_int`, `f_u_int`, `f_bigint`, `f_u_bigint`, `f_float`, `f_u_float`, `f_double`, `f_u_double`, `f_decimal`, `f_u_decimal` " +
            "FROM type_test WHERE id = 1";

        stmtUpdate = driverConnection.prepareStatement(sql1);
        stmtSelect = driverConnection.prepareStatement(sql2);
    }

    @Test
    public void setbool_getint_test_true() throws SQLException {
        test0();
        for (int i = 1; i <= 16; i++) {
            stmtUpdate.setBoolean(i, true);
        }
        stmtUpdate.executeUpdate();
        ResultSet rs = stmtSelect.executeQuery();
        while (rs.next()) {
            for (int i = 1; i <= 16; i++) {
                Assert.assertEquals(1, rs.getInt(i));
                Assert.assertTrue(rs.getBoolean(i));
            }
        }
    }

    @Test
    public void setbool_getint_test_false() throws SQLException {
        test0();
        for (int i = 1; i <= 16; i++) {
            stmtUpdate.setBoolean(i, false);
        }
        stmtUpdate.executeUpdate();
        ResultSet rs = stmtSelect.executeQuery();
        while (rs.next()) {
            for (int i = 1; i <= 16; i++) {
                Assert.assertEquals(0, rs.getInt(i));
                Assert.assertFalse(rs.getBoolean(i));
            }
        }
    }

    @Test
    public void setint_getbool_test_true() throws SQLException {
        test0();

        for (int i = 1; i <= 16; i++) {
            stmtUpdate.setInt(i, 1);
        }
        stmtUpdate.executeUpdate();

        ResultSet rs = stmtSelect.executeQuery();
        while (rs.next()) {
            for (int i = 1; i <= 16; i++) {
                Assert.assertTrue(rs.getBoolean(i));
                Assert.assertEquals(1, rs.getInt(i));
            }
        }
    }

    @Test
    public void setint_getbool_test_false() throws SQLException {
        test0();

        for (int i = 1; i <= 16; i++) {
            stmtUpdate.setInt(i, 0);
        }
        stmtUpdate.executeUpdate();

        ResultSet rs = stmtSelect.executeQuery();
        while (rs.next()) {
            for (int i = 1; i <= 16; i++) {
                Assert.assertFalse(rs.getBoolean(i));
                Assert.assertEquals(0, rs.getInt(i));
            }
        }
    }

    @Test
    public void testNul() throws SQLException {
        List<Long> bits = Lists.newArrayList(null, null, -2L, -3L, 1L, 128L);
        for (Long bit : bits) {
            String randomStr = RandomStringUtils.random(5);
            printInfo("bit: " + bit + " randomStr: " + Arrays.toString(randomStr.getBytes()));
            int stringIndex = 2;
            int idIndex = 1;
            int bigDecimalIndex = 17;
            int bitIndex = 19;
            try (PreparedStatement stmt = driverConnection.prepareStatement(sql)) {
                for (int i = 1; i <= 40; i++) {
                    if (i == stringIndex) {
                        stmt.setString(stringIndex, randomStr);
                    } else if (i == bigDecimalIndex) {
                        stmt.setBigDecimal(bigDecimalIndex, new BigDecimal("-123456789012345.6789"));
                    } else if (i == bitIndex) {
                        if (bit == null) {
                            stmt.setNull(bitIndex, 0);
                        } else {
                            stmt.setLong(bitIndex, bit);
                        }
                    } else {
                        stmt.setNull(i, Types.NULL);
                    }
                }

                int affectedRows = stmt.executeUpdate();
                assertEquals("affeted rows not 1, but " + affectedRows, 1, affectedRows);
            }

            try (PreparedStatement stmt = driverConnection.prepareStatement(selectSql)) {
                ResultSet resultSet = stmt.executeQuery();
                assertTrue(resultSet.next());
                assertEquals(randomStr, resultSet.getString(stringIndex));
                assertEquals(randomStr, resultSet.getObject(stringIndex));

                assertEquals(new BigDecimal("-123456789012345.6789"), resultSet.getBigDecimal(bigDecimalIndex));
                assertEquals(new BigDecimal("-123456789012345.6789"), resultSet.getObject(bigDecimalIndex));
                int columnCount = resultSet.getMetaData().getColumnCount();
                for (int i = 1; i <= columnCount; i++) {
                    if (i == idIndex || i == stringIndex || i == bigDecimalIndex) {
                        continue;
                    }
                    if (i == bitIndex) {
                        Object object = resultSet.getObject(i);
                        if (object != null) {
                            long long1 = Longs.fromByteArray((byte[]) object);
                            assertEquals(bit.longValue(), long1);
                        } else {
                            assertNull(object);
                        }
                        continue;
                    }
                    assertNull(resultSet.getString(i));
                    assertNull(resultSet.getObject(i));
                }
            }
            try (Statement stmt = driverConnection.createStatement()) {
                stmt.executeUpdate("delete from type_test");
            }
        }
    }

    @Test
    public void bitTinyInt() throws SQLException {
        cleanBitTinyint();
        Set<Integer> ids = new HashSet<>();
        while (ids.size() < 20) {
            ids.add(RandomUtils.nextInt(1, 2000));
        }
        for (Integer id : ids) {
            int bit1 = RandomUtils.nextInt(0, 1);
            long bit64 = RandomUtils.nextLong(0, Long.MAX_VALUE);
            int utinyint1 = RandomUtils.nextInt(0, 200);
            int tinyint1 = RandomUtils.nextInt(0, 128);
            if (utinyint1 % 2 == 0) {
                tinyint1 = -tinyint1;
            }
            int utinyint64 = RandomUtils.nextInt(0, 256);
            int tinyint64 = RandomUtils.nextInt(0, 128);
            String sql = "INSERT INTO `type_bit_tinyint` (`id`, `f_bit_1`, `f_bit_64`, `f_u_tinyint_1`, `f_tinyint_1`, `f_u_tinyint_64`, `f_tinyint_64`) VALUES (?,?,?,?,?,?,?);";
            try (PreparedStatement stmt = driverConnection.prepareStatement(sql)) {
                stmt.setInt(1, id);
                stmt.setInt(2, bit1);
                stmt.setLong(3, bit64);
                stmt.setInt(4, utinyint1);
                stmt.setInt(5, tinyint1);
                stmt.setInt(6, utinyint64);
                stmt.setInt(7, tinyint64);
                int affectedRows = stmt.executeUpdate();
                assertEquals("affeted rows not 1, but " + affectedRows, 1, affectedRows);
            }
            try (PreparedStatement stmt = driverConnection.prepareStatement("select * from type_bit_tinyint where id = ?")) {
                stmt.setInt(1, id);
                ResultSet resultSet = stmt.executeQuery();
                assertTrue(resultSet.next());
                assertEquals((int) id, resultSet.getInt(1));
                assertEquals(bit1, resultSet.getInt(2));
                assertEquals(bit1 != 0, resultSet.getObject(2));
                assertEquals(resultSet.getBoolean(2), resultSet.getObject(2));
                assertEquals(bit64, resultSet.getLong(3));
                assertEquals(bit64, Longs.fromByteArray((byte[]) resultSet.getObject(3)));
                assertEquals(utinyint1, resultSet.getInt(4));
                assertEquals(tinyint1, resultSet.getInt(5));
                assertEquals(" tinyint1=" + tinyint1, tinyint1 > 0 || tinyint1 == -1, resultSet.getObject(5));
                assertEquals(resultSet.getBoolean(5), resultSet.getObject(5));
                assertEquals(utinyint64, resultSet.getInt(6));
                assertEquals(tinyint64, resultSet.getInt(7));
            }
        }
        cleanBitTinyint();
    }

    private void cleanBitTinyint() throws SQLException {
        try (Statement stmt = driverConnection.createStatement()) {
            stmt.executeUpdate("delete from type_bit_tinyint");
        }
    }

    @Test
    public void sumFields() throws SQLException {
        String sql = "select sum(f_key), sum(f_tinyint), sum(f_u_tinyint),sum(f_smallint),sum(f_u_smallint),sum(f_mediumint),sum(f_u_mediumint),sum(f_int),sum(f_u_int),sum(f_bigint),sum(f_u_bigint)" +
            ",sum(f_float),sum(f_u_float),sum(f_double),sum(f_u_double),sum(f_decimal),sum(f_u_decimal),sum(f_bit),sum(f_date)," +
            "sum(f_time),sum(f_datetime),sum(f_timestamp),sum(f_year),sum(f_boolean),sum(f_varchar),sum(f_text),sum(f_ttext)" +
            ",sum(f_mtext),sum(f_ltext),sum(f_varbinary),sum(f_blob),sum(f_mblob),sum(f_tblob),sum(f_lblob),sum(f_binary)," +
            "sum(f_enum),sum(f_set),sum(f_json)  from type_test ";
        stmtSelect = driverConnection.prepareStatement(sql);
        ResultSet rs = stmtSelect.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getMetaData().getColumnTypeName(1));
            System.out.println(rs.getMetaData().getColumnTypeName(2));
            System.out.println(rs.getMetaData().getColumnTypeName(3));
            System.out.println(rs.getMetaData().getColumnTypeName(4));
            System.out.println(rs.getMetaData().getColumnTypeName(5));
            System.out.println(rs.getMetaData().getColumnTypeName(6));
            System.out.println(rs.getMetaData().getColumnTypeName(7));
            System.out.println(rs.getMetaData().getColumnTypeName(8));
            System.out.println(rs.getMetaData().getColumnTypeName(9));
            System.out.println(rs.getMetaData().getColumnTypeName(10));
            System.out.println(rs.getMetaData().getColumnTypeName(11));
            System.out.println(rs.getMetaData().getColumnTypeName(12));
            System.out.println(rs.getMetaData().getColumnTypeName(13));
            System.out.println(rs.getMetaData().getColumnTypeName(14));
            System.out.println(rs.getMetaData().getColumnTypeName(15));
            System.out.println(rs.getMetaData().getColumnTypeName(16));
            System.out.println(rs.getMetaData().getColumnTypeName(17));
            System.out.println(rs.getMetaData().getColumnTypeName(18));
            System.out.println(rs.getMetaData().getColumnTypeName(19));
            System.out.println(rs.getMetaData().getColumnTypeName(20));
            System.out.println(rs.getMetaData().getColumnTypeName(21));
            System.out.println(rs.getMetaData().getColumnTypeName(22));
            System.out.println(rs.getMetaData().getColumnTypeName(23));
            System.out.println(rs.getMetaData().getColumnTypeName(24));
            System.out.println(rs.getMetaData().getColumnTypeName(25));
            System.out.println(rs.getMetaData().getColumnTypeName(26));
            System.out.println(rs.getMetaData().getColumnTypeName(27));
            System.out.println(rs.getMetaData().getColumnTypeName(28));
            System.out.println(rs.getMetaData().getColumnTypeName(29));
            System.out.println(rs.getMetaData().getColumnTypeName(30));
            System.out.println(rs.getMetaData().getColumnTypeName(31));
            System.out.println(rs.getMetaData().getColumnTypeName(32));
            System.out.println(rs.getMetaData().getColumnTypeName(33));
            System.out.println(rs.getMetaData().getColumnTypeName(34));
            System.out.println(rs.getMetaData().getColumnTypeName(35));
            System.out.println(rs.getMetaData().getColumnTypeName(36));
            System.out.println(rs.getMetaData().getColumnTypeName(37));
            System.out.println(rs.getMetaData().getColumnTypeName(38));
        }
    }

    @Test
    public void countFields() throws SQLException {
        String sql =
            "select count(f_key), count(f_tinyint), count(f_u_tinyint),count(f_smallint),count(f_u_smallint),count(f_mediumint),count(f_u_mediumint),count(f_int),count(f_u_int),count(f_bigint),count(f_u_bigint)" +
                ",count(f_float),count(f_u_float),count(f_double),count(f_u_double),count(f_decimal),count(f_u_decimal),count(f_bit),count(f_date)," +
                "count(f_time),count(f_datetime),count(f_timestamp),count(f_year),count(f_boolean),count(f_varchar),count(f_text),count(f_ttext)" +
                ",count(f_mtext),count(f_ltext),count(f_varbinary),count(f_blob),count(f_mblob),count(f_tblob),count(f_lblob),count(f_binary)," +
                "count(f_enum),count(f_set),count(f_json)  from type_test ";
        stmtSelect = driverConnection.prepareStatement(sql);
        ResultSet rs = stmtSelect.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getMetaData().getColumnTypeName(1));
            System.out.println(rs.getMetaData().getColumnTypeName(2));
            System.out.println(rs.getMetaData().getColumnTypeName(3));
            System.out.println(rs.getMetaData().getColumnTypeName(4));
            System.out.println(rs.getMetaData().getColumnTypeName(5));
            System.out.println(rs.getMetaData().getColumnTypeName(6));
            System.out.println(rs.getMetaData().getColumnTypeName(7));
            System.out.println(rs.getMetaData().getColumnTypeName(8));
            System.out.println(rs.getMetaData().getColumnTypeName(9));
            System.out.println(rs.getMetaData().getColumnTypeName(10));
            System.out.println(rs.getMetaData().getColumnTypeName(11));
            System.out.println(rs.getMetaData().getColumnTypeName(12));
            System.out.println(rs.getMetaData().getColumnTypeName(13));
            System.out.println(rs.getMetaData().getColumnTypeName(14));
            System.out.println(rs.getMetaData().getColumnTypeName(15));
            System.out.println(rs.getMetaData().getColumnTypeName(16));
            System.out.println(rs.getMetaData().getColumnTypeName(17));
            System.out.println(rs.getMetaData().getColumnTypeName(18));
            System.out.println(rs.getMetaData().getColumnTypeName(19));
            System.out.println(rs.getMetaData().getColumnTypeName(20));
            System.out.println(rs.getMetaData().getColumnTypeName(21));
            System.out.println(rs.getMetaData().getColumnTypeName(22));
            System.out.println(rs.getMetaData().getColumnTypeName(23));
            System.out.println(rs.getMetaData().getColumnTypeName(24));
            System.out.println(rs.getMetaData().getColumnTypeName(25));
            System.out.println(rs.getMetaData().getColumnTypeName(26));
            System.out.println(rs.getMetaData().getColumnTypeName(27));
            System.out.println(rs.getMetaData().getColumnTypeName(28));
            System.out.println(rs.getMetaData().getColumnTypeName(29));
            System.out.println(rs.getMetaData().getColumnTypeName(30));
            System.out.println(rs.getMetaData().getColumnTypeName(31));
            System.out.println(rs.getMetaData().getColumnTypeName(32));
            System.out.println(rs.getMetaData().getColumnTypeName(33));
            System.out.println(rs.getMetaData().getColumnTypeName(34));
            System.out.println(rs.getMetaData().getColumnTypeName(35));
            System.out.println(rs.getMetaData().getColumnTypeName(36));
            System.out.println(rs.getMetaData().getColumnTypeName(37));
            System.out.println(rs.getMetaData().getColumnTypeName(38));
        }
    }
}