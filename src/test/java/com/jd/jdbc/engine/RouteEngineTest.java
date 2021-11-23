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

package com.jd.jdbc.engine;

import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class RouteEngineTest extends TestSuite {
    public static Connection conn;

    @BeforeClass
    public static void init() throws SQLException, InterruptedException {
        conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        initBefore();
    }

    @AfterClass
    public static void close() {
        closeConnection(conn);
    }

    public static void initBefore() throws SQLException {
        Statement statement = conn.createStatement();
        statement.executeUpdate("delete from time_test");

        ResultSet resultSet = statement.executeQuery("select max(id) from time_test");
        int maxid = 0;
        while (resultSet.next()) {
            maxid = resultSet.getInt(1);
        }
        String sql = "INSERT INTO `time_test`" +
            " (`id`, `date`, `year`, `time0`, `time1`, `time2`, " +
            "`time3`, `time4`, `time5`, `time6`, `datetime0`," +
            " `datetime1`, `datetime2`, `datetime3`, `datetime4`," +
            " `datetime5`, `datetime6`, `timestamp0`, `timestamp1`, " +
            "`timestamp2`, `timestamp3`, `timestamp4`, `timestamp5`, `timestamp6`) " +
            "VALUES (%s, '2021-09-14', '2021', '18:39:18', '18:39:18.0', '18:39:19.00', " +
            "'18:39:19.000', '18:39:20.0000', '18:39:21.00000', '18:39:21.000000', " +
            "'2021-09-14 18:39:25', '2021-09-14 18:39:26.0', '2021-09-14 18:39:27.00', " +
            "'2021-09-14 18:39:28.000', '2021-09-14 18:39:29.0000', '2021-09-14 18:39:31.00000'," +
            " '2021-09-14 18:39:32.000000', '2021-09-14 18:39:33', '2021-09-14 18:39:34.0', " +
            "'2021-09-14 18:39:36.00', '2021-09-14 18:39:37.000', '2021-09-14 18:39:39.0000', " +
            "'2021-09-14 18:39:40.00000', '%s');";

        List<String> timeSrings =
            Lists.newArrayList("2021-09-14 15:39:42.000000", "2021-09-14 18:19:42.000000", "2021-09-14 18:29:42.000000", "2021-09-14 18:39:41.000000", "2021-09-14 18:20:42.000000");
        maxid = maxid + 2;
        List<String> sqlList = new ArrayList<>();
        for (String timeSring : timeSrings) {
            for (int i = 0; i < 1000; i++) {
                sqlList.add(String.format(sql, maxid++, timeSring));
            }
        }


        Collections.shuffle(sqlList);
        List<List<String>> partition = Lists.partition(sqlList, 100);
        List<String> sqls = new ArrayList<>();
        for (List<String> strings : partition) {
            StringBuilder stringBuilder = new StringBuilder();
            for (String string : strings) {
                stringBuilder.append(string);
            }
            sqls.add(stringBuilder.toString());
        }

        for (String a : sqls) {
            statement.execute(a);
        }
        printInfo("[INSERT OK]");
    }

    @Test
    public void testTimestamp() throws SQLException {
        String sql = "select `timestamp6` from time_test order by `timestamp6` desc limit 50000";
        Statement statement = conn.createStatement();
        statement.executeQuery(sql);
    }

    @Test
    public void testTimestamp2() throws SQLException {
        String sql = "select `date`,`timestamp6` from time_test group by `date` order by `timestamp6` desc limit 50000";
        Statement statement = conn.createStatement();
        statement.executeQuery(sql);
    }

    @Test
    public void testDate() throws SQLException {
        String sql = "select `date` from time_test order by `date` desc limit 50000";
        Statement statement = conn.createStatement();
        statement.executeQuery(sql);
    }

    @Test
    public void testDatetime0() throws SQLException {
        String sql = "select `datetime0` from time_test order by `datetime0` desc limit 50000";
        Statement statement = conn.createStatement();
        statement.executeQuery(sql);
    }


}
