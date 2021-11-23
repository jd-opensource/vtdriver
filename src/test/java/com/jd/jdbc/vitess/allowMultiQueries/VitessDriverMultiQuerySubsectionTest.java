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

package com.jd.jdbc.vitess.allowMultiQueries;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class VitessDriverMultiQuerySubsectionTest extends TestSuite {
    Connection connection;

    @Before
    public void init() throws SQLException {
        connection = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        Statement statement = connection.createStatement();
        statement.execute("delete from auto");
    }

    @After
    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void subsection() throws SQLException {
        Statement statement = connection.createStatement();
        int count = 1000;
        int id = 1;
        boolean flag;

        String insertSql = getMultiSql(count, "insert");
        printInfo(insertSql);
        statement.executeUpdate(insertSql);

        String selectSql = getMultiSql(count, "select");
        printInfo(selectSql);
        flag = statement.execute(selectSql);
        do {
            ResultSet resultSet1 = statement.getResultSet();
            while (resultSet1.next()) {
                Assert.assertEquals(resultSet1.getInt("id"), id);
                id++;
            }
            flag = statement.getMoreResults();
        } while (flag);

        Assert.assertEquals(count, id);
        printOk("Insert OK!");
        printOk("Select OK!");

        String updateSql = getMultiSql(count, "update");
        printInfo(updateSql);
        statement.execute(updateSql);
        flag = statement.execute(getMultiSql(count, "select"));
        id = 1;
        do {
            ResultSet resultSet1 = statement.getResultSet();
            while (resultSet1.next()) {
                Assert.assertNotNull(resultSet1.getObject("email"));
                id++;
            }
            flag = statement.getMoreResults();
        } while (flag);

        Assert.assertEquals(count, id);
        printOk("Update OK!");

        String deleteSql = getMultiSql(count, "delete");
        printInfo(deleteSql);
        statement.execute(deleteSql);
        flag = statement.execute(getMultiSql(count, "select"));
        ResultSet resultSet = statement.getResultSet();
        Assert.assertFalse(resultSet.next());
        printOk("delete OK!");

        connection.setAutoCommit(false);
        Statement stat = connection.prepareStatement(insertSql + selectSql);
        stat.execute(insertSql);
        flag = stat.execute(selectSql);

        connection.commit();
        do {
            ResultSet resultSet1 = statement.getResultSet();
            while (resultSet1.next()) {
                Assert.assertEquals(resultSet1.getInt("id"), id);
                id++;
            }
            flag = statement.getMoreResults();
        } while (flag);

        Assert.assertEquals(count, id);
        printOk("Transaction OK!");

    }

    private String getMultiSql(int count, String type) {
        String originalSql = null;
        StringBuffer sqlBuf = new StringBuffer();
        if (type.equals("insert")) {
            originalSql = "insert into auto(id,ai) values(%s,%s)";
        } else if (type.equals("select")) {
            originalSql = "select * from auto where id=%s and ai=%s";
        } else if (type.equals("update")) {
            originalSql = "update auto set email =%d where id=%s";
        } else if (type.equals("delete")) {
            originalSql = "delete from auto where id=%s";
        }
        String sql = null;
        for (int idx = 1; idx < count; idx++) {
            if (type.equals("update")) {
                sql = String.format(originalSql, 1, idx);
            } else {
                sql = String.format(originalSql, idx, idx);
            }

            sqlBuf.append(sql + ";");
        }
        return sqlBuf.toString();
    }

}
