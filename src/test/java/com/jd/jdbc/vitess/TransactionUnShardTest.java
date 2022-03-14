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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;

import static testsuite.internal.TestSuiteShardSpec.NO_SHARDS;

public class TransactionUnShardTest extends TransactionTest {
    @Override
    public void testLoadDriver() throws Exception {
        conn = getConnection(Driver.of(NO_SHARDS));
    }

    @Override
    public List<Connection> getExecuteBatchConnection() throws SQLException {
        List<Connection> connectionList = new ArrayList<>();
        connectionList.add(this.conn);
        connectionList.add(DriverManager.getConnection(getConnectionUrl(Driver.of(NO_SHARDS)) + "&rewriteBatchedStatements=true"));
        return connectionList;
    }

    @Override
    public List<List<String>> ErrorInTransactionTestCase() {
        List<List<String>> testCase = new ArrayList<List<String>>() {{
            add(new ArrayList<String>() {{
                add("DELETE FROM user_unsharded;");
                add("INSERT INTO user_unsharded (name, textcol1, textcol2) VALUES ('%s', '1', '1');");
                add("INSERT INTO user_unsharded (name, textcol1, textcol2_) VALUES ('%s', '1', '1');");
                add("SELECT COUNT(*) FROM user_unsharded;");
            }});
            add(new ArrayList<String>() {{
                add("DELETE FROM plan_test;");
                add("INSERT INTO plan_test (f_tinyint, f_int, f_midint) VALUES (%s, 1, 1);");
                add("INSERT INTO plan_test (f_tinyint, f_int, f_midint_) VALUES (%s, 1, 1);");
                add("SELECT COUNT(*) FROM plan_test;");
            }});
        }};
        return testCase;
    }

    @Override
    public void testErrorInTransactionExecuteBatch(List<Connection> connList, String initSql, String sql, String errorSql, String checkSql) throws SQLException {
        for (Connection conn : connList) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(initSql);
                conn.setAutoCommit(false);

                for (int i = 0; i < 10; i++) {
                    stmt.addBatch(String.format(sql, i));
                }
                stmt.executeBatch();
                try {
                    stmt.addBatch(String.format(sql, 11));
                    stmt.addBatch(String.format(errorSql, 12));
                    stmt.executeBatch();
                } catch (SQLException ignored) {
                }
                ResultSet resultSet = stmt.executeQuery(checkSql);
                resultSet.next();
                Assert.assertEquals(11, resultSet.getInt(1));
                conn.rollback();

                stmt.addBatch(String.format(sql, 13));
                stmt.addBatch(String.format(sql, 14));
                stmt.executeBatch();
                conn.commit();
                conn.setAutoCommit(true);

                resultSet = stmt.executeQuery(checkSql);
                resultSet.next();
                Assert.assertEquals(2, resultSet.getInt(1));
            }
        }
    }
}
