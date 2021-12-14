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

import com.jd.jdbc.key.Destination;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.srvtopo.BindVariable;
import com.jd.jdbc.srvtopo.Resolver;
import com.jd.jdbc.vindexes.hash.BinaryHash;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.vitess.proto.Topodata;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class VitessDriverCPTest extends TestSuite {
    protected static String driverName = "com.jd.jdbc.vitess.VitessDriver";

    private final Map<String, Integer> testSqlAndMaybeResult = new HashMap<>();

    protected String url;

    protected DataSource dataSource;

    @Before
    public void buildDataSourceAndAddData() {
        String innerPoolConfig = "&vtMinimumIdle=10&vtMaximumPoolSize=10" +
            "&vtConnectionInitSql=select&nbsp;2&vtConnectionTestQuery=select&nbsp;2&vtonnectionTimeout=30000" +
            "&vtIdleTimeout=60000";
        url = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS)) + innerPoolConfig;

        try {
            buildHikariDataSource();
            testExecuteDelete("delete from test;", 0, false);
            realRun();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test() throws Exception {
        VitessConnection connection = (VitessConnection) new VitessDataSource(url).getConnection();
        VitessPreparedStatement psmt = (VitessPreparedStatement) connection.prepareStatement("select * from test_vindex_timestamp where v_key = ?;");
        Date date = new Date();
        System.out.println(date);
        psmt.setTimestamp(1, new Timestamp(date.getTime()));
        BindVariable bindVariable = psmt.getClientPreparedQueryBindings().get(0).getBindVariableMap().get("0");
        Destination[] destinations = new BinaryHash().map(new VtValue[] {VtValue.newVtValue(bindVariable)});
        Resolver.ResolveDestinationResult rdr = connection.getResolver().resolveDestinations(null, connection.getDefaultKeyspace(),
            Topodata.TabletType.MASTER, null, Arrays.asList(destinations));
        System.out.println(rdr);

    }

    @Test
    public void testDelete() {
        testSqlAndMaybeResult.put("delete from test where f_tinyint = 1;", 1);
        testSqlAndMaybeResult.put("delete from test where f_tinyint between 1 and 2;", 1);
        testSqlAndMaybeResult.put("delete from test where f_utinyint in (1, 2);", 1);
        testSqlAndMaybeResult.put("delete from test where f_tinyint in (1, 2, 3);", 1);
        testSqlAndMaybeResult.put("delete from test where f_tinyint + 0 = 1;", 1); // change
        testSqlAndMaybeResult.put("delete from test where f_tinyint + 0 = 1 limit 0;", 0); // change
        testSqlAndMaybeResult.put("delete from test where f_tinyint + 0 = 1 limit 1;", 1); // change
        //testSqlAndMaybeResult.put("delete from test where f_tinyint=(select 1);", 1); // unsupported

        for (Map.Entry<String, Integer> entry : testSqlAndMaybeResult.entrySet()) {
            System.out.println(entry.getKey());
            try {
                testExecuteDelete(entry.getKey(), entry.getValue(), true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected void testExecuteDelete(final String sql, final int maybeResult, final boolean check) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement stmt = connection.createStatement()) {
            connection.setAutoCommit(false);
            try {
                int affectedRows = stmt.executeUpdate(sql);
                connection.commit();
                if (check) {
                    Assert.assertEquals(maybeResult, affectedRows);
                }
                if (maybeResult > 0) {
                    realRun();
                }
            } catch (Exception e) {
                e.printStackTrace();
                connection.rollback();
            }
        }
    }

    protected void buildHikariDataSource() {
        Properties props = new Properties();
        props.setProperty("useServerPrepStmts", Boolean.TRUE.toString());
        props.setProperty("cachePrepStmts", "true");
        props.setProperty("prepStmtCacheSize", "500");
        props.setProperty("prepStmtCacheSqlLimit", "2048");
        props.setProperty("useLocalSessionState", Boolean.TRUE.toString());
        props.setProperty("rewriteBatchedStatements", Boolean.TRUE.toString());
        props.setProperty("cacheResultSetMetadata", Boolean.TRUE.toString());
        props.setProperty("cacheServerConfiguration", Boolean.TRUE.toString());
        props.setProperty("elideSetAutoCommits", Boolean.TRUE.toString());
        props.setProperty("maintainTimeStats", Boolean.FALSE.toString());
        props.setProperty("netTimeoutForStreamingResults", "0");
        HikariConfig config = new HikariConfig();
        config.setDriverClassName(driverName);
        config.setJdbcUrl(url);
        config.setConnectionInitSql("select 1");
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(10);
        config.setDataSourceProperties(props);
        dataSource = new HikariDataSource(config);
    }

    protected void realRun() {
        try (Connection connection = dataSource.getConnection(); Statement stmt = connection.createStatement()) {
            connection.setAutoCommit(false);
            int affectedRows = 0;
            String sql = "insert into test (f_tinyint,f_utinyint,f_smallint) values(1,2,3);";
            try {
                affectedRows += stmt.executeUpdate(sql);
                connection.commit();
                Assert.assertEquals(1, affectedRows);
            } catch (Exception e) {
                connection.rollback();
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
}
