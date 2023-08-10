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

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.jd.jdbc.pool.StatefulConnectionPool;
import com.jd.jdbc.util.InnerConnectionPoolUtil;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.vitess.proto.Topodata;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class VitessDriverConnectionPoolTest extends TestSuite {
    protected static String driverName = "com.jd.jdbc.vitess.VitessDriver";

    protected String url;

    private StatefulConnectionPool statefulConnectionPool;

    private Properties props;

    private Random random;

    private String vtMinimumIdle;

    private String vtMaximumPoolSize;

    private String vtConnectionTimeout;

    private String vtIdleTimeout;

    private String propsUrl;

    private String user;

    private String password;

    @BeforeClass
    public static void beforeClass() throws NoSuchFieldException, IllegalAccessException {
        InnerConnectionPoolUtil.clearAll();
    }

    @Before
    public void init() {
        url = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        user = getUser(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        password = getPassword(Driver.of(TestSuiteShardSpec.TWO_SHARDS));

        url = url.replaceAll("user", "nouser").replaceAll("password", "nopassword");

        random = new Random();
        vtMinimumIdle = (random.nextInt(10) + 1) + "";
        vtMaximumPoolSize = (random.nextInt(150) + 1) + "";
        vtConnectionTimeout = (random.nextInt(50000) + 250) + "";
        vtIdleTimeout = (random.nextInt(600000) + 10000) + "";
        propsUrl = "vtMinimumIdle=" + vtMinimumIdle + ";vtMaximumPoolSize=" + vtMaximumPoolSize + ";vtConnectionInitSql=select 1;vtConnectionTestQuery=select 1;vtConnectionTimeout=" + vtConnectionTimeout + ";vtIdleTimeout=" + vtIdleTimeout;
    }

    public void test0() throws NoSuchFieldException, IllegalAccessException {
        Class<StatefulConnectionPool> statefulConnectionPoolClass = StatefulConnectionPool.class;
        Field getStatefulConnectionPoolMap = statefulConnectionPoolClass.getDeclaredField("STATEFUL_CONNECTION_POOL_MAP");
        getStatefulConnectionPoolMap.setAccessible(true);
        Map<String, Map<String, StatefulConnectionPool>> map = (Map<String, Map<String, StatefulConnectionPool>>) getStatefulConnectionPoolMap.get(null);
        map.forEach((s, stringStatefulConnectionPoolMap) -> stringStatefulConnectionPoolMap.forEach((s1, statefulConnectionPool1) -> statefulConnectionPool = statefulConnectionPool1));


        HikariConfig hikariConfig = statefulConnectionPool.getHikariPool().config;
        Assert.assertEquals(vtMinimumIdle, hikariConfig.getMinimumIdle() + "");
        Assert.assertEquals(vtMaximumPoolSize, hikariConfig.getMaximumPoolSize() + "");
        Assert.assertEquals("select 1", hikariConfig.getConnectionInitSql() + "");
        Assert.assertEquals("select 1", hikariConfig.getConnectionTestQuery() + "");
        Assert.assertEquals(vtConnectionTimeout, hikariConfig.getConnectionTimeout() + "");
        Assert.assertEquals(vtIdleTimeout, hikariConfig.getIdleTimeout() + "");
    }

    public void testInit() {
        props = new Properties();
        props.setProperty("vtMinimumIdle", vtMinimumIdle);
        props.setProperty("vtMaximumPoolSize", vtMaximumPoolSize);
        props.setProperty("vtConnectionInitSql", "select 1");
        props.setProperty("vtConnectionTestQuery", "select 1");
        props.setProperty("vtConnectionTimeout", vtConnectionTimeout);
        props.setProperty("vtIdleTimeout", vtIdleTimeout);
    }

    @Test
    public void testHikari() throws Exception {
        print();
        testInit();

        HikariConfig config = new HikariConfig();
        config.setDriverClassName(driverName);
        config.setJdbcUrl(url);
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(5);
        config.setUsername(user);
        config.setPassword(password);
        config.setDataSourceProperties(props);

        HikariDataSource hikariDataSource = new HikariDataSource(config);
        Connection conn = hikariDataSource.getConnection();
        executeSelect(conn);
        test0();
        InnerConnectionPoolUtil.removeInnerConnectionConfig(conn);
        conn.close();
    }

    @Test
    public void testDruid1() throws Exception {
        print();
        testInit();

        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(driverName);
        dataSource.setUrl(url);

        dataSource.setConnectProperties(props);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        dataSource.setPoolPreparedStatements(true);
        dataSource.setMaxPoolPreparedStatementPerConnectionSize(10);

        DruidPooledConnection connection = dataSource.getConnection();
        executeSelect(connection);
        test0();
        InnerConnectionPoolUtil.removeInnerConnectionConfig(connection);
        connection.close();
    }

    @Test
    public void testDruid2() throws Exception {
        print();
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(driverName);
        dataSource.setUrl(url);

        dataSource.setConnectionProperties(propsUrl);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        DruidPooledConnection connection = dataSource.getConnection();
        executeSelect(connection);
        test0();
        InnerConnectionPoolUtil.removeInnerConnectionConfig(connection);
        connection.close();
    }

    @Test
    public void testdbcp() throws Exception {
        print();
        BasicDataSource ds = new BasicDataSource();
        ds.setUrl(url);
        ds.setDriverClassName(driverName);

        ds.setConnectionProperties(propsUrl);
        ds.setUsername(user);
        ds.setPassword(password);
        Connection connection = ds.getConnection();
        executeSelect(connection);
        test0();
        InnerConnectionPoolUtil.removeInnerConnectionConfig(connection);
        connection.close();
    }

    @Test
    public void testc3p0() throws Exception {
        print();
        testInit();

        ComboPooledDataSource comboPooledDataSource = new ComboPooledDataSource();
        comboPooledDataSource.setJdbcUrl(url);

        comboPooledDataSource.setDriverClass(driverName);
        comboPooledDataSource.setProperties(props);
        comboPooledDataSource.setUser(user);
        comboPooledDataSource.setPassword(password);

        Connection connection = comboPooledDataSource.getConnection();
        executeSelect(connection);
        test0();
        InnerConnectionPoolUtil.removeInnerConnectionConfig(connection);
        connection.close();
    }

    @Test
    public void testTomcat() throws Exception {
        print();
        DataSource dataSource = new DataSource();
        dataSource.setUrl(url);
        dataSource.setDriverClassName(driverName);

        dataSource.setConnectionProperties(propsUrl);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        Connection connection = dataSource.getConnection();
        executeSelect(connection);
        test0();
        InnerConnectionPoolUtil.removeInnerConnectionConfig(connection);
        connection.close();
    }

    @Test
    public void testDefaultConnectionPoolSize() throws NoSuchFieldException, IllegalAccessException, SQLException, NoSuchMethodException, InvocationTargetException, InterruptedException {
        printNormal("testDefaultConnectionPoolSize<<<<<");
        String url3 = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS)) + "&vtMinimumIdle=6&vtMaximumPoolSize=7";
        checkInnerPoolSize(url3, Topodata.TabletType.MASTER, 6, 7);

        String url4 = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        checkInnerPoolSize(url4, Topodata.TabletType.MASTER, 5, 10);

        String url5 = getConnectionUrl(Driver.of(TestSuiteShardSpec.NO_SHARDS)) + "&vtMinimumIdle=7&vtMaximumPoolSize=8";
        checkInnerPoolSize(url5, Topodata.TabletType.MASTER, 7, 8);

        String url6 = getConnectionUrl(Driver.of(TestSuiteShardSpec.NO_SHARDS));
        checkInnerPoolSize(url6, Topodata.TabletType.MASTER, 5, 10);
        printNormal("testDefaultConnectionPoolSize>>>>>");
    }

    public void checkInnerPoolSize(String url, Topodata.TabletType type, int expectedMin, int expectedMax) throws SQLException, NoSuchFieldException, IllegalAccessException {
        printInfo("url: " + url);

        Connection conn = DriverManager.getConnection(url);
        executeSelect(conn);

        Map<String, Map<String, StatefulConnectionPool>> map = getStatefulConnectionPoolMap();
        Map<String, StatefulConnectionPool> v1 = map.get(conn.getCatalog());
        Assert.assertNotNull(v1);

        for (Map.Entry<String, StatefulConnectionPool> entry : v1.entrySet()) {
            StatefulConnectionPool pool = entry.getValue();
            if (isTabletType(pool, type)) {
                int minimumIdle = pool.getHikariPool().config.getMinimumIdle();
                int maximumPoolSize = pool.getHikariPool().config.getMaximumPoolSize();
                Assert.assertEquals(expectedMin, minimumIdle);
                Assert.assertEquals(expectedMax, maximumPoolSize);
            }
        }
        printOk("[OK]");

        InnerConnectionPoolUtil.removeInnerConnectionConfig(conn);
        conn.close();
    }

    private void print() {
        printNormal("url: " + url);
        printNormal("props: " + propsUrl);
    }

    private void executeSelect(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.executeQuery("select * from engine_test limit 1");
        stmt.close();
    }

    public Map<String, Map<String, StatefulConnectionPool>> getStatefulConnectionPoolMap() throws NoSuchFieldException, IllegalAccessException {
        Field field = StatefulConnectionPool.class.getDeclaredField("STATEFUL_CONNECTION_POOL_MAP");
        field.setAccessible(true);
        Map<String, Map<String, StatefulConnectionPool>> map = (Map<String, Map<String, StatefulConnectionPool>>) field.get(null);
        return map;
    }

    public boolean isTabletType(StatefulConnectionPool pool, Topodata.TabletType type) throws NoSuchFieldException, IllegalAccessException {
        Field field = StatefulConnectionPool.class.getDeclaredField("tablet");
        field.setAccessible(true);
        Topodata.Tablet tablet = (Topodata.Tablet) field.get(pool);
        return Objects.equals(type, tablet.getType());
    }
}
