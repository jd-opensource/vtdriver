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
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.Assert;
import org.junit.Before;
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

    @Before
    public void init() {
        url = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        url = url.replaceAll("user", "nouser").replaceAll("password", "nopassword");

        random = new Random();
        vtMinimumIdle = (random.nextInt(10) + 1) + "";
        vtMaximumPoolSize = (random.nextInt(150) + 1) + "";
        vtConnectionTimeout = (random.nextInt(50000) + 250) + "";
        vtIdleTimeout = (random.nextInt(600000) + 10000) + "";

        propsUrl =
            "vtMinimumIdle=" + vtMinimumIdle + ";vtMaximumPoolSize=" + vtMaximumPoolSize + ";vtConnectionInitSql=select 1;vtConnectionTestQuery=select 1;vtConnectionTimeout=" + vtConnectionTimeout +
                ";vtIdleTimeout=" + vtIdleTimeout;

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
        testInit();

        HikariConfig config = new HikariConfig();
        config.setDriverClassName(driverName);
        config.setJdbcUrl(url);
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(5);
        config.setUsername("vtdriver");
        config.setPassword("vtdriver_password");
        config.setDataSourceProperties(props);

        HikariDataSource hikariDataSource = new HikariDataSource(config);

        test0();
    }


    @Test
    public void testDruid1() throws Exception {
        testInit();

        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(driverName);
        dataSource.setUrl(url);

        dataSource.setConnectProperties(props);
        dataSource.setUsername("vtdriver");
        dataSource.setPassword("vtdriver_password");
        dataSource.setPoolPreparedStatements(true);
        dataSource.setMaxPoolPreparedStatementPerConnectionSize(10);

        DruidPooledConnection connection = dataSource.getConnection();

        test0();
        connection.close();
    }

    @Test
    public void testDruid2() throws Exception {
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setDriverClassName(driverName);
        dataSource.setUrl(url);

        dataSource.setConnectionProperties(propsUrl);
        dataSource.setUsername("vtdriver");
        dataSource.setPassword("vtdriver_password");
        DruidPooledConnection connection = dataSource.getConnection();

        test0();
        connection.close();
    }


    @Test
    public void testdbcp() throws Exception {
        BasicDataSource ds = new BasicDataSource();
        ds.setUrl(url);
        ds.setDriverClassName(driverName);

        ds.setConnectionProperties(propsUrl);
        ds.setUsername("vtdriver");
        ds.setPassword("vtdriver_password");
        Connection connection = ds.getConnection();

        test0();
        connection.close();
    }


    @Test
    public void testc3p0() throws Exception {
        testInit();

        ComboPooledDataSource comboPooledDataSource = new ComboPooledDataSource();
        comboPooledDataSource.setJdbcUrl(url);

        comboPooledDataSource.setDriverClass(driverName);
        comboPooledDataSource.setProperties(props);
        comboPooledDataSource.setUser("vtdriver");
        comboPooledDataSource.setPassword("vtdriver_password");

        Connection connection = comboPooledDataSource.getConnection();

        test0();
        connection.close();
    }

    @Test
    public void testTomcat() throws Exception {
        DataSource dataSource = new DataSource();
        dataSource.setUrl(url);
        dataSource.setDriverClassName(driverName);

        dataSource.setConnectionProperties(propsUrl);
        dataSource.setUsername("vtdriver");
        dataSource.setPassword("vtdriver_password");
        Connection connection = dataSource.getConnection();

        test0();
        connection.close();
    }
}
