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

package com.jd.jdbc.pool;

import com.jd.jdbc.common.Constant;
import com.jd.jdbc.monitor.MonitorServer;
import com.jd.jdbc.monitor.ThreadPoolCollector;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.topo.topoproto.TopoProto;
import com.jd.jdbc.util.KeyspaceUtil;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.metrics.prometheus.PrometheusHistogramMetricsTrackerFactory;
import com.zaxxer.hikari.pool.HikariProxyConnection;
import com.zaxxer.hikari.pool.ProxyConnection;
import com.zaxxer.hikari.util.UtilityElf;
import io.vitess.proto.Topodata;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

public class HikariUtil {
    private static final Log logger = LogFactory.getLog(HikariUtil.class);

    private static final ScheduledThreadPoolExecutor HOUSEKEEPER_EXECUTOR;

    private static final MetricsTrackerFactory METRICS_TRACKER_FACTORY;

    static {
        ThreadFactory threadFactory = new UtilityElf.DefaultThreadFactory("housekeeper", true);
        HOUSEKEEPER_EXECUTOR = new ScheduledThreadPoolExecutor(1, threadFactory, new ThreadPoolExecutor.DiscardPolicy());
        HOUSEKEEPER_EXECUTOR.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        HOUSEKEEPER_EXECUTOR.setRemoveOnCancelPolicy(true);
        ThreadPoolCollector.getInstance().add("housekeeper", HOUSEKEEPER_EXECUTOR);
        // set Prometheus Monitor
        METRICS_TRACKER_FACTORY = new PrometheusHistogramMetricsTrackerFactory(MonitorServer.getCollectorRegistry());
    }

    public static HikariConfig getHikariConfig(Topodata.Tablet tablet, String user, String password, Properties properties, Properties dsProperties) {
        HikariConfig hikariConfig;
        if (properties != null) {
            hikariConfig = new HikariConfig(properties);
        } else {
            hikariConfig = new HikariConfig();
            hikariConfig.setConnectionTestQuery("select 1");
            hikariConfig.setConnectionTimeout(10_000);
            hikariConfig.setMinimumIdle(5);
            hikariConfig.setMaximumPoolSize(10);
            hikariConfig.setConnectionInitSql("select 1");
        }

        hikariConfig.setDataSourceProperties(dsProperties);

        String realSchema = KeyspaceUtil.getRealSchema(tablet.getKeyspace());
        hikariConfig.setDriverClassName(Constant.MYSQL_PROTOCOL_DRIVER_CLASS);
        String nativeUrl = "jdbc:mysql://" + tablet.getMysqlHostname() + ":" + tablet.getMysqlPort() + "/" + realSchema;
        hikariConfig.setJdbcUrl(nativeUrl);
        hikariConfig.setPoolName(TopoProto.getPoolName(tablet));
        hikariConfig.setUsername(user);
        hikariConfig.setPassword(password);
        hikariConfig.setSchema(realSchema);
        hikariConfig.setScheduledExecutor(HOUSEKEEPER_EXECUTOR);
        hikariConfig.setMetricsTrackerFactory(METRICS_TRACKER_FACTORY);
        if (logger.isDebugEnabled()) {
            logger.debug("hikariConfig:nativeUrl=" + nativeUrl + " poolName=" + TopoProto.getPoolName(tablet));
        }
        return hikariConfig;
    }

    public static ConnectionImpl getConnectionImpl(Connection conn) throws SQLException {
        if (conn instanceof ProxyConnection) {
            ProxyConnection proxyConnection = (HikariProxyConnection) conn;
            return proxyConnection.unwrap(ConnectionImpl.class);
        }
        throw new SQLException("Connection not instanceof ProxyConnection!");
    }
}