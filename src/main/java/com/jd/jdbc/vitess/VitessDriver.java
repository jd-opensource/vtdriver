/*
Copyright 2021 JD Project Authors. Licensed under Apache-2.0.

Copyright 2019 The Vitess Authors.

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

import com.jd.jdbc.VSchemaManager;
import com.jd.jdbc.api.VtApiServer;
import com.jd.jdbc.api.handler.VtVschemaRefreshHandler;
import com.jd.jdbc.common.Constant;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.discovery.SecurityCenter;
import com.jd.jdbc.discovery.TopologyWatcherManager;
import com.jd.jdbc.monitor.ConnectionCollector;
import com.jd.jdbc.monitor.MonitorServer;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.Utils;
import com.jd.jdbc.srvtopo.ResilientServer;
import com.jd.jdbc.srvtopo.Resolver;
import com.jd.jdbc.srvtopo.ScatterConn;
import com.jd.jdbc.srvtopo.SrvTopo;
import com.jd.jdbc.srvtopo.TabletGateway;
import com.jd.jdbc.srvtopo.TxConn;
import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.tindexes.SplitTableUtil;
import com.jd.jdbc.topo.Topo;
import com.jd.jdbc.topo.TopoException;
import com.jd.jdbc.topo.TopoExceptionCode;
import com.jd.jdbc.topo.TopoServer;
import com.jd.jdbc.util.Config;
import com.jd.jdbc.util.threadpool.impl.VtDaemonExecutorService;
import com.jd.jdbc.util.threadpool.impl.VtHealthCheckExecutorService;
import com.jd.jdbc.util.threadpool.impl.VtQueryExecutorService;
import io.prometheus.client.Histogram;
import io.vitess.proto.Topodata;
import io.vitess.proto.Vtgate;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import static com.jd.jdbc.common.Constant.DRIVER_PROPERTY_ROLE_KEY;
import static com.jd.jdbc.common.Constant.DRIVER_PROPERTY_ROLE_RW;

public class VitessDriver implements java.sql.Driver {
    private static final Log log = LogFactory.getLog(VitessDriver.class);

    private static final IContext globalContext = VtContext.withCancel(VtContext.background());

    private static final Histogram HISTOGRAM = ConnectionCollector.getConnectionHistogram();

    private static final MonitorServer monitorServer;

    private static VtApiServer apiServer;

    static {
        // Register ourselves with the DriverManager
        try {
            java.sql.DriverManager.registerDriver(new VitessDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Can't register driver!");
        }
        monitorServer = MonitorServer.getInstance();
    }

    static {
        Map<String, Map<String, LogicTable>> tableIndexesMap = SplitTableUtil.getTableIndexesMap(Constant.DEFAULT_SPLIT_TABLE_CONFIG_PATH);
        VitessDataSource.setTableIndexesMap(tableIndexesMap);
    }

    private final ReentrantLock lock = new ReentrantLock();

    private volatile boolean inited = false;

    public Connection initConnect(String url, Properties info, boolean initOnly) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("initConnect,url=" + url);
        }

        Properties prop = VitessJdbcUrlParser.parse(url, info);

        this.lock.lock();
        try {
            initializePoolSize(prop);
        } finally {
            this.lock.unlock();
        }

        try {
            List<String> keySpaces = Arrays.asList(prop.getProperty("schema").split(","));
            SecurityCenter.INSTANCE.addCredential(prop);
            String defaultKeyspace = keySpaces.get(0);

            Config.setUrlConfig(prop, defaultKeyspace, SecurityCenter.INSTANCE.getCredential(defaultKeyspace).getUser());

            TopoServer topoServer = Topo.getTopoServer(Topo.TopoServerImplementType.TOPO_IMPLEMENTATION_ETCD2, "http://" + prop.getProperty("host") + ":" + prop.getProperty("port"));
            ResilientServer resilientServer = SrvTopo.newResilientServer(topoServer, "ResilientSrvTopoServer");

            List<String> cells = Arrays.asList(prop.getProperty("cell").split(","));
            String localCell = cells.get(0);
            try {
                topoServer.getSrvKeyspace(globalContext, localCell, defaultKeyspace);
            } catch (TopoException e) {
                if (e.getCode() == TopoExceptionCode.NO_NODE) {
                    localCell = cells.get(1);
                }
            }
            Set<String> ksSet = new HashSet<>(keySpaces);

            for (String cell : cells) {
                TopologyWatcherManager.INSTANCE.startWatch(globalContext, topoServer, cell, ksSet);
            }

            TabletGateway tabletGateway = TabletGateway.build(resilientServer);

            for (String cell : cells) {
                TopologyWatcherManager.INSTANCE.watch(globalContext, cell, ksSet);
            }

            String role = prop.getProperty(DRIVER_PROPERTY_ROLE_KEY, DRIVER_PROPERTY_ROLE_RW);
            if (!prop.containsKey(DRIVER_PROPERTY_ROLE_KEY)) {
                prop.put(DRIVER_PROPERTY_ROLE_KEY, role);
            }
            List<Topodata.TabletType> tabletTypes = role.equalsIgnoreCase(DRIVER_PROPERTY_ROLE_RW) ?
                new ArrayList<Topodata.TabletType>() {{
                    add(Topodata.TabletType.MASTER);
                }} :
                new ArrayList<Topodata.TabletType>() {{
                    add(Topodata.TabletType.REPLICA);
                    add(Topodata.TabletType.RDONLY);
                }};

            tabletGateway.waitForTablets(globalContext, localCell, ksSet, tabletTypes);
            TxConn txConn = new TxConn(tabletGateway, Vtgate.TransactionMode.MULTI);
            ScatterConn scatterConn = ScatterConn.newScatterConn("VttabletCall", txConn, tabletGateway);
            Resolver resolver = new Resolver(resilientServer, tabletGateway, localCell, scatterConn);

            if (initOnly) {
                return null;
            }

            /**
             * sharded
             * */
            VSchemaManager vSchemaManager = VSchemaManager.getInstance(topoServer);
            vSchemaManager.initVschema(ksSet);

            if (apiServer == null) {
                String uniquePrefix = "/" + prop.getProperty("host") + "/" + prop.getProperty("schema");
                apiServer = VtApiServer.getInstance(uniquePrefix);
                if (apiServer != null) {
                    apiServer.regiest("/vschema/refresh", new VtVschemaRefreshHandler(vSchemaManager));
                    apiServer.start();
                }
            }
            return new VitessConnection(url, prop, topoServer, resolver, ksSet, vSchemaManager, defaultKeyspace);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Histogram.Timer histogramTimer = HISTOGRAM.labels("connect").startTimer();
        if (!acceptsURL(url)) {
            return null;
        }

        try {
            return initConnect(url, info, false);
        } finally {
            if (histogramTimer != null) {
                histogramTimer.observeDuration();
            }
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith(VitessJdbcUrlParser.JDBC_VITESS_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        Properties prop = new Properties(info);

        List<DriverPropertyInfo> result = new ArrayList<DriverPropertyInfo>();
        info.forEach((k, v) -> {
            DriverPropertyInfo propInfo = new DriverPropertyInfo(String.valueOf(k), String.valueOf(v));
            if ("host,port".contains(String.valueOf(k))) {
                propInfo.required = true;
            }
            result.add(propInfo);
        });

        return result.toArray(new DriverPropertyInfo[0]);
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    private void initializePoolSize(Properties prop) {
        if (this.inited) {
            return;
        }

        Integer daemonCorePoolSize = Utils.getInteger(prop, "daemonCoreSize");
        Integer daemonMaximumPoolSize = Utils.getInteger(prop, "daemonMaximumSize");
        Long daemonRejectedExecutionTimeoutMillis = Utils.getLong(prop, "daemonRejectedTimeout");
        VtDaemonExecutorService.initialize(daemonCorePoolSize, daemonMaximumPoolSize, daemonRejectedExecutionTimeoutMillis);

        Integer queryCorePoolSize = Utils.getInteger(prop, "queryCoreSize");
        Integer queryMaximumPoolSize = Utils.getInteger(prop, "queryMaximumSize");
        Integer queryQueueSize = Utils.getInteger(prop, "queryQueueSize");
        Long queryRejectedExecutionTimeoutMillis = Utils.getLong(prop, "queryRejectedTimeout");
        VtQueryExecutorService.initialize(queryCorePoolSize, queryMaximumPoolSize, queryQueueSize, queryRejectedExecutionTimeoutMillis);

        Integer healthCheckCorePoolSize = Utils.getInteger(prop, "healthCheckCoreSize");
        Integer healthCheckMaximumPoolSize = Utils.getInteger(prop, "healthCheckMaximumSize");
        Integer healthCheckQueueSize = Utils.getInteger(prop, "healthCheckQueueSize");
        Long healthCheckRejectedExecutionTimeoutMillis = Utils.getLong(prop, "healthCheckRejectedTimeout");
        VtHealthCheckExecutorService.initialize(healthCheckCorePoolSize, healthCheckMaximumPoolSize, healthCheckQueueSize, healthCheckRejectedExecutionTimeoutMillis);

        this.inited = true;
    }
}
