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

import com.google.common.collect.Lists;
import com.jd.jdbc.VSchemaManager;
import com.jd.jdbc.api.VtApiServer;
import com.jd.jdbc.common.Constant;
import com.jd.jdbc.common.util.CollectionUtils;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.context.VtContext;
import com.jd.jdbc.discovery.HealthCheck;
import com.jd.jdbc.discovery.SecurityCenter;
import com.jd.jdbc.discovery.TopologyWatcherManager;
import com.jd.jdbc.monitor.ConnectionCollector;
import com.jd.jdbc.monitor.MonitorServer;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.srvtopo.ResilientServer;
import com.jd.jdbc.srvtopo.ResolvedShard;
import com.jd.jdbc.srvtopo.Resolver;
import com.jd.jdbc.srvtopo.ScatterConn;
import com.jd.jdbc.srvtopo.SrvTopo;
import com.jd.jdbc.srvtopo.TabletGateway;
import com.jd.jdbc.srvtopo.TxConn;
import com.jd.jdbc.tindexes.SplitTableUtil;
import com.jd.jdbc.topo.Topo;
import com.jd.jdbc.topo.TopoServer;
import com.jd.jdbc.util.KeyspaceUtil;
import com.jd.jdbc.util.threadpool.InitThreadPoolService;
import com.jd.jdbc.vindexes.hash.BinaryHash;
import io.prometheus.client.Histogram;
import io.vitess.proto.Topodata;
import io.vitess.proto.Vtgate;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class VitessDriver implements java.sql.Driver {
    private static final Log log = LogFactory.getLog(VitessDriver.class);

    private static final IContext globalContext = VtContext.withCancel(VtContext.background());

    private static final Histogram HISTOGRAM = ConnectionCollector.getConnectionHistogram();

    static {
        // Register ourselves with the DriverManager
        try {
            java.sql.DriverManager.registerDriver(new VitessDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Can't register driver!");
        }
    }

    static {
        SplitTableUtil.getTableIndexesMap();
        new BinaryHash();
    }

    private final ReentrantLock lock = new ReentrantLock();

    public Connection initConnect(String url, Properties info, boolean initOnly) throws SQLException {
        if (log.isDebugEnabled()) {
            log.debug("initConnect,url=" + url);
        }

        Properties prop = VitessJdbcUrlParser.parse(url, info);

        InitThreadPoolService.getInstance();

        try {
            SecurityCenter.INSTANCE.addCredential(prop);
            String defaultKeyspace = prop.getProperty(Constant.DRIVER_PROPERTY_SCHEMA);
            String tabletKeyspace = KeyspaceUtil.getTabletKeyspace(defaultKeyspace);

            String role = prop.getProperty(Constant.DRIVER_PROPERTY_ROLE_KEY, Constant.DRIVER_PROPERTY_ROLE_RW);
            if (!prop.containsKey(Constant.DRIVER_PROPERTY_ROLE_KEY)) {
                prop.put(Constant.DRIVER_PROPERTY_ROLE_KEY, role);
            }
            TopoServer topoServer = Topo.getTopoServer(Topo.TopoServerImplementType.TOPO_IMPLEMENTATION_ETCD2, "http://" + prop.getProperty("host") + ":" + prop.getProperty("port"));

            ResilientServer resilientServer = SrvTopo.newResilientServer(topoServer, "ResilientSrvTopoServer");

            List<String> cells = topoServer.getAllCells(globalContext);

            for (String cell : cells) {
                TopologyWatcherManager.INSTANCE.startWatch(globalContext, topoServer, cell, tabletKeyspace);
            }

            TabletGateway tabletGateway = TabletGateway.build(resilientServer);

            for (String cell : cells) {
                TopologyWatcherManager.INSTANCE.watch(globalContext, cell, tabletKeyspace);
            }

            String localCell = topoServer.getLocalCell(globalContext, topoServer, cells, tabletKeyspace);

            boolean masterFlag = role.equalsIgnoreCase(Constant.DRIVER_PROPERTY_ROLE_RW);
            List<Topodata.TabletType> tabletTypes = masterFlag
                ? Lists.newArrayList(Topodata.TabletType.MASTER)
                : Lists.newArrayList(Topodata.TabletType.REPLICA, Topodata.TabletType.RDONLY);
            tabletGateway.waitForTablets(globalContext, localCell, tabletKeyspace, tabletTypes);

            TxConn txConn = new TxConn(tabletGateway, Vtgate.TransactionMode.MULTI);
            ScatterConn scatterConn = ScatterConn.newScatterConn("VttabletCall", txConn, tabletGateway);
            Resolver resolver = new Resolver(resilientServer, tabletGateway, localCell, scatterConn);
            Topodata.TabletType tabletType = VitessJdbcProperyUtil.getTabletType(prop);

            List<ResolvedShard> resolvedShardList = resolver.getAllShards(globalContext, tabletKeyspace, Topodata.TabletType.MASTER).getResolvedShardList();
            int shardNumber = CollectionUtils.isEmpty(resolvedShardList) ? 0 : resolvedShardList.size();
            Config.setConfig(prop, defaultKeyspace, SecurityCenter.INSTANCE.getCredential(defaultKeyspace).getUser(), tabletType, shardNumber);
            if (masterFlag) {
                HealthCheck.INSTANCE.initConnectionPool(defaultKeyspace);
            }
            if (initOnly) {
                return null;
            }

            MonitorServer.getInstance().register(defaultKeyspace);
            VSchemaManager vSchemaManager = VSchemaManager.getInstance(topoServer);
            vSchemaManager.initVschema(defaultKeyspace);
            VtApiServer apiServer = VtApiServer.getInstance();
            if (apiServer != null) {
                apiServer.register(defaultKeyspace, vSchemaManager);
            } else {
                if (log.isWarnEnabled()) {
                    log.warn("cannot get apiServer, register:" + defaultKeyspace + "skipped.");
                }
            }
            return new VitessConnection(url, prop, topoServer, resolver, vSchemaManager, defaultKeyspace);
        } catch (SQLException e) {
            throw e;
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
}