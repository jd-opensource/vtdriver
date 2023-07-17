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

package com.jd.jdbc.pool;

import com.jd.jdbc.discovery.HealthCheck;
import com.jd.jdbc.discovery.SecurityCenter;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.topo.topoproto.TopoProto;
import com.jd.jdbc.common.util.MapUtil;
import com.jd.jdbc.vitess.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.pool.HikariPool;
import io.vitess.proto.Topodata;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

public class StatefulConnectionPool {

    private static final Log logger = LogFactory.getLog(StatefulConnectionPool.class);

    private static final Map<String, Map<String, StatefulConnectionPool>> STATEFUL_CONNECTION_POOL_MAP = new ConcurrentHashMap<>(128 + 1);

    private final HikariPool hikariPool;

    private final Numbered active;

    private final AtomicLong lastID;

    private final Topodata.Tablet tablet;

    private StatefulConnectionPool(final Topodata.Tablet tablet, final String user, final String password, final Properties dsProperties, final Properties properties) {
        this.tablet = tablet;

        HikariConfig hikariConfig = HikariUtil.getHikariConfig(tablet, user, password, properties, dsProperties);
        hikariPool = new HikariPool(hikariConfig);
        active = new Numbered();
        lastID = new AtomicLong(java.lang.System.nanoTime());
    }

    public static StatefulConnectionPool getStatefulConnectionPool(final Topodata.Tablet tablet, final String user, final String password, final Properties dsProperties, final Properties properties) {
        StatefulConnectionPool statefulConnectionPool = getStatefulConnectionPool(tablet);
        if (statefulConnectionPool != null) {
            return statefulConnectionPool;
        }
        synchronized (StatefulConnectionPool.class) {
            statefulConnectionPool = getStatefulConnectionPool(tablet);
            if (statefulConnectionPool != null) {
                return statefulConnectionPool;
            }
            StatefulConnectionPool pool = new StatefulConnectionPool(tablet, user, password, dsProperties, properties);
            MapUtil.computeIfAbsent(STATEFUL_CONNECTION_POOL_MAP, tablet.getKeyspace(), map -> new ConcurrentHashMap<>(16)).put(TopoProto.tabletAliasString(tablet.getAlias()), pool);
            return pool;
        }
    }

    public static void shutdown(final Topodata.Tablet tablet) {
        Map<String, StatefulConnectionPool> map = STATEFUL_CONNECTION_POOL_MAP.get(tablet.getKeyspace());
        StatefulConnectionPool statefulConnectionPool = null;
        if (map != null && !map.isEmpty()) {
            statefulConnectionPool = map.remove(TopoProto.tabletAliasString(tablet.getAlias()));
            if (statefulConnectionPool != null) {
                statefulConnectionPool.close();
            }
            if (map.isEmpty()) {
                STATEFUL_CONNECTION_POOL_MAP.remove(tablet.getKeyspace());
            }
        }
    }

    public static InnerConnection getJdbcConnection(final String keyspace, final Topodata.TabletType tabletType) throws SQLException {
        if (StringUtils.isEmpty(keyspace) || tabletType == null) {
            throw new IllegalArgumentException("keyspace or tabletType should not be null");
        }
        if (STATEFUL_CONNECTION_POOL_MAP.isEmpty() || !STATEFUL_CONNECTION_POOL_MAP.containsKey(keyspace)) {
            synchronized (StatefulConnectionPool.class) {
                if (STATEFUL_CONNECTION_POOL_MAP.isEmpty() || !STATEFUL_CONNECTION_POOL_MAP.containsKey(keyspace)) {
                    Topodata.Tablet tablet = HealthCheck.INSTANCE.getHealthyTablets(keyspace, tabletType);
                    if (tablet == null) {
                        throw new SQLException("connot find tablet");
                    }
                    StatefulConnectionPool statefulConnectionPool = getStatefulConnectionPool(keyspace, tablet);
                    return statefulConnectionPool.getNoStatefulConn();
                }
            }
        }
        Map<String, StatefulConnectionPool> map = STATEFUL_CONNECTION_POOL_MAP.get(keyspace);
        int random = map.size() == 1 ? 0 : ThreadLocalRandom.current().nextInt(0, map.size() / 2 + 1);
        int i = 0;
        for (Map.Entry<String, StatefulConnectionPool> entry : map.entrySet()) {
            StatefulConnectionPool pool = entry.getValue();
            if (pool == null) {
                continue;
            }
            if (i++ < random) {
                continue;
            }
            try {
                return pool.getNoStatefulConn(2000L);
            } catch (SQLException e) {
                logger.error("getNoStatefulConn error.causeby:" + e.getMessage());
            }
        }
        throw new SQLException("cannot find the Connection for the keyspace:" + keyspace);
    }

    private static StatefulConnectionPool getStatefulConnectionPool(final Topodata.Tablet tablet) {
        if (STATEFUL_CONNECTION_POOL_MAP.containsKey(tablet.getKeyspace())) {
            Map<String, StatefulConnectionPool> map = STATEFUL_CONNECTION_POOL_MAP.get(tablet.getKeyspace());
            if (map != null && !map.isEmpty() && map.containsKey(TopoProto.tabletAliasString(tablet.getAlias()))) {
                return map.get(TopoProto.tabletAliasString(tablet.getAlias()));
            }
        }
        return null;
    }

    public static StatefulConnectionPool getStatefulConnectionPool(final String keyspace, final Topodata.Tablet tablet) {
        String user = SecurityCenter.INSTANCE.getCredential(keyspace).getUser();
        String password = SecurityCenter.INSTANCE.getCredential(keyspace).getPassword();
        Properties properties = Config.getConnectionPoolConfig(tablet.getKeyspace(), user, tablet.getType());
        Properties dsProperties = Config.getDataSourceConfig(tablet.getKeyspace(), user, tablet.getType());
        return StatefulConnectionPool.getStatefulConnectionPool(tablet, user, password, dsProperties, properties);
    }

    public InnerConnection getNoStatefulConn() throws SQLException {
        Connection connection = hikariPool.getConnection();
        return new InnerConnection(connection);
    }

    protected InnerConnection getNoStatefulConn(long hardTimeout) throws SQLException {
        Connection connection = hikariPool.getConnection(hardTimeout);
        return new InnerConnection(connection);
    }

    // GetAndLock locks the connection for use. It accepts a purpose as a string.
    public StatefulConnection getAndLock(long id, String reason) throws SQLException {
        return active.get(id, reason);
    }

    //NewConn creates a new StatefulConnection.
    public StatefulConnection newConn(boolean enforceTimeout) throws SQLException {
        InnerConnection connection = getNoStatefulConn();
        long connID = lastID.addAndGet(1);
        StatefulConnection conn = new StatefulConnection(this, connection, connID, false, enforceTimeout);
        if (!active.register(connID, conn, false)) {
            conn.release();
            throw new SQLException(TopoProto.tabletToHumanString(tablet) + " stateful connection pool duplicated connection id: " + connID);
        }

        return getAndLock(connID, "new connection");
    }

    private void close() {
        try {
            hikariPool.shutdown();
        } catch (Exception e) {
            logger.error(TopoProto.tabletToHumanString(tablet) + " StatefulConnectionPool close error " + e.getMessage(), e);
        }
    }

    // Unregister forgets the specified connection.  If the connection is not present, it's ignored.
    public void unregister(long id) {
        active.unregister(id);
    }

    //markAsNotInUse marks the connection as not in use at the moment
    public void markAsNotInUse(long id, boolean updateTime) {
        active.put(id, updateTime);
    }

    protected Topodata.Tablet getTablet() {
        return tablet;
    }

    public HikariPool getHikariPool() {
        return hikariPool;
    }
}
