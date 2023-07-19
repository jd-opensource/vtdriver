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

package com.jd.jdbc.monitor;

import com.jd.jdbc.sqlparser.parser.VitessRuntimeException;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.TreeSet;

public final class MonitorServer {
    private static final Log LOG = LogFactory.getLog(MonitorServer.class);

    private static final int DEFAULT_PROMETHEUS_SERVER_PORT = 15001;

    private static final CollectorRegistry COLLECTOR_REGISTRY = new CollectorRegistry();

    private static volatile MonitorServer INSTANCE;

    private static Integer port;

    private static HTTPServer server;

    private final Set<String> keyspaceSet = new TreeSet<>();

    static {
        String monitorPort = System.getProperty("vtdriver.monitor.port");
        if (!StringUtils.isEmpty(monitorPort)) {
            try {
                port = Integer.parseInt(monitorPort);
            } catch (NumberFormatException ex) {
                LOG.error("get monitor.port error!" + ex.getMessage());
            }
        }
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(MonitorServer::stop));
    }

    private MonitorServer() {
        new Thread(() -> {
            initHttpServer();
            if (server == null) {
                throw new VitessRuntimeException("MonitorServer:init prometheus HTTPServer error");
            }
            ThreadPoolCollector.getInstance().register(COLLECTOR_REGISTRY);
            HealthCheckCollector.getInstance().register(COLLECTOR_REGISTRY);
            HealthyCollector.getInstance().register(COLLECTOR_REGISTRY);
            SqlErrorCollector.getInstance().register(COLLECTOR_REGISTRY);
            SrvKeyspaceCollector.getInstance().register(COLLECTOR_REGISTRY);
            PlanCollector.PlanCacheSizeCollector.getInstance().register(COLLECTOR_REGISTRY);
        }).start();
    }

    public static MonitorServer getInstance() {
        if (INSTANCE == null) {
            synchronized (MonitorServer.class) {
                if (INSTANCE == null) {
                    INSTANCE = new MonitorServer();
                }
            }
        }
        return INSTANCE;
    }

    public static void stop() {
        if (server != null) {
            server.close();
            LOG.info(" Stop monitorServer succeed");
        }
    }

    public static CollectorRegistry getCollectorRegistry() {
        return COLLECTOR_REGISTRY;
    }

    private void initHttpServer() {
        if (port != null) {
            try {
                server = new HTTPServer(new InetSocketAddress(port), COLLECTOR_REGISTRY);
            } catch (IOException e) {
                LOG.error("Create MONITOR server on " + port + " failed!");
            }
            return;
        }
        int retryCount = 0;
        port = DEFAULT_PROMETHEUS_SERVER_PORT;
        while (retryCount < 20) {
            try {
                server = new HTTPServer(new InetSocketAddress(port), COLLECTOR_REGISTRY);
            } catch (IOException e) {
                LOG.info("Create MONITOR server on " + port + " failed!");
                port += 2;
            }
            retryCount++;
            if (server != null) {
                break;
            }
        }
    }

    public void register(final String keyspace) {
        if (!keyspaceSet.contains(keyspace)) {
            synchronized (this) {
                keyspaceSet.add(keyspace);
            }
        }
    }
}
