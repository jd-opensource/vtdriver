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

public final class MonitorServer {
    private static final Log log = LogFactory.getLog(MonitorServer.class);

    private static final int DEFAULT_PROMETHEUS_SERVER_PORT = 15001;

    private static final CollectorRegistry collectorRegistry = new CollectorRegistry();

    private static volatile MonitorServer INSTANCE;

    private static Integer port;

    private static HTTPServer server;

    static {
        String monitorPort = System.getProperty("vtdriver.monitor.port");
        if (!StringUtils.isEmpty(monitorPort)) {
            try {
                port = Integer.parseInt(monitorPort);
            } catch (NumberFormatException ex) {
                log.error("get monitor.port error!" + ex.getMessage());
            }
        }
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(MonitorServer::stop));
    }

    private MonitorServer() {
        initHttpServer();
        if (server == null) {
            throw new VitessRuntimeException("MonitorServer:init prometheus HTTPServer error");
        }
        ThreadPoolCollector.getInstance().register(collectorRegistry);
        HealthCheckCollector.getInstance().register(collectorRegistry);
        SqlErrorCollector.getInstance().register(collectorRegistry);
        SrvKeyspaceCollector.getInstance().register(collectorRegistry);
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
            server.stop();
            log.info(" Stop monitorServer succeed");
        }
    }

    public static CollectorRegistry getCollectorRegistry() {
        return collectorRegistry;
    }

    private void initHttpServer() {
        if (port != null) {
            try {
                server = new HTTPServer(new InetSocketAddress(port), collectorRegistry);
            } catch (IOException e) {
                log.error("Create MONITOR server on " + port + " failed!");
            }
            return;
        }
        int retryCount = 0;
        port = DEFAULT_PROMETHEUS_SERVER_PORT;
        while (retryCount < 20) {
            try {
                server = new HTTPServer(new InetSocketAddress(port), collectorRegistry);
            } catch (IOException e) {
                log.info("Create MONITOR server on " + port + " failed!");
                port += 2;
            }
            retryCount++;
            if (server != null) {
                break;
            }
        }
    }
}
