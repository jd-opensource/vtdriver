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

package com.jd.jdbc.api;

import com.jd.jdbc.api.handler.VtStatusHandler;
import com.jd.jdbc.monitor.PortCollector;
import com.jd.jdbc.sqlparser.parser.VitessRuntimeException;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;

public class VtApiServer {
    private static final Log LOGGER = LogFactory.getLog(VtApiServer.class);

    private static final int DEFAULT_PROMETHEUS_SERVER_PORT = 15002;

    public static String uniquePrefix;

    public static Integer port = null;

    private volatile static VtApiServer instance;

    private static HttpServer httpServer;

    static {
        String apiPort = System.getProperty("vtdriver.api.port");
        if (!StringUtils.isEmpty(apiPort)) {
            try {
                port = Integer.parseInt(apiPort);
            } catch (NumberFormatException ex) {
                LOGGER.error("get api.port error!" + ex.getMessage());
            }
        }
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(VtApiServer::stop));
    }

    private VtApiServer(final String uniquePrefix) {
        VtApiServer.uniquePrefix = uniquePrefix;
        initHttpServer();
        if (httpServer == null) {
            throw new VitessRuntimeException("VtApiServer:Monitoring function failed to start!");
        }
        PortCollector.getApiServerPortGauge().labels(uniquePrefix).set(port.doubleValue());
        httpServer.setExecutor(null);
        httpServer.createContext(uniquePrefix + "/status", new VtStatusHandler(uniquePrefix));
    }

    public static VtApiServer getInstance(String uniquePrefix) {
        if (instance == null) {
            synchronized (VtApiServer.class) {
                if (instance == null) {
                    instance = new VtApiServer(uniquePrefix);
                }
            }
        }
        return instance;
    }

    public static void stop() {
        if (httpServer != null) {
            httpServer.stop(0);
            LOGGER.info("Stop API server succeed!");
        }
    }

    private void initHttpServer() {
        if (port != null) {
            try {
                httpServer = HttpServer.create(new InetSocketAddress(port), 0);
            } catch (IOException e) {
                LOGGER.error("Create API server on " + port + " failed!");
            }
            return;
        }
        int retryCount = 0;
        port = DEFAULT_PROMETHEUS_SERVER_PORT;
        while (retryCount < 20) {
            try {
                httpServer = HttpServer.create(new InetSocketAddress(port), 0);
            } catch (IOException e) {
                LOGGER.info("Create API server on " + port + " failed!");
                port += 2;
            }
            retryCount++;
            if (httpServer != null) {
                break;
            }
        }
    }

    public void regiest(String path, HttpHandler handler) {
        if (httpServer != null) {
            httpServer.createContext(VtApiServer.uniquePrefix + path, handler);
        }
    }

    public void start() {
        if (httpServer != null) {
            httpServer.start();
        }
    }
}
