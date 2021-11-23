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

import com.jd.jdbc.monitor.ConnectionCollector;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.tindexes.LogicTable;
import io.prometheus.client.Histogram;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class VitessDataSource extends VitessWrapper implements javax.sql.DataSource {

    private static final Histogram HISTOGRAM = ConnectionCollector.getConnectionHistogram();

    private static Map<String, Map<String, LogicTable>> tableIndexesMap;

    protected final VitessDriver driver = new VitessDriver();

    protected final String url;

    protected final Properties prop;

    protected PrintWriter printWriter;

    protected int loginTimeoutSeconds = 0;

    public VitessDataSource(String url) throws Exception {
        this(url, new Properties());
    }

    VitessDataSource(String url, Properties info) throws Exception {
        this.url = url;
        this.prop = info;
        Histogram.Timer histogramTimer = HISTOGRAM.labels("init").startTimer();
        try {
            driver.initConnect(url, info, true);
        } finally {
            if (histogramTimer != null) {
                histogramTimer.observeDuration();
            }
        }
    }

    public static LogicTable getLogicTable(final String keyspace, final String logicTable) {
        if (tableIndexesMap == null || tableIndexesMap.isEmpty()) {
            return null;
        }
        if (StringUtils.isEmpty(keyspace) || StringUtils.isEmpty(logicTable)) {
            return null;
        }
        String lowerCaseKeyspace = keyspace.toLowerCase();
        String lowerCaseLogicTable = logicTable.toLowerCase();
        if (tableIndexesMap.containsKey(lowerCaseKeyspace) && tableIndexesMap.get(lowerCaseKeyspace).containsKey(lowerCaseLogicTable)) {
            return tableIndexesMap.get(lowerCaseKeyspace).get(lowerCaseLogicTable);
        }
        return null;
    }

    protected static void setTableIndexesMap(final Map<String, Map<String, LogicTable>> tableIndexesMap) {
        VitessDataSource.tableIndexesMap = tableIndexesMap;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return driver.connect(url, prop);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return driver.connect(url, prop);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return printWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        printWriter = out;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return loginTimeoutSeconds;
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        loginTimeoutSeconds = seconds;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    }
}
