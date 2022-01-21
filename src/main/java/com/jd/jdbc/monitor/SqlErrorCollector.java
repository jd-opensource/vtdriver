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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.SqlParser;
import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRestoreVisitor;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.srvtopo.BindVariable;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import java.sql.DataTruncation;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class SqlErrorCollector extends Collector {
    private static final Log LOG = LogFactory.getLog(SqlErrorCollector.class);

    private static final Integer DEFAULT_CAPACITY = 300;

    private static final Integer MAX_KEY_SIZE = 10240;

    private static final Integer MAX_BINDVAR_SIZE = 500;

    private static final Integer MINUTES_TO_KEEP = 10;

    private static final List<String> LABEL_NAMES = Lists.newArrayList("Keyspace", "sqlStatement", "bindVariableMap", "errorClassName", "errorMessage", "errorTime", "sql");

    private static final String COLLECT_NAME = "error_sql";

    private static final String COLLECT_HELP = "error sql info.";

    private static final Cache<String, SqlErrorRecorder> SQL_ERROR_RECORDER_CACHE = CacheBuilder.newBuilder()
            .maximumSize(DEFAULT_CAPACITY)
            .expireAfterWrite(MINUTES_TO_KEEP, TimeUnit.MINUTES)
            .build();

    private static final SqlErrorCollector INSTANCE = new SqlErrorCollector();

    private SqlErrorCollector() {
    }

    public static SqlErrorCollector getInstance() {
        return INSTANCE;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        GaugeMetricFamily labeledGauge = new GaugeMetricFamily(COLLECT_NAME, COLLECT_HELP, LABEL_NAMES);
        int index = -1;
        Map<String, SqlErrorRecorder> copyMap = new HashMap<>(SQL_ERROR_RECORDER_CACHE.asMap());
        for (Map.Entry<String, SqlErrorRecorder> entry : copyMap.entrySet()) {
            SqlErrorRecorder recorder = entry.getValue();
            List<String> labelValues = Lists.newArrayList(recorder.getKeyspace(),
                    recorder.getSqlStatement().toString(),
                    recorder.getBindVariableMap().isEmpty() ? "" : recorder.getBindVariableMap().toString(),
                    recorder.getErrorClassName(),
                    recorder.getErrorMessage(),
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(recorder.getErrorTime()),
                    recorder.getSql());
            labeledGauge.addMetric(labelValues, index--);
        }
        return Collections.singletonList(labeledGauge);
    }

    public void add(final String keyspace, final String userSQL, final Map<String, BindVariable> userBindVarMap, final String charEncoding, final SQLException e) {
        if (Ignored.match(e)) {
            return;
        }
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(userSQL);

        Map<String, BindVariable> bdMap = userBindVarMap;
        String sql = userSQL;
        try {
            SqlParser.PrepareAstResult prepareAstResult = SqlParser.prepareAst(stmt, userBindVarMap, charEncoding);
            bdMap = prepareAstResult.getBindVariableMap();
            StringBuilder querySqlBuffer = new StringBuilder();
            VtRestoreVisitor restoreVisitor = new VtRestoreVisitor(querySqlBuffer, bdMap, charEncoding);
            stmt.accept(restoreVisitor);
            sql = querySqlBuffer.toString();
        } catch (Exception ex) {
            //ignore
        }

        String key = keyspace + ":" + SQLUtils.toMySqlString(stmt, SQLUtils.NOT_FORMAT_OPTION);
        if (key.length() > MAX_KEY_SIZE || bdMap.size() > MAX_BINDVAR_SIZE) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error sql size too long, this record has been ignored: key=" + key + "bdMap=" + bdMap);
            }
            return;
        }

        LocalDateTime date = LocalDateTime.now();
        SqlErrorRecorder sqlErrorRecorder = new SqlErrorRecorder(keyspace, e.getMessage() == null ? "" : e.getMessage(),
                e.getClass().getSimpleName(), stmt, date, bdMap, sql);
        synchronized (this) {
            SqlErrorRecorder recorder = SQL_ERROR_RECORDER_CACHE.getIfPresent(key);
            if (recorder != null && recorder.equals(sqlErrorRecorder)) {
                recorder.setBindVariableMap(bdMap);
                recorder.setErrorTime(date);
                recorder.setSql(sql);
            } else {
                SQL_ERROR_RECORDER_CACHE.put(key, sqlErrorRecorder);
            }
        }
    }

    private static class Ignored {
        public static boolean match(final SQLException e) {
            return e instanceof SQLIntegrityConstraintViolationException
                    || e instanceof DataTruncation;
        }
    }
}
