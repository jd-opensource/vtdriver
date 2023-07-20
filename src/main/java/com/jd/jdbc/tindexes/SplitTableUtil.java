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

package com.jd.jdbc.tindexes;

import com.jd.jdbc.common.Constant;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqltypes.VtValue;
import com.jd.jdbc.tindexes.config.LogicTableConfig;
import com.jd.jdbc.tindexes.config.SchemaConfig;
import com.jd.jdbc.tindexes.config.SplitTableConfig;
import io.vitess.proto.Query;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;

public final class SplitTableUtil {

    private static final String ACTUAL_TABLE_EXPR_REGEX = "\\$\\{\\d+\\.\\.\\d+}";

    private static final Log logger = LogFactory.getLog(SplitTableUtil.class);

    private static Map<String, Map<String, LogicTable>> tableIndexesMap;

    private static String preConfigPath;

    private SplitTableUtil() {
    }

    public static String getActualTableName(final String keyspace, final String logicTableName, final Object value) {
        return getActualTableName(Constant.DEFAULT_SPLIT_TABLE_CONFIG_PATH, keyspace, logicTableName, value);
    }

    public static String getActualTableName(final String configPath, final String keyspace, final String logicTableName, final Object value) {
        ActualTable actualTable = getActualTable(configPath, keyspace, logicTableName, value);
        return actualTable.getActualTableName();
    }

    public static String getShardingColumnName(final String configPath, final String keyspace, final String logicTableName) {
        LogicTable logicTable = getLogicTable(configPath, keyspace, logicTableName);
        return logicTable.getTindexCol().getColumnName();
    }

    public static String getShardingColumnName(final String keyspace, final String logicTableName) {
        return getShardingColumnName(Constant.DEFAULT_SPLIT_TABLE_CONFIG_PATH, keyspace, logicTableName);
    }

    private static LogicTable getLogicTable(String configPath, String keyspace, String logicTableName) {
        if (StringUtils.isEmpty(keyspace) || StringUtils.isEmpty(logicTableName)) {
            throw new RuntimeException("keyspace or logicTableName should not empty");
        }
        Map<String, Map<String, LogicTable>> tableIndexesMap = getTableIndexesMap(configPath);
        if (tableIndexesMap == null || tableIndexesMap.isEmpty()) {
            throw new RuntimeException("cat not find split-table config through configPath=" + configPath);
        }
        String lowerCaseKeyspace = keyspace.toLowerCase();
        String lowerCaseLogicTable = logicTableName.toLowerCase();
        if (!tableIndexesMap.containsKey(lowerCaseKeyspace)) {
            throw new RuntimeException("cat not find keyspace in split-table config, target keyspace=" + keyspace);
        }
        if (!tableIndexesMap.get(lowerCaseKeyspace).containsKey(lowerCaseLogicTable)) {
            throw new RuntimeException("cat not find logicTable in split-table config, target keyspace=" + keyspace + " ,target logicTable=" + logicTableName);
        }
        return tableIndexesMap.get(lowerCaseKeyspace).get(lowerCaseLogicTable);
    }

    private static ActualTable getActualTable(String configPath, String keyspace, String logicTableName, Object value) {
        LogicTable logicTable = getLogicTable(configPath, keyspace, logicTableName);
        VtValue vtValue;
        try {
            vtValue = VtValue.toVtValue(value);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        final ActualTable actualTable = logicTable.map(vtValue);
        if (actualTable == null) {
            throw new RuntimeException("cannot calculate split table, logic table: " + logicTable.getLogicTable() + ",current value: " + vtValue);
        }
        return actualTable;
    }

    public static Map<String, Map<String, LogicTable>> getTableIndexesMap() {
        return getTableIndexesMap(Constant.DEFAULT_SPLIT_TABLE_CONFIG_PATH);
    }

    public static synchronized Map<String, Map<String, LogicTable>> getTableIndexesMap(final String configPath) {
        if (StringUtils.isEmpty(configPath)) {
            throw new RuntimeException("configPath should not empty");
        }
        if (Objects.equals(configPath, preConfigPath)) {
            return SplitTableUtil.tableIndexesMap;
        }
        SplitTableUtil.tableIndexesMap = initTableIndexesMapFromYaml(configPath);
        preConfigPath = configPath;
        return tableIndexesMap;
    }

    //call by vtdriver-spring-boot-starter
    public static void setSplitIndexesMapFromSpring(SplitTableConfig splitTableConfig) {
        SplitTableUtil.tableIndexesMap = buildTableIndexesMap(splitTableConfig);
    }

    public static Map<String, Map<String, LogicTable>> initTableIndexesMapFromYaml(final String configPath) {
        Yaml yaml = new Yaml(new Constructor(SplitTableConfig.class));
        try (InputStream resourceAsStream = SplitTableUtil.class.getClassLoader().getResourceAsStream(configPath)) {
            SplitTableConfig splitTableConfig = yaml.load(resourceAsStream);
            return buildTableIndexesMap(splitTableConfig);
        } catch (YAMLException | IOException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Init split-table config through configPath=" + configPath + " fail, caused by:" + e.getMessage());
            }
        }
        return null;
    }

    public static Map<String, Map<String, LogicTable>> buildTableIndexesMap(final SplitTableConfig splitTableConfig) {
        Map<String, Map<String, LogicTable>> map = new ConcurrentHashMap<>(16);
        List<SchemaConfig> schemas = splitTableConfig.getSchemas();
        if (schemas == null || schemas.isEmpty()) {
            return null;
        }
        try {
            for (SchemaConfig schema : schemas) {
                if (StringUtils.isEmpty(schema.getSchema()) || schema.getLogicTables() == null || schema.getLogicTables().isEmpty()) {
                    return null;
                }
                Map<String, LogicTable> logicTableMap = new ConcurrentHashMap<>(16);
                for (LogicTableConfig logicTableConfig : schema.getLogicTables()) {
                    LogicTable logicTable = buildLogicTable(logicTableConfig);
                    if (logicTable != null) {
                        logicTableMap.put(logicTable.getLogicTable().toLowerCase(), logicTable);
                    }
                }
                map.put(schema.getSchema().toLowerCase(), logicTableMap);
            }
        } catch (InstantiationException | IllegalAccessException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Init split-table fail, caused by:" + e.getMessage());
            }
        }
        return map;
    }

    private static LogicTable buildLogicTable(final LogicTableConfig logicTableConfig) throws InstantiationException, IllegalAccessException {
        if (StringUtils.isEmpty(logicTableConfig.getLogicTable()) || StringUtils.isEmpty(logicTableConfig.getActualTableExprs())
            || StringUtils.isEmpty(logicTableConfig.getShardingColumnType()) || StringUtils.isEmpty(logicTableConfig.getShardingAlgorithms())
            || StringUtils.isEmpty(logicTableConfig.getShardingColumnName())) {
            return null;
        }

        int index = logicTableConfig.getActualTableExprs().indexOf("$");
        if (index == -1) {
            return null;
        }
        String actualTableExpr = logicTableConfig.getActualTableExprs();
        String logicNamePrefix = actualTableExpr.substring(0, index);
        String postfix = actualTableExpr.substring(index);
        if (!postfix.matches(ACTUAL_TABLE_EXPR_REGEX)) {
            return null;
        }
        int begin = -1;
        int end = -1;
        boolean flag = false;
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < postfix.length(); i++) {
            char each = postfix.charAt(i);
            switch (each) {
                case '.':
                    if (!flag) {
                        begin = Integer.parseInt(builder.toString());
                        builder = new StringBuilder();
                        flag = true;
                    }
                    continue;
                case '$':
                case '{':
                case '}':
                    continue;
                default:
                    builder.append(each);
                    break;
            }
        }
        end = Integer.parseInt(builder.toString());
        if (begin == -1 || end == -1) {
            return null;
        }
        List<ActualTable> actualTableList = new ArrayList<>(end - begin);

        LogicTable logicTable = new LogicTable();
        logicTable.setLogicTable(logicTableConfig.getLogicTable().toLowerCase());
        for (int i = begin; i <= end; i++) {
            ActualTable actualTable = new ActualTable();
            actualTable.setActualTableName((logicNamePrefix + i).toLowerCase());
            actualTable.setLogicTable(logicTable);
            actualTableList.add(actualTable);
        }
        logicTable.setActualTableList(actualTableList);
        Query.Type type = Query.Type.valueOf(logicTableConfig.getShardingColumnType());
        Column column = new Column(logicTableConfig.getShardingColumnName().toLowerCase(), type, logicTable);
        logicTable.setTindexCol(column);
        ServiceLoader<TableIndex> serviceLoader = ServiceLoader.load(TableIndex.class);
        Iterator<TableIndex> iterator = serviceLoader.iterator();
        TableIndex tableIndex = null;
        while (iterator.hasNext()) {
            TableIndex next = iterator.next();
            if (Objects.equals(next.getClass().getSimpleName(), logicTableConfig.getShardingAlgorithms())) {
                tableIndex = next.getClass().newInstance();
                break;
            }
        }
        if (tableIndex == null) {
            return null;
        } else {
            logicTable.setTableIndex(tableIndex);
        }
        return logicTable;
    }
}