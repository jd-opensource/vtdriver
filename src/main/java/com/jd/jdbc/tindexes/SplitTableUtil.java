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
import com.jd.jdbc.vitess.VitessDriver;
import io.vitess.proto.Query;
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

public class SplitTableUtil {

    private static final String ACTUAL_TABLE_EXPR_REGEX = "\\$\\{\\d+\\.\\.\\d+}";

    private static final Log logger = LogFactory.getLog(SplitTableUtil.class);

    private static Map<String, Map<String, LogicTable>> tableIndexesMap;

    /**
     * 根据分表字段的值，获取分表算法计算后的分表名
     *
     * @param keyspace       数据库的keyspace
     * @param logicTableName 逻辑表名
     * @param value          分表字段的值
     * @return
     */
    public static String getActualTableName(final String keyspace, final String logicTableName, final Object value) {
        return getActualTableName(Constant.DEFAULT_SPLIT_TABLE_CONFIG_PATH, keyspace, logicTableName, value);
    }

    /**
     * 根据分表字段的值，获取分表算法计算后的分表名
     *
     * @param configPath     分表配置文件路径
     * @param keyspace       数据库的keyspace
     * @param logicTableName 逻辑表名
     * @param value          分表字段的值
     * @return
     */
    public static String getActualTableName(final String configPath, final String keyspace, final String logicTableName, final Object value) {
        if (StringUtils.isEmpty(keyspace) || StringUtils.isEmpty(logicTableName)) {
            return null;
        }
        final Map<String, Map<String, LogicTable>> tableIndexesMap = initTableIndexesMapFromYaml(configPath);
        if (tableIndexesMap == null || tableIndexesMap.isEmpty()) {
            return null;
        }
        String lowerCaseKeyspace = keyspace.toLowerCase();
        String lowerCaseLogicTable = logicTableName.toLowerCase();
        if (tableIndexesMap.containsKey(lowerCaseKeyspace) && tableIndexesMap.get(lowerCaseKeyspace).containsKey(lowerCaseLogicTable)) {
            final LogicTable logicTable = tableIndexesMap.get(lowerCaseKeyspace).get(lowerCaseLogicTable);
            VtValue vtValue;
            try {
                vtValue = VtValue.toVtValue(value);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            final ActualTable actualTable = logicTable.map(vtValue);
            return actualTable.getActualTableName();
        }
        return null;
    }

    public static Map<String, Map<String, LogicTable>> getTableIndexesMap() {
        if (SplitTableUtil.tableIndexesMap == null) {
            SplitTableUtil.tableIndexesMap = initTableIndexesMapFromYaml(Constant.DEFAULT_SPLIT_TABLE_CONFIG_PATH);
        }
        return SplitTableUtil.tableIndexesMap;
    }

    //call by vtdriver-spring-boot-starter
    public static void setSplitIndexesMapFromSpring(SplitTableConfig splitTableConfig) {
        SplitTableUtil.tableIndexesMap = buildTableIndexesMap(splitTableConfig);
    }

    public static Map<String, Map<String, LogicTable>> initTableIndexesMapFromYaml(final String configPath) {
        Yaml yaml = new Yaml(new Constructor(SplitTableConfig.class));
        try {
            SplitTableConfig splitTableConfig = yaml.load(VitessDriver.class.getClassLoader().getResourceAsStream(configPath));
            return buildTableIndexesMap(splitTableConfig);
        } catch (YAMLException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("Init split-table from vtdriver-split-table.yaml fail, caused by:" + e.getMessage());
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