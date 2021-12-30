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

package com.jd.jdbc.vitess;

import com.jd.jdbc.common.Constant;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.tindexes.ActualTable;
import com.jd.jdbc.tindexes.Column;
import com.jd.jdbc.tindexes.LogicTable;
import com.jd.jdbc.tindexes.TableIndex;
import com.jd.jdbc.tindexes.config.LoginTableConfig;
import com.jd.jdbc.tindexes.config.SchemeConfig;
import com.jd.jdbc.tindexes.config.SplitTableConfig;
import com.jd.jdbc.vitess.mysql.VitessPropertyKey;
import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

public class VitessJdbcProperyUtil {
    private static final String ACTUAL_TABLE_EXPR_REGEX = "\\$\\{\\d+\\.\\.\\d+}";

    private VitessJdbcProperyUtil() {
    }

    public static void replaceLegacyPropertyValues(Properties info) {
        //all property keys are case-sensitive
        String zeroDateTimeBehavior = info.getProperty(VitessPropertyKey.ZERO_DATE_TIME_BEHAVIOR.getKeyName());
        if (zeroDateTimeBehavior != null && zeroDateTimeBehavior.equalsIgnoreCase("convertToNull")) {
            info.setProperty(VitessPropertyKey.ZERO_DATE_TIME_BEHAVIOR.getKeyName(), "CONVERT_TO_NULL");
        }
    }

    public static void addDefaultProperties(Properties info) {
        if (!info.containsKey(VitessPropertyKey.CHARACTER_ENCODING.getKeyName())) {
            info.setProperty(VitessPropertyKey.CHARACTER_ENCODING.getKeyName(), "utf-8");
        }
        if (!info.containsKey(VitessPropertyKey.SEND_FRACTIONAL_SECONDS.getKeyName())) {
            info.setProperty(VitessPropertyKey.SEND_FRACTIONAL_SECONDS.getKeyName(), "true");
        }
        if (!info.containsKey(VitessPropertyKey.TREAT_UTIL_DATE_AS_TIMESTAMP.getKeyName())) {
            info.setProperty(VitessPropertyKey.TREAT_UTIL_DATE_AS_TIMESTAMP.getKeyName(), "true");
        }
        if (!info.containsKey(VitessPropertyKey.USE_STREAM_LENGTHS_IN_PREP_STMTS.getKeyName())) {
            info.setProperty(VitessPropertyKey.USE_STREAM_LENGTHS_IN_PREP_STMTS.getKeyName(), "true");
        }
        if (!info.containsKey(VitessPropertyKey.AUTO_CLOSE_P_STMT_STREAMS.getKeyName())) {
            info.setProperty(VitessPropertyKey.AUTO_CLOSE_P_STMT_STREAMS.getKeyName(), "false");
        }
    }

    public static void checkCredentials(String path, Properties info) {
        if (!info.containsKey(VitessPropertyKey.USER.getKeyName()) || !info.containsKey(VitessPropertyKey.PASSWORD.getKeyName())) {
            throw new IllegalArgumentException("no user or password: '" + path + "'");
        }
    }

    public static void checkCell(Properties info) throws IllegalArgumentException {
        if (info.getProperty("cell") == null) {
            throw new IllegalArgumentException("no cell found in jdbc url");
        }
        String[] cells = info.getProperty("cell").split(",");
        if (cells.length < 1) {
            throw new IllegalArgumentException("no cell found in jdbc url");
        }
    }

    public static void checkSchema(String path) {
        if (path == null || path.isEmpty() || !path.startsWith("/")) {
            throw new IllegalArgumentException("wrong database name path: '" + path + "'");
        }
    }

    public static String getDefaultKeyspace(Properties props) {
        List<String> keySpaces = Arrays.asList(props.getProperty("schema").split(","));
        return keySpaces.get(0);
    }

    public static Topodata.TabletType getTabletType(Properties props) {
        String role = props.getProperty(Constant.DRIVER_PROPERTY_ROLE_KEY, Constant.DRIVER_PROPERTY_ROLE_RW);
        switch (role.toLowerCase()) {
            case Constant.DRIVER_PROPERTY_ROLE_RW:
                return Topodata.TabletType.MASTER;
            case Constant.DRIVER_PROPERTY_ROLE_RR:
                return Topodata.TabletType.REPLICA;
            case Constant.DRIVER_PROPERTY_ROLE_RO:
                return Topodata.TabletType.RDONLY;
            default:
                throw new IllegalArgumentException("'role=" + role + "' " + "error in jdbc url");
        }
    }

    public static String getRole(Properties props) {
        return props.getProperty(Constant.DRIVER_PROPERTY_ROLE_KEY, Constant.DRIVER_PROPERTY_ROLE_RW);
    }

    public static Map<String, Map<String, LogicTable>> buildTableIndexesMap(final SplitTableConfig splitTableConfig) throws InstantiationException, IllegalAccessException {
        Map<String, Map<String, LogicTable>> map = new ConcurrentHashMap<>(16);
        List<SchemeConfig> schemas = splitTableConfig.getSchemas();
        if (schemas == null || schemas.isEmpty()) {
            return null;
        }
        for (SchemeConfig schema : schemas) {
            if (StringUtils.isEmpty(schema.getSchema()) || schema.getLogicTables() == null || schema.getLogicTables().isEmpty()) {
                return null;
            }
            Map<String, LogicTable> logicTableMap = new ConcurrentHashMap<>(16);
            for (LoginTableConfig loginTableConfig : schema.getLogicTables()) {
                LogicTable logicTable = buildLogicTable(loginTableConfig);
                if (logicTable != null) {
                    logicTableMap.put(logicTable.getLogicTable().toLowerCase(), logicTable);
                }
            }
            map.put(schema.getSchema().toLowerCase(), logicTableMap);
        }
        return map;
    }

    private static LogicTable buildLogicTable(final LoginTableConfig loginTableConfig) throws InstantiationException, IllegalAccessException {
        if (StringUtils.isEmpty(loginTableConfig.getLogicTable()) || StringUtils.isEmpty(loginTableConfig.getActualTableExprs())
            || StringUtils.isEmpty(loginTableConfig.getShardingColumnType()) || StringUtils.isEmpty(loginTableConfig.getShardingAlgorithms())
            || StringUtils.isEmpty(loginTableConfig.getShardingColumnName())) {
            return null;
        }

        int index = loginTableConfig.getActualTableExprs().indexOf("$");
        if (index == -1) {
            return null;
        }
        String actualTableExpr = loginTableConfig.getActualTableExprs();
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
        logicTable.setLogicTable(loginTableConfig.getLogicTable().toLowerCase());
        for (int i = begin; i <= end; i++) {
            ActualTable actualTable = new ActualTable();
            actualTable.setActualTableName((logicNamePrefix + i).toLowerCase());
            actualTable.setLogicTable(logicTable);
            actualTableList.add(actualTable);
        }
        logicTable.setActualTableList(actualTableList);
        Query.Type type = Query.Type.valueOf(loginTableConfig.getShardingColumnType());
        Column column = new Column(loginTableConfig.getShardingColumnName().toLowerCase(), type, logicTable);
        logicTable.setTindexCol(column);
        ServiceLoader<TableIndex> serviceLoader = ServiceLoader.load(TableIndex.class);
        Iterator<TableIndex> iterator = serviceLoader.iterator();
        TableIndex tableIndex = null;
        while (iterator.hasNext()) {
            TableIndex next = iterator.next();
            if (Objects.equals(next.getClass().getSimpleName(), loginTableConfig.getShardingAlgorithms())) {
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
