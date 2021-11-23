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
import com.jd.jdbc.tindexes.config.SplitTableConfig;
import com.jd.jdbc.vitess.VitessDriver;
import com.jd.jdbc.vitess.VitessJdbcProperyUtil;
import java.sql.SQLException;
import java.util.Map;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;

public class SplitTableUtil {
    private static final Log logger = LogFactory.getLog(SplitTableUtil.class);

    public static Map<String, Map<String, LogicTable>> getTableIndexesMap(final String configPath) {
        try {
            Yaml yaml = new Yaml(new Constructor(SplitTableConfig.class));
            SplitTableConfig splitTableConfig = yaml.load(VitessDriver.class.getClassLoader().getResourceAsStream(configPath));
            return VitessJdbcProperyUtil.buildTableIndexesMap(splitTableConfig);
        } catch (YAMLException | InstantiationException | IllegalAccessException e) {
            if (logger.isDebugEnabled()) {
                logger.debug("init split-table fail.causeby:" + e.getMessage());
            }
        }
        return null;
    }

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
        final Map<String, Map<String, LogicTable>> tableIndexesMap = getTableIndexesMap(configPath);
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
}