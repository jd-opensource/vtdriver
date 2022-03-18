package com.jd.vtdriver.spring.boot.autoconfigure;

import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.tindexes.SplitTableUtil;
import com.jd.jdbc.tindexes.config.LogicTableConfig;
import com.jd.jdbc.tindexes.config.SchemaConfig;
import com.jd.jdbc.tindexes.config.SplitTableConfig;
import com.jd.vtdriver.spring.boot.autoconfigure.properties.VtDriverSplitTableProperties;
import java.util.ArrayList;
import java.util.List;
import org.springframework.beans.factory.InitializingBean;

public class VtDriverSplitTable implements InitializingBean {

    private final static Log LOG = LogFactory.getLog(VtDriverSplitTable.class);

    private final VtDriverSplitTableProperties splitTableProperties;

    public VtDriverSplitTable(VtDriverSplitTableProperties splitTableProperties) {
        this.splitTableProperties = splitTableProperties;
    }

    @Override
    public void afterPropertiesSet() {
        if (splitTableProperties != null) {
            SplitTableUtil.setSplitIndexesMapFromSpring(convertSplitTableConfig(splitTableProperties.getSplitTable()));
            LOG.info("VtDriver set split table config from spring");
        }
    }

    public SplitTableConfig convertSplitTableConfig(VtDriverSplitTableProperties.SplitTableConfig starterSplitTableConfig) {
        SplitTableConfig splitTableConfig = new SplitTableConfig();
        List<SchemaConfig> schemas = new ArrayList<>();
        if (starterSplitTableConfig.getSchemas() == null) {
            return splitTableConfig;
        }
        for (VtDriverSplitTableProperties.SplitTableConfig.SchemaConfig starterSchemaConfig : starterSplitTableConfig.getSchemas()) {
            SchemaConfig schemeConfig = new SchemaConfig();
            schemeConfig.setSchema(starterSchemaConfig.getSchema());
            List<LogicTableConfig> logicTables = new ArrayList<>();
            if (starterSchemaConfig.getLogicTables() == null) {
                continue;
            }
            for (VtDriverSplitTableProperties.SplitTableConfig.LogicTableConfig starterLogicTable : starterSchemaConfig.getLogicTables()) {
                LogicTableConfig logicTable = new LogicTableConfig();
                logicTable.setLogicTable(starterLogicTable.getLogicTable());
                logicTable.setActualTableExprs(starterLogicTable.getActualTableExprs());
                logicTable.setShardingAlgorithms(starterLogicTable.getShardingAlgorithms());
                logicTable.setShardingColumnType(starterLogicTable.getShardingColumnType());
                logicTable.setShardingColumnName(starterLogicTable.getShardingColumnName());
                logicTables.add(logicTable);
            }
            schemeConfig.setLogicTables(logicTables);
            schemas.add(schemeConfig);
        }
        splitTableConfig.setSchemas(schemas);
        return splitTableConfig;
    }
}
