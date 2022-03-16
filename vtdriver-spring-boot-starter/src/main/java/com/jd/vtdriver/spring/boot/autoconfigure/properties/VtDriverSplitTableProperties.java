package com.jd.vtdriver.spring.boot.autoconfigure.properties;

import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@ConfigurationProperties("vtdriver")
public class VtDriverSplitTableProperties {
    private SplitTableConfig splitTable;

    @Data
    @Component
    @ConfigurationProperties("vtdriver.split-table")
    public static class SplitTableConfig {

        private List<SchemaConfig> schemas;

        @Data
        @Component
        @ConfigurationProperties("vtdriver.split-table.schemas")
        public static class SchemaConfig {
            private String schema;

            private List<LogicTableConfig> logicTables;
        }

        @Data
        @Component
        @ConfigurationProperties("vtdriver.split-table.schemas.logic-tables")
        public static class LogicTableConfig {
            private String logicTable;

            private String actualTableExprs;

            private String shardingColumnName;

            private String shardingColumnType;

            private String shardingAlgorithms;
        }
    }
}
