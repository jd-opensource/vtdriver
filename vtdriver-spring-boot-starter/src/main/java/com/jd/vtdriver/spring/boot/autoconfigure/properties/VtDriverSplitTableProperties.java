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

package com.jd.vtdriver.spring.boot.autoconfigure.properties;

import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@ConfigurationProperties(prefix = "vtdriver", ignoreUnknownFields = false)
public class VtDriverSplitTableProperties {
    private SplitTableConfig splitTable;

    @Data
    @Component
    @ConfigurationProperties(prefix = "vtdriver.split-table", ignoreUnknownFields = false)
    public static class SplitTableConfig {

        private List<SchemaConfig> schemas;

        @Data
        @Component
        @ConfigurationProperties(prefix = "vtdriver.split-table.schemas", ignoreUnknownFields = false)
        public static class SchemaConfig {
            private String schema;

            private List<LogicTableConfig> logicTables;
        }

        @Data
        @Component
        @ConfigurationProperties(prefix = "vtdriver.split-table.schemas.logic-tables", ignoreUnknownFields = false)
        public static class LogicTableConfig {
            private String logicTable;

            private String actualTableExprs;

            private String shardingColumnName;

            private String shardingColumnType;

            private String shardingAlgorithms;

            private String sequenceColumnName;
        }
    }
}
