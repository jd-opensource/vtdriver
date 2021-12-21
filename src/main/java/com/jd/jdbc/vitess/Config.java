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

import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import io.vitess.proto.Topodata;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import lombok.EqualsAndHashCode;
import lombok.Setter;

/**
 * configuration.
 */
public final class Config {

    private static final Map<ConfigKey, Properties> PROPERTIES_MAP;

    private static final Map<ConfigKey, InnerCPConfig> INNER_CP_CONFIG_MAP;

    private static final String PREFIX = "vt";

    private static final Map<String, Class<?>> FILED_TYPES = new HashMap<>();

    private static final List<String> FILED_NAMES = new ArrayList<>();

    static {
        FILED_TYPES.put("MinimumIdle", int.class);
        FILED_TYPES.put("MaximumPoolSize", int.class);
        FILED_TYPES.put("ConnectionTimeout", long.class);
        FILED_TYPES.put("IdleTimeout", long.class);
        FILED_TYPES.put("MaxLifetime", long.class);
        FILED_TYPES.put("ValidationTimeout", long.class);
        FILED_TYPES.put("ConnectionInitSql", String.class);
        FILED_TYPES.put("ConnectionTestQuery", String.class);

        FILED_NAMES.add("MinimumIdle");
        FILED_NAMES.add("MaximumPoolSize");
        FILED_NAMES.add("ConnectionTimeout");
        FILED_NAMES.add("IdleTimeout");
        FILED_NAMES.add("MaxLifetime");
        FILED_NAMES.add("ValidationTimeout");
        FILED_NAMES.add("ConnectionInitSql");
        FILED_NAMES.add("ConnectionTestQuery");
        PROPERTIES_MAP = new ConcurrentHashMap<>(16);
        INNER_CP_CONFIG_MAP = new ConcurrentHashMap<>(16);
    }

    public static void setConfig(final Properties prop, final String keySpace, final String user, final Topodata.TabletType tabletType) {
        ConfigKey key = buildConfigKey(keySpace, user, tabletType);
        if (!PROPERTIES_MAP.containsKey(key)) {
            PROPERTIES_MAP.put(key, prop);
        }
        if (!INNER_CP_CONFIG_MAP.containsKey(key)) {
            INNER_CP_CONFIG_MAP.put(key, new InnerCPConfig(prop));
        }
    }

    public static Properties getDataSourceConfig(String keySpace, String user, Topodata.TabletType tabletType) {
        ConfigKey key = buildConfigKey(keySpace, user, tabletType);
        return PROPERTIES_MAP.get(key);
    }

    public static Properties getConnectionPoolConfig(String keySpace, String user, Topodata.TabletType tabletType) {
        ConfigKey key = buildConfigKey(keySpace, user, tabletType);
        InnerCPConfig innerCPConfig = INNER_CP_CONFIG_MAP.get(key);
        if (null != innerCPConfig) {
            return innerCPConfig.buildProperties();
        }
        return null;
    }

    private static ConfigKey buildConfigKey(String keySpace, String user, Topodata.TabletType tabletType) {
        return new ConfigKey(keySpace, user, tabletType);
    }

    @EqualsAndHashCode
    public static class ConfigKey {
        private final String keySpace;

        private final String user;

        @Setter
        private Topodata.TabletType tabletType;

        public ConfigKey(String keySpace, String user, Topodata.TabletType tabletType) {
            this.keySpace = keySpace;
            this.user = user;
            this.tabletType = tabletType;
        }
    }

    public static class InnerCPConfig {
        private static final Log logger = LogFactory.getLog(InnerCPConfig.class);

        private int minimumIdle = 5;

        private int maximumPoolSize = 10;

        private long connectionTimeout = 30_000;

        private long idleTimeout = 600_000;

        private long maxLifetime = 1_800_000;

        private long validationTimeout = 5_000;

        private String connectionInitSql = "select 1";

        private String connectionTestQuery = "select 1";

        public InnerCPConfig(Properties properties) {
            FILED_NAMES.forEach(s -> {
                String key = PREFIX + s;
                Object keyValue = properties.get(key);
                if (null != keyValue) {
                    try {
                        Class<?> clazz = FILED_TYPES.get(s);
                        Method method = InnerCPConfig.class.getMethod("set" + s, clazz);
                        switch (clazz.getName()) {
                            case "int":
                                method.invoke(this, Integer.valueOf(keyValue.toString()));
                                break;
                            case "long":
                                method.invoke(this, Long.valueOf(keyValue.toString()));
                                break;
                            default:
                                method.invoke(this, keyValue);
                                break;
                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            });
        }

        public Properties buildProperties() {
            Properties properties = new Properties();
            if (validationTimeout > connectionTimeout) {
                validationTimeout = connectionTimeout >> 1;
            }
            FILED_NAMES.forEach(s -> {
                try {
                    Method method = InnerCPConfig.class.getMethod("get" + s);
                    Object value = method.invoke(this);
                    properties.setProperty(s.substring(0, 1).toLowerCase() + s.substring(1), String.valueOf(value));
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            });
            return properties;
        }

        public int getMinimumIdle() {
            return minimumIdle;
        }

        public void setMinimumIdle(int minimumIdle) {
            this.minimumIdle = minimumIdle;
        }

        public int getMaximumPoolSize() {
            return maximumPoolSize;
        }

        public void setMaximumPoolSize(int maximumPoolSize) {
            this.maximumPoolSize = maximumPoolSize;
        }

        public long getConnectionTimeout() {
            return connectionTimeout;
        }

        public void setConnectionTimeout(long connectionTimeout) {
            this.connectionTimeout = connectionTimeout;
        }

        public long getIdleTimeout() {
            return idleTimeout;
        }

        public void setIdleTimeout(long idleTimeout) {
            this.idleTimeout = idleTimeout;
        }

        public long getMaxLifetime() {
            return maxLifetime;
        }

        public void setMaxLifetime(long maxLifetime) {
            this.maxLifetime = maxLifetime;
        }

        public long getValidationTimeout() {
            return validationTimeout;
        }

        public void setValidationTimeout(long validationTimeout) {
            this.validationTimeout = validationTimeout;
        }

        public String getConnectionInitSql() {
            return connectionInitSql;
        }

        public void setConnectionInitSql(String connectionInitSql) {
            this.connectionInitSql = connectionInitSql;
        }

        public String getConnectionTestQuery() {
            return connectionTestQuery;
        }

        public void setConnectionTestQuery(String connectionTestQuery) {
            this.connectionTestQuery = connectionTestQuery;
        }
    }
}
