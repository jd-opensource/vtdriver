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

package com.jd.jdbc.util;

import com.jd.jdbc.discovery.HealthCheck;
import com.jd.jdbc.discovery.SecurityCenter;
import com.jd.jdbc.monitor.MonitorServer;
import com.jd.jdbc.queryservice.IParentQueryService;
import com.jd.jdbc.queryservice.TabletDialer;
import com.jd.jdbc.vitess.Config;
import com.jd.jdbc.vitess.VitessConnection;
import io.vitess.proto.Topodata;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class InnerConnectionPoolUtil {

    public static void removeInnerConnectionConfig(Connection conn) throws IllegalAccessException, NoSuchFieldException, SQLException {
        closeInnerConnection(conn);
        clearConfig();
    }

    public static void clearAll() throws NoSuchFieldException, IllegalAccessException {
        Field ksSetField = MonitorServer.class.getDeclaredField("keyspaceSet");
        ksSetField.setAccessible(true);
        Set<String> ksSet = (Set<String>) ksSetField.get(MonitorServer.getInstance());
        for (String ks : ksSet) {
            System.out.println("clear inner connection pool: " + ks);
            clearByKeyspace(ks);
        }
        clearConfig();
    }

    private static void clearByKeyspace(String keyspace) {
        List<Topodata.Tablet> tabletList = HealthCheck.INSTANCE.getHealthyTablets(KeyspaceUtil.getLogicSchema(keyspace));
        for (Topodata.Tablet tablet : tabletList) {
            IParentQueryService queryService = TabletDialer.dial(tablet);
            queryService.closeNativeQueryService();
        }
    }

    private static void closeInnerConnection(Connection conn) throws SQLException {
        if (conn == null) {
            return;
        }
        VitessConnection vitessConnection = conn.unwrap(VitessConnection.class);
        vitessConnection.closeInnerConnection();
    }

    private static void clearConfig() throws NoSuchFieldException, IllegalAccessException {
        // Config.PROPERTIES_MAP
        Field propertiesMapField = Config.class.getDeclaredField("PROPERTIES_MAP");
        propertiesMapField.setAccessible(true);
        Map<Config.ConfigKey, Properties> p1 = (Map<Config.ConfigKey, Properties>) propertiesMapField.get(null);
        p1.clear();

        // Config.INNER_CP_CONFIG_MAP
        Field innerCPConfigMapField = Config.class.getDeclaredField("INNER_CP_CONFIG_MAP");
        innerCPConfigMapField.setAccessible(true);
        Map<Config.ConfigKey, Config.InnerCPConfig> p2 = (Map<Config.ConfigKey, Config.InnerCPConfig>) innerCPConfigMapField.get(null);
        p2.clear();

        // SecurityCenter.INSTANCE.keySpaceCredentialMap
        Field credentialMapField = SecurityCenter.class.getDeclaredField("keySpaceCredentialMap");
        credentialMapField.setAccessible(true);
        Map<String, SecurityCenter.Credential> p3 = (Map<String, SecurityCenter.Credential>) credentialMapField.get(SecurityCenter.INSTANCE);
        p3.clear();
    }
}
