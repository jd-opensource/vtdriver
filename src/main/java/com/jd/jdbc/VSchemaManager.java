/*
Copyright 2021 JD Project Authors. Licensed under Apache-2.0.

Copyright 2019 The Vitess Authors.

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

package com.jd.jdbc;

import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.topo.TopoException;
import com.jd.jdbc.topo.TopoServer;
import io.netty.util.internal.StringUtil;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import vschema.Vschema;

public class VSchemaManager {
    private static final Map<TopoServer, VSchemaManager> topoServerVSchemaManagerMap = new HashMap<>();

    private final Map<String, Vschema.Keyspace> ksMap;

    private TopoServer topoServer;

    private VSchemaManager(TopoServer topoServer) {
        this.topoServer = topoServer;
        this.ksMap = new ConcurrentHashMap<>();
    }

    public VSchemaManager(Map<String, Vschema.Keyspace> ksMap) {
        this.ksMap = ksMap;
    }

    public static VSchemaManager getInstance(TopoServer topoServer) {
        synchronized (VSchemaManager.class) {
            VSchemaManager vSchemaManager = topoServerVSchemaManagerMap.get(topoServer);
            if (vSchemaManager != null) {
                return vSchemaManager;
            }
            vSchemaManager = new VSchemaManager(topoServer);
            topoServerVSchemaManagerMap.put(topoServer, vSchemaManager);
            return vSchemaManager;
        }
    }

    /**
     * @param ksSet
     * @throws TopoException
     */
    public void initVschema(Set<String> ksSet) throws TopoException {
        for (String ks : ksSet) {
            if (this.ksMap.containsKey(ks)) {
                continue;
            }
            this.ksMap.put(ks, this.topoServer.getVschema(null, ks));
        }
    }

    /**
     * @throws TopoException
     */
    public void refreshVschema() throws TopoException {
        for (String ks : this.ksMap.keySet()) {
            this.ksMap.put(ks, this.topoServer.getVschema(null, ks));
        }
    }

    /**
     * @param keyspace
     * @param table
     * @return
     */
    public String getVindex(String keyspace, String table) {
        Map<String, Vschema.Table> vschemaTableMap = this.getVschemaTableMap(keyspace, table);
        if (vschemaTableMap == null || vschemaTableMap.isEmpty()) {
            return null;
        }

        for (Map.Entry<String, Vschema.Table> entry : vschemaTableMap.entrySet()) {
            if (SQLUtils.nameEquals(entry.getKey(), table)) {
                Vschema.Table vTable = entry.getValue();
                if (vTable == null) {
                    return null;
                }

                List<Vschema.ColumnVindex> columnVindexList = vTable.getColumnVindexesList();
                if (columnVindexList.isEmpty()) {
                    // Protobuf will be initialized columnVindexList to ensure that it never be null
                    return null;
                }

                Vschema.ColumnVindex columnVindex = columnVindexList.get(0);
                return columnVindex == null ? null : columnVindex.getColumn();
            }
        }
        return null;
    }

    /**
     * @param keyspace
     * @param table
     * @return
     */
    public Vschema.Table getTable(String keyspace, String table) {
        Map<String, Vschema.Table> vschemaTableMap = this.getVschemaTableMap(keyspace, table);
        if (vschemaTableMap == null || vschemaTableMap.isEmpty()) {
            return null;
        }

        for (Map.Entry<String, Vschema.Table> entry : vschemaTableMap.entrySet()) {
            if (SQLUtils.nameEquals(entry.getKey(), table)) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * @param keyspace
     * @return
     */
    public Vschema.Keyspace getKeyspace(String keyspace) {
        if (this.ksMap == null || this.ksMap.isEmpty() || StringUtil.isNullOrEmpty(keyspace)) {
            return null;
        }

        for (Map.Entry<String, Vschema.Keyspace> entry : this.ksMap.entrySet()) {
            if (SQLUtils.nameEquals(entry.getKey(), keyspace)) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * @param keyspace
     * @param table
     * @return
     */
    private Map<String, Vschema.Table> getVschemaTableMap(String keyspace, String table) {
        if (this.ksMap == null || this.ksMap.isEmpty()) {
            return null;
        }

        if (StringUtil.isNullOrEmpty(keyspace) || StringUtil.isNullOrEmpty(table)) {
            return null;
        }

        Vschema.Keyspace vKeyspace = null;
        for (Map.Entry<String, Vschema.Keyspace> entry : this.ksMap.entrySet()) {
            if (SQLUtils.nameEquals(entry.getKey(), keyspace)) {
                vKeyspace = entry.getValue();
            }
        }

        if (vKeyspace == null) {
            return null;
        }

        return vKeyspace.getTablesMap();
    }
}
