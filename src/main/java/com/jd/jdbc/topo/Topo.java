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

package com.jd.jdbc.topo;

import com.jd.jdbc.topo.etcd2topo.Etcd2TopoFactory;
import io.vitess.proto.Vtrpc;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.jd.jdbc.topo.TopoExceptionCode.NO_IMPLEMENTATION;
import static com.jd.jdbc.topo.TopoServer.CELLS_PATH;
import static com.jd.jdbc.topo.TopoServer.CELL_INFO_FILE;
import static com.jd.jdbc.topo.TopoServer.GLOBAL_CELL;
import static com.jd.jdbc.topo.TopoServer.GLOBAL_READ_ONLY_CELL;
import static com.jd.jdbc.topo.TopoServer.KEYSPACES_PATH;
import static com.jd.jdbc.topo.TopoServer.SRV_KEYSPACE_FILE;
import static com.jd.jdbc.topo.TopoServer.TABLETS_PATH;
import static com.jd.jdbc.topo.TopoServer.TABLET_FILE;
import static com.jd.jdbc.topo.TopoServer.VSCHEMA_FILE;

public class Topo {

    private static final String separator = "/";

    private static final Map<TopoServerImplementType, TopoFactory> factories = new HashMap<>(16);

    private static final Map<String, TopoServer> topoServers = new HashMap<>(16);

    public static String TOPO_GLOBAL_ROOT = "/vitess/global";

    public static TopoServer getTopoServer(TopoServerImplementType topoServerImplementType, String topoServerAddress) throws TopoException {
        synchronized (Topo.class) {
            registerFactory(topoServerImplementType);

            if (topoServers.containsKey(topoServerAddress)) {
                return topoServers.get(topoServerAddress);
            }

            TopoServer topoServer = openServer(topoServerImplementType, topoServerAddress, TOPO_GLOBAL_ROOT);
            if (topoServer == null) {
                throw new TopoException(Vtrpc.Code.UNKNOWN, "");
            }
            topoServers.put(topoServerAddress, topoServer);
            return topoServer;
        }
    }

    /**
     * @param topoServerImplementType
     * @throws TopoException
     */
    public static void registerFactory(TopoServerImplementType topoServerImplementType) throws TopoException {
        if (factories.containsKey(topoServerImplementType)) {
            return;
        }

        switch (topoServerImplementType) {
            case TOPO_IMPLEMENTATION_ETCD2:
                factories.put(topoServerImplementType, new Etcd2TopoFactory());
                break;
            default:
                throw TopoException.wrap(String.format("no topo.Factory implementation for %s", topoServerImplementType.topoImplementationName));
        }
    }

    /**
     * @param topoServerImplementType
     */
    public static void unRegisterFactory(TopoServerImplementType topoServerImplementType) {
        factories.remove(topoServerImplementType);
    }

    /**
     * @param topoServerImplementType
     * @param serverAddress
     * @param root
     * @return
     * @throws TopoException
     */
    private static TopoServer openServer(TopoServerImplementType topoServerImplementType, String serverAddress, String root) throws TopoException {
        if (!factories.containsKey(topoServerImplementType)) {
            throw TopoException.wrap(NO_IMPLEMENTATION, topoServerImplementType.name());
        }
        TopoFactory topoFactory = factories.get(topoServerImplementType);
        return newWithFactory(topoFactory, serverAddress, root);
    }

    /**
     * @param topoFactory
     * @param serverAddress
     * @param root
     * @return
     * @throws TopoException
     */
    protected static TopoServer newWithFactory(TopoFactory topoFactory, String serverAddress, String root) throws TopoException {
        TopoConnection conn = topoFactory.create(GLOBAL_CELL, serverAddress, root);
        conn = new TopoStatsConnection(GLOBAL_CELL, conn);

        TopoConnection connReadOnly;
        if (topoFactory.hasGlobalReadOnlyCell(serverAddress, root)) {
            connReadOnly = topoFactory.create(GLOBAL_READ_ONLY_CELL, serverAddress, root);
            connReadOnly = new TopoStatsConnection(GLOBAL_READ_ONLY_CELL, connReadOnly);
        } else {
            connReadOnly = conn;
        }
        TopoServer topoServer = new TopoServer();
        topoServer.globalCell = conn;
        topoServer.globalReadOnlyCell = connReadOnly;
        topoServer.topoFactory = topoFactory;
        topoServer.cells = new HashMap<>(16);
        topoServer.serverAddress = serverAddress;
        return topoServer;
    }

    /**
     * @param cell
     * @return
     */
    static String pathForCellInfo(String cell) {
        return CELLS_PATH + separator + cell + separator + CELL_INFO_FILE;
    }

    /**
     * @param alias
     * @return
     */
    static String pathForTabletAlias(String alias) {
        return TABLETS_PATH + separator + alias + separator + TABLET_FILE;
    }

    /**
     * @param keyspace
     * @return
     */
    public static String pathForSrvKeyspaceFile(String keyspace) {
        return KEYSPACES_PATH + separator + keyspace + separator + SRV_KEYSPACE_FILE;
    }

    /**
     * @param keyspace
     * @return
     */
    static String pathForVschemaFile(String keyspace) {
        return KEYSPACES_PATH + separator + keyspace + separator + VSCHEMA_FILE;
    }

    /**
     * @param dirEntryList
     * @return
     */
    static List<String> dirEntriesToStringArray(List<TopoConnection.DirEntry> dirEntryList) {
        List<String> result = new ArrayList<>(dirEntryList.size());
        dirEntryList.forEach(dirEntry -> result.add(dirEntry.getName()));
        return result;
    }

    public enum TopoServerImplementType {
        TOPO_IMPLEMENTATION_ETCD2("etcd2");

        String topoImplementationName;

        TopoServerImplementType(String name) {
            this.topoImplementationName = name;
        }
    }
}
