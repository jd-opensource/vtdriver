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
import io.etcd.jetcd.Watch;
import io.vitess.proto.Topodata;
import io.vitess.proto.Vtrpc;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import static com.jd.jdbc.topo.TopoExceptionCode.NO_IMPLEMENTATION;
import static com.jd.jdbc.topo.TopoServer.CELLS_ALIASES_PATH;
import static com.jd.jdbc.topo.TopoServer.CELLS_ALIAS_FILE;
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
    static String pathForCellAlias(String alias) {
        return CELLS_ALIASES_PATH + separator + alias + separator + CELLS_ALIAS_FILE;
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
    static String pathForSrvKeyspaceFile(String keyspace) {
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

    /**
     *
     */
    public static class WatchData {
        private byte[] contents;

        private TopoConnection.Version version;

        private TopoException topoException;

        public WatchData() {
        }

        public WatchData(byte[] contents, TopoConnection.Version version) {
            this.contents = contents;
            this.version = version;
        }

        public WatchData(TopoException topoException) {
            this.topoException = topoException;
        }

        public byte[] getContents() {
            return contents;
        }

        public void setContents(byte[] contents) {
            this.contents = contents;
        }

        public TopoConnection.Version getVersion() {
            return version;
        }

        public void setVersion(TopoConnection.Version version) {
            this.version = version;
        }

        public TopoException getTopoException() {
            return topoException;
        }

        public void setTopoException(TopoException topoException) {
            this.topoException = topoException;
        }
    }

    /**
     *
     */
    public static class WatchDataResponse {
        private Topo.WatchData current;

        private BlockingQueue<WatchData> change;

        private Watch.Watcher watcher;

        public Topo.WatchData getCurrent() {
            return current;
        }

        public void setCurrent(Topo.WatchData current) {
            this.current = current;
        }

        public BlockingQueue<Topo.WatchData> getChange() {
            return change;
        }

        public void setChange(BlockingQueue<Topo.WatchData> change) {
            this.change = change;
        }

        public Watch.Watcher getWatcher() {
            return watcher;
        }

        public void setWatcher(Watch.Watcher watcher) {
            this.watcher = watcher;
        }
    }

    /**
     *
     */
    public static class WatchSrvKeyspaceData {
        private Topodata.SrvKeyspace value;

        private TopoException topoException;

        public WatchSrvKeyspaceData(Topodata.SrvKeyspace value) {
            this.value = value;
        }

        public WatchSrvKeyspaceData(TopoException topoException) {
            this.topoException = topoException;
        }

        public Topodata.SrvKeyspace getValue() {
            return value;
        }

        public void setValue(Topodata.SrvKeyspace value) {
            this.value = value;
        }

        public TopoException getTopoException() {
            return topoException;
        }

        public void setTopoException(TopoException topoException) {
            this.topoException = topoException;
        }
    }

    /**
     *
     */
    public static class WatchSrvKeyspaceResponse {
        private Topo.WatchSrvKeyspaceData current;

        private BlockingQueue<Topo.WatchSrvKeyspaceData> change;

        private Watch.Watcher watcher;

        public WatchSrvKeyspaceResponse(WatchSrvKeyspaceData current) {
            this.current = current;
        }

        public WatchSrvKeyspaceResponse(WatchSrvKeyspaceData current, BlockingQueue<WatchSrvKeyspaceData> change, Watch.Watcher watcher) {
            this.current = current;
            this.change = change;
            this.watcher = watcher;
        }

        public WatchSrvKeyspaceData getCurrent() {
            return current;
        }

        public void setCurrent(WatchSrvKeyspaceData current) {
            this.current = current;
        }

        public BlockingQueue<WatchSrvKeyspaceData> getChange() {
            return change;
        }

        public void setChange(BlockingQueue<WatchSrvKeyspaceData> change) {
            this.change = change;
        }

        public Watch.Watcher getWatcher() {
            return watcher;
        }

        public void setWatcher(Watch.Watcher watcher) {
            this.watcher = watcher;
        }
    }

    /**
     *
     */
    public static class GetSrvKeyspaceNamesResponse {
        private final List<String> keyspaceNames;

        private final Exception exception;

        public GetSrvKeyspaceNamesResponse(List<String> keyspaceNames, Exception exception) {
            this.keyspaceNames = keyspaceNames;
            this.exception = exception;
        }

        public List<String> getKeyspaceNames() {
            return keyspaceNames;
        }

        public Exception getException() {
            return exception;
        }
    }
}
