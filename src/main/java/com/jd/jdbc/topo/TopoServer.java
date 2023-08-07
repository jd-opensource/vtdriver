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

import com.google.protobuf.InvalidProtocolBufferException;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.key.CurrentShard;
import static com.jd.jdbc.topo.Topo.pathForCellInfo;
import static com.jd.jdbc.topo.Topo.pathForSrvKeyspaceFile;
import static com.jd.jdbc.topo.Topo.pathForTabletAlias;
import static com.jd.jdbc.topo.Topo.pathForVschemaFile;
import static com.jd.jdbc.topo.TopoConnection.ConnGetResponse;
import com.jd.jdbc.topo.TopoConnection.DirEntry;
import static com.jd.jdbc.topo.TopoExceptionCode.NO_NODE;
import com.jd.jdbc.topo.topoproto.TopoProto;
import io.vitess.proto.Topodata;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import vschema.Vschema;

public class TopoServer implements Resource, TopoCellInfo, TopoSrvKeyspace, TopoTablet, TopoVschema {

    static final String GLOBAL_CELL = "global";

    static final String GLOBAL_READ_ONLY_CELL = "global-read-only";

    static final String CELL_INFO_FILE = "CellInfo";

    static final String VSCHEMA_FILE = "VSchema";

    static final String TABLET_FILE = "Tablet";

    static final String SRV_KEYSPACE_FILE = "SrvKeyspace";

    static final String CELLS_PATH = "cells";

    static final String KEYSPACES_PATH = "keyspaces";

    static final String TABLETS_PATH = "tablets";

    private static final Map<String, Topodata.SrvKeyspace> SRVKEYSPACE_MAP = new ConcurrentHashMap<>();

    TopoConnection globalCell;

    TopoConnection globalReadOnlyCell;

    TopoFactory topoFactory;

    ReentrantLock lock;

    Map<String, TopoConnection> cells;

    /**
     *
     */
    TopoServer() {
        lock = new ReentrantLock(true);
    }

    /**
     * @param ctx
     * @param cell
     * @return
     * @throws TopoException
     */
    public TopoConnection connForCell(IContext ctx, String cell) throws TopoException {
        if (GLOBAL_CELL.equalsIgnoreCase(cell)) {
            return this.globalCell;
        }

        this.lock.lock();
        TopoConnection topoConnection;
        try {
            topoConnection = this.cells.get(cell);
        } finally {
            this.lock.unlock();
        }
        if (topoConnection != null) {
            return topoConnection;
        }

        Topodata.CellInfo cellInfo = this.getCellInfo(ctx, cell, false);

        this.lock.lock();
        try {
            topoConnection = this.cells.get(cell);
            if (topoConnection != null) {
                return topoConnection;
            }
            String serverAddr = cellInfo.getServerAddress();
            topoConnection = this.topoFactory.create(cell, serverAddr, cellInfo.getRoot());
            topoConnection = new TopoStatsConnection(cell, topoConnection);
            this.cells.put(cell, topoConnection);
            return topoConnection;
        } catch (Exception e) {
            if (TopoException.isErrType(e, NO_NODE)) {
                throw TopoException.wrap(NO_NODE, String.format("failed to create topo connection to %s, %s", cellInfo.getServerAddress(), cellInfo.getRoot()));
            } else {
                throw TopoException.wrap(String.format("failed to create topo connection to %s, %s", cellInfo.getServerAddress(), cellInfo.getRoot()));
            }
        } finally {
            this.lock.unlock();
        }
    }

    /**
     *
     */
    @Override
    public void close() {
        this.globalCell.close();
        if (this.globalReadOnlyCell != this.globalCell) {
            this.globalReadOnlyCell.close();
        }
        this.globalCell = null;
        this.globalReadOnlyCell = null;
        lock.lock();
        try {
            this.cells.forEach((s, topoConnection) -> topoConnection.close());
            this.cells = new HashMap<>(16);
        } finally {
            lock.unlock();
        }
    }

    /**
     * @param ctx
     * @param cell
     * @param strongRead
     * @return
     * @throws TopoException
     */
    @Override
    public Topodata.CellInfo getCellInfo(IContext ctx, String cell, Boolean strongRead) throws TopoException {
        TopoConnection conn = this.globalCell;
        if (!strongRead) {
            conn = this.globalReadOnlyCell;
        }
        String filePath = pathForCellInfo(cell);
        ConnGetResponse connGetResponse = conn.get(ctx, filePath);
        Topodata.CellInfo cellInfo;
        try {
            cellInfo = Topodata.CellInfo.parseFrom(connGetResponse.getContents());
        } catch (InvalidProtocolBufferException e) {
            throw TopoException.wrap(e.getMessage());
        }
        return cellInfo;
    }

    public static void updateSrvKeyspaceCache(String keyspace, Topodata.SrvKeyspace srvKeyspace) {
        SRVKEYSPACE_MAP.put(keyspace, srvKeyspace);
        CurrentShard.setShardReferences(keyspace, srvKeyspace);
    }

    public static Topodata.SrvKeyspace getSrvKeyspaceFromCache(String keyspace) {
        return SRVKEYSPACE_MAP.get(keyspace);
    }

    public static Map<String, Topodata.SrvKeyspace> getSrvkeyspaceMapCopy() {
        return new HashMap<>(SRVKEYSPACE_MAP);
    }

    /**
     * @param ctx
     * @param cell
     * @param keyspace
     * @return
     * @throws TopoException
     */
    @Override
    public Topodata.SrvKeyspace getSrvKeyspace(IContext ctx, String cell, String keyspace) throws TopoException {
        TopoConnection topoConnection = this.connForCell(ctx, cell);
        String nodePath = pathForSrvKeyspaceFile(keyspace);
        ConnGetResponse connGetResponse = topoConnection.get(ctx, nodePath);
        Topodata.SrvKeyspace srvKeyspace;
        try {
            srvKeyspace = Topodata.SrvKeyspace.parseFrom(connGetResponse.getContents());
        } catch (InvalidProtocolBufferException e) {
            throw TopoException.wrap(e.getMessage());
        }
        if (Objects.equals(srvKeyspace, Topodata.SrvKeyspace.getDefaultInstance())) {
            throw TopoException.wrap("SrvKeyspace Information missing");
        }
        return srvKeyspace;
    }

    @Override
    public Topodata.SrvKeyspace getAndWatchSrvKeyspace(IContext ctx, String cell, String keyspace) throws TopoException {
        Topodata.SrvKeyspace srvKeyspace = getSrvKeyspaceFromCache(keyspace);
        if (srvKeyspace != null) {
            return srvKeyspace;
        }
        this.lock.lock();
        try {
            srvKeyspace = getSrvKeyspaceFromCache(keyspace);
            if (srvKeyspace != null) {
                return srvKeyspace;
            }
            srvKeyspace = getSrvKeyspace(ctx, cell, keyspace);
            updateSrvKeyspaceCache(keyspace, srvKeyspace);
        } finally {
            this.lock.unlock();
        }
        TopoConnection topoConnection = this.connForCell(ctx, cell);
        topoConnection.watchSrvKeyspace(ctx, cell, keyspace);
        return srvKeyspace;
    }

    /**
     * @param ctx
     * @param cell
     * @return
     * @throws TopoException
     */
    @Override
    public List<Topodata.Tablet> getTabletsByRange(IContext ctx, String cell) throws TopoException {
        TopoConnection topoConnection = this.connForCell(ctx, cell);
        List<ConnGetResponse> connGetResponseList = topoConnection.getTabletsByCell(ctx, TABLETS_PATH);
        List<Topodata.Tablet> tablets = new ArrayList<>(connGetResponseList.size());
        Topodata.Tablet tablet;
        try {
            for (ConnGetResponse connGetResponse : connGetResponseList) {
                tablet = Topodata.Tablet.parseFrom(connGetResponse.getContents());
                if (tablet == null) {
                    continue;
                }
                tablets.add(tablet);
            }
        } catch (InvalidProtocolBufferException e) {
            throw TopoException.wrap(e.getMessage());
        }
        return tablets;
    }

    /**
     * @param ctx
     * @param tabletAlias
     * @return
     * @throws TopoException
     */
    @Override
    public Topodata.Tablet getTablet(IContext ctx, Topodata.TabletAlias tabletAlias) throws TopoException {
        TopoConnection topoConnection = this.connForCell(ctx, tabletAlias.getCell());

        String tabletPath = pathForTabletAlias(TopoProto.tabletAliasString(tabletAlias));
        ConnGetResponse connGetResponse = topoConnection.get(ctx, tabletPath);
        Topodata.Tablet tablet;
        try {
            tablet = Topodata.Tablet.parseFrom(connGetResponse.getContents());
            return tablet;
        } catch (InvalidProtocolBufferException e) {
            throw TopoException.wrap(e.getMessage());
        }
    }

    @Override
    public CompletableFuture<Topodata.Tablet> getTabletFuture(IContext ctx, Topodata.TabletAlias tabletAlias) throws TopoException {
        TopoConnection topoConnection = this.connForCell(ctx, tabletAlias.getCell());
        String tabletPath = pathForTabletAlias(TopoProto.tabletAliasString(tabletAlias));
        return topoConnection.getFuture(ctx, tabletPath).thenApply(connGetResponse -> {
            Topodata.Tablet tablet;
            try {
                tablet = Topodata.Tablet.parseFrom(connGetResponse.getContents());
                return tablet;
            } catch (InvalidProtocolBufferException e) {
                throw new CompletionException(TopoException.wrap(e.getMessage()));
            }
        });
    }

    /**
     * @param ctx
     * @param cell
     * @return
     * @throws TopoException
     */
    @Override
    public List<Topodata.TabletAlias> getTabletAliasByCell(IContext ctx, String cell) throws TopoException {
        TopoConnection topoConnection = this.connForCell(ctx, cell);
        try {
            List<DirEntry> children = topoConnection.listDir(ctx, TABLETS_PATH, false, true);
            List<Topodata.TabletAlias> result = new ArrayList<>(children.size());
            for (DirEntry child : children) {
                result.add(TopoProto.parseTabletAlias(child.getName()));
            }
            return result;
        } catch (TopoException e) {
            if (TopoException.isErrType(e, NO_NODE)) {
                return null;
            }
            throw e;
        }
    }

    @Override
    public CompletableFuture<List<Topodata.TabletAlias>> getTabletsByCellFuture(IContext ctx, String cell) throws TopoException {
        TopoConnection topoConnection = this.connForCell(ctx, cell);
        return topoConnection.listDirFuture(ctx, TABLETS_PATH, false).thenApply(children -> {
            List<Topodata.TabletAlias> result = new ArrayList<>(children.size());
            try {
                for (DirEntry child : children) {
                    result.add(TopoProto.parseTabletAlias(child.getName()));
                }
                return result;
            } catch (TopoException e) {
                if (TopoException.isErrType(e, NO_NODE)) {
                    return null;
                }
                throw new CompletionException(e);
            }
        });
    }

    /**
     * @param ctx
     * @param keyspaceName
     * @return
     * @throws TopoException
     */
    @Override
    public Vschema.Keyspace getVschema(IContext ctx, String keyspaceName) throws TopoException {
        String nodePath = pathForVschemaFile(keyspaceName);
        ConnGetResponse connGetResponse = this.globalCell.get(ctx, nodePath, true);
        Vschema.Keyspace keyspace;
        try {
            keyspace = vschema.Vschema.Keyspace.parseFrom(connGetResponse.getContents());
        } catch (InvalidProtocolBufferException e) {
            throw TopoException.wrap(e.getMessage());
        }
        return keyspace;
    }
}