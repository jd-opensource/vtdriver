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
import com.jd.jdbc.discovery.TopologyWatcherManager;
import com.jd.jdbc.key.CurrentShard;
import com.jd.jdbc.monitor.TopoServerCollector;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.srvtopo.ResilientServer;
import com.jd.jdbc.srvtopo.SrvTopoServer;
import static com.jd.jdbc.topo.Topo.pathForCellInfo;
import static com.jd.jdbc.topo.Topo.pathForSrvKeyspaceFile;
import static com.jd.jdbc.topo.Topo.pathForTabletAlias;
import static com.jd.jdbc.topo.Topo.pathForVschemaFile;
import static com.jd.jdbc.topo.TopoConnection.ConnGetResponse;
import com.jd.jdbc.topo.TopoConnection.DirEntry;
import static com.jd.jdbc.topo.TopoExceptionCode.NO_NODE;
import com.jd.jdbc.topo.topoproto.TopoProto;
import com.jd.jdbc.util.ScheduledManager;
import io.vitess.proto.Topodata;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import lombok.Setter;
import vschema.Vschema;

public class TopoServer implements Resource, TopoCellInfo, TopoSrvKeyspace, TopoTablet, TopoVschema {

    private static final Log log = LogFactory.getLog(TopoServer.class);

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

    Map<String, TopoConnection> cellsTopoConnMap;

    Set<String> cells;

    String localCell;

    Set<String> keyspaces;

    String serverAddress;

    @Setter
    private ScheduledManager scheduledManager;

    /**
     *
     */
    TopoServer() {
        lock = new ReentrantLock(true);
        scheduledManager = new ScheduledManager("reload-cell-schedule", 1, TimeUnit.MINUTES);
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
            topoConnection = this.cellsTopoConnMap.get(cell);
        } finally {
            this.lock.unlock();
        }
        if (topoConnection != null) {
            return topoConnection;
        }

        Topodata.CellInfo cellInfo = this.getCellInfo(ctx, cell, false);

        this.lock.lock();
        try {
            topoConnection = this.cellsTopoConnMap.get(cell);
            if (topoConnection != null) {
                return topoConnection;
            }
            String serverAddr = cellInfo.getServerAddress();
            topoConnection = this.topoFactory.create(cell, serverAddr, cellInfo.getRoot());
            topoConnection = new TopoStatsConnection(cell, topoConnection);
            this.cellsTopoConnMap.put(cell, topoConnection);
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
        scheduledManager.close();

        if (this.globalReadOnlyCell != this.globalCell) {
            this.globalReadOnlyCell.close();
        }
        this.globalCell = null;
        this.globalReadOnlyCell = null;
        this.keyspaces.clear();
        this.cells.clear();
        lock.lock();
        try {
            this.cellsTopoConnMap.forEach((s, topoConnection) -> topoConnection.close());
            this.cellsTopoConnMap = new HashMap<>(16);
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

    public List<String> getAllCells(IContext ctx) throws TopoException {
        List<TopoConnection.DirEntry> dirEntryList = this.globalCell.listDir(ctx, CELLS_PATH, false, false);
        List<String> cells = Topo.dirEntriesToStringArray(dirEntryList);
        if (cells.size() < 1) {
            throw TopoException.wrap("Cells Information missing");
        }
        return cells;
    }

    public Set<String> getCells(IContext ctx) throws TopoException {
        if (this.cells.isEmpty()) {
            this.cells.addAll(this.getAllCells(ctx));
        }
        return this.cells;
    }

    public String getLocalCell(IContext globalContext, SrvTopoServer srvTopoServer, Set<String> cells, String defaultKeyspace) throws TopoException {
        if (this.localCell.isEmpty()) {
            return resetLocalCell(globalContext, cells, defaultKeyspace);
        }

        String currentLocalCell = this.localCell;
        ResilientServer.GetSrvKeyspaceResponse srvKeyspace = srvTopoServer.getSrvKeyspace(globalContext, currentLocalCell, defaultKeyspace);
        Exception err = srvKeyspace.getException();
        if (err != null) {
            return resetLocalCell(globalContext, cells, defaultKeyspace);
        }

        return currentLocalCell;
    }

    private String resetLocalCell(IContext globalContext, Set<String> cells, String defaultKeyspace) throws TopoException {
        String errMessage = "";
        for (String cell : cells) {
            try {
                Topodata.SrvKeyspace getSrvKeyspace = this.getSrvKeyspace(globalContext, cell, defaultKeyspace);
                if (getSrvKeyspace != null) {
                    this.localCell = cell;
                    return cell;
                }
            } catch (TopoException e) {
                if (e.getCode() != NO_NODE) {
                    throw TopoException.wrap(e.getMessage());
                }
                errMessage = e.getMessage();
            }
        }

        throw TopoException.wrap(NO_NODE, errMessage + " OR invalid local cell: " + cells);
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
                log.debug("failed to get tablet in cell " + cell + " . error: " + e);
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

    public String getServerAddress() {
        return this.serverAddress;
    }

    public void addKeyspace(String ks) {
        this.keyspaces.add(ks);
    }

    public void startTickerReloadCell(IContext globalContext) {
        scheduledManager.getScheduledExecutor().scheduleWithFixedDelay(() -> {
            try {
                tickerUpdateCells(globalContext);
            } catch (Throwable e) {
                log.error("tickerUpdateCells error: " + e);
            }
        }, 5, 10, scheduledManager.getTimeUnit());
    }

    private void tickerUpdateCells(IContext globalContext) throws TopoException {
        List<String> allCells = this.getAllCells(globalContext);
        for (String cell : allCells) {
            if (!this.cells.contains(cell)) {
                this.cells.add(cell);
                TopologyWatcherManager.INSTANCE.startWatch(globalContext, this, cell, this.keyspaces);

                TopoServerCollector.geCellsCounter().labels(this.serverAddress).inc();
            }
        }
        TopoServerCollector.getExecCounterCounter().labels(this.serverAddress).inc();
    }
}
