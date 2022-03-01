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
import com.jd.jdbc.monitor.SrvKeyspaceCollector;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.topo.TopoConnection.DirEntry;
import com.jd.jdbc.topo.topoproto.TopoProto;
import com.jd.jdbc.util.JsonUtil;
import com.jd.jdbc.util.threadpool.impl.VtDaemonExecutorService;
import io.vitess.proto.Topodata;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import vschema.Vschema;

import static com.jd.jdbc.topo.Topo.GetSrvKeyspaceNamesResponse;
import static com.jd.jdbc.topo.Topo.WatchData;
import static com.jd.jdbc.topo.Topo.WatchDataResponse;
import static com.jd.jdbc.topo.Topo.WatchSrvKeyspaceData;
import static com.jd.jdbc.topo.Topo.WatchSrvKeyspaceResponse;
import static com.jd.jdbc.topo.Topo.dirEntriesToStringArray;
import static com.jd.jdbc.topo.Topo.pathForCellAlias;
import static com.jd.jdbc.topo.Topo.pathForCellInfo;
import static com.jd.jdbc.topo.Topo.pathForSrvKeyspaceFile;
import static com.jd.jdbc.topo.Topo.pathForTabletAlias;
import static com.jd.jdbc.topo.Topo.pathForVschemaFile;
import static com.jd.jdbc.topo.TopoConnection.ConnGetResponse;
import static com.jd.jdbc.topo.TopoExceptionCode.NO_NODE;

public class TopoServer implements Resource, TopoCellInfo, TopoCellsAliases, TopoSrvKeyspace, TopoSrvVschema, TopoTablet, TopoVschema {

    static final String GLOBAL_CELL = "global";

    static final String GLOBAL_READ_ONLY_CELL = "global-read-only";

    static final String CELL_INFO_FILE = "CellInfo";

    static final String CELLS_ALIAS_FILE = "CellsAlias";

    static final String KEYSPACE_FILE = "Keyspace";

    static final String SHARD_FILE = "Shard";

    static final String VSCHEMA_FILE = "VSchema";

    static final String SHARD_REPLICATION_FILE = "ShardReplication";

    static final String TABLET_FILE = "Tablet";

    static final String SRV_VSCHEMA_FILE = "SrvVSchema";

    static final String SRV_KEYSPACE_FILE = "SrvKeyspace";

    static final String ROUTING_RULES_FILE = "RoutingRules";

    static final String CELLS_PATH = "cells";

    static final String CELLS_ALIASES_PATH = "cells_aliases";

    static final String KEYSPACES_PATH = "keyspaces";

    static final String SHARDS_PATH = "shards";

    static final String TABLETS_PATH = "tablets";

    static final String METADATA_PATH = "metadata";

    private final TopoCellsToAliasesMap cellsAliases;

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
        cellsAliases = new TopoCellsToAliasesMap(new HashMap<>(16));
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
            String serverAddr = StringUtils.isEmpty(cellInfo.getProxyServerAddress()) ? cellInfo.getServerAddress() : cellInfo.getProxyServerAddress();
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
     * @param ctx
     * @param topoServer
     * @param cell
     * @return
     */
    public String getAliasByCell(IContext ctx, TopoServer topoServer, String cell) {
        cellsAliases.lock.lock();
        try {
            if (cellsAliases.cellsToAliases.containsKey(cell)) {
                return cellsAliases.cellsToAliases.get(cell);
            }
            if (topoServer != null) {
                Map<String, Topodata.CellsAlias> cellsAliasMap = topoServer.getCellsAliases(ctx, false);
                for (Map.Entry<String, Topodata.CellsAlias> entry : cellsAliasMap.entrySet()) {
                    String alias = entry.getKey();
                    Topodata.CellsAlias cellsAlias = entry.getValue();
                    for (String cellAlias : cellsAlias.getCellsList()) {
                        if (cellAlias.equalsIgnoreCase(cell)) {
                            cellsAliases.cellsToAliases.put(cell, alias);
                            return alias;
                        }
                    }
                }
            }
        } catch (TopoException e) {
            return cell;
        } finally {
            cellsAliases.lock.unlock();
        }
        return cell;
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
     *
     */
    private void clearCellAliasesCache() {
        cellsAliases.lock.lock();
        try {
            cellsAliases.cellsToAliases = new HashMap<>(16);
        } finally {
            cellsAliases.lock.unlock();
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

    /**
     * @param ctx
     * @param strongRead
     * @return
     * @throws TopoException
     */
    @Override
    public Map<String, Topodata.CellsAlias> getCellsAliases(IContext ctx, Boolean strongRead) throws TopoException {
        TopoConnection conn = this.globalCell;
        if (!strongRead) {
            conn = this.globalReadOnlyCell;
        }

        try {
            List<DirEntry> dirEntryList = this.globalCell.listDir(ctx, CELLS_ALIASES_PATH, false, false);
            List<String> aliasList = dirEntriesToStringArray(dirEntryList);
            Map<String, Topodata.CellsAlias> ret = new HashMap<>(aliasList.size());
            for (String alias : aliasList) {
                String aliasPath = pathForCellAlias(alias);
                ConnGetResponse connGetResponse = conn.get(ctx, aliasPath);
                Topodata.CellsAlias cellsAlias = JsonUtil.parseObject(connGetResponse.getContents(), Topodata.CellsAlias.class);
                ret.put(alias, cellsAlias);
            }
            return ret;
        } catch (java.lang.Exception e) {
            if (TopoException.isErrType(e, NO_NODE)) {
                return null;
            } else {
                throw e;
            }
        }
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
        return srvKeyspace;
    }

    /**
     * @param ctx
     * @param cell
     * @param keyspace
     * @return
     * @throws TopoException
     */
    @Override
    public WatchSrvKeyspaceResponse watchSrvKeyspace(IContext ctx, String cell, String keyspace) throws TopoException {
        TopoConnection topoConnection = this.connForCell(ctx, cell);

        String filePath = pathForSrvKeyspaceFile(keyspace);
        WatchDataResponse watchDataResponse = topoConnection.watch(ctx, filePath);
        TopoException topoException = watchDataResponse.getCurrent().getTopoException();
        if (topoException != null) {
            return new WatchSrvKeyspaceResponse(new WatchSrvKeyspaceData(topoException));
        }

        Topodata.SrvKeyspace value;
        try {
            value = Topodata.SrvKeyspace.parseFrom(watchDataResponse.getCurrent().getContents());
        } catch (InvalidProtocolBufferException e) {
            watchDataResponse.getWatcher().close();
            return new WatchSrvKeyspaceResponse(new WatchSrvKeyspaceData(
                TopoException.wrap(String.format("%s, error unpacking initial SrvKeyspace object", e.getMessage()))));
        }

        BlockingQueue<WatchSrvKeyspaceData> change = new LinkedBlockingQueue<>(1);
        VtDaemonExecutorService.execute(new WatchSrvKeyspaceTask(watchDataResponse, change, cell, keyspace));

        return new WatchSrvKeyspaceResponse(new WatchSrvKeyspaceData(value), change, watchDataResponse.getWatcher());
    }

    /**
     * @param ctx
     * @param cell
     * @return
     */
    @Override
    public GetSrvKeyspaceNamesResponse getSrvKeyspaceNames(IContext ctx, String cell) {
        TopoConnection topoConnection;
        try {
            topoConnection = this.connForCell(ctx, cell);
        } catch (TopoException e) {
            return new GetSrvKeyspaceNamesResponse(null, e);
        }

        try {
            List<DirEntry> dirEntryList = topoConnection.listDir(ctx, KEYSPACES_PATH, false, false);
            return new GetSrvKeyspaceNamesResponse(dirEntriesToStringArray(dirEntryList), null);
        } catch (TopoException e) {
            if (TopoException.isErrType(e, NO_NODE)) {
                return new GetSrvKeyspaceNamesResponse(null, null);
            }
            return new GetSrvKeyspaceNamesResponse(null, e);
        }
    }

    /**
     * @param ctx
     * @param cell
     * @return
     * @throws TopoException
     */
    @Override
    public List<TopoTabletInfo> getTabletsByRange(IContext ctx, String cell) throws TopoException {
        TopoConnection topoConnection = this.connForCell(ctx, cell);
        List<ConnGetResponse> connGetResponseList = topoConnection.getTabletsByCell(ctx, TABLETS_PATH);
        List<TopoTabletInfo> topoTabletInfos = new ArrayList<>(connGetResponseList.size());
        Topodata.Tablet tablet;
        try {
            for (ConnGetResponse connGetResponse : connGetResponseList) {
                tablet = Topodata.Tablet.parseFrom(connGetResponse.getContents());
                topoTabletInfos.add(new TopoTabletInfo(connGetResponse.getVersion(), tablet));
            }
        } catch (InvalidProtocolBufferException e) {
            throw TopoException.wrap(e.getMessage());
        }
        return topoTabletInfos;
    }

    /**
     * @param ctx
     * @param tabletAlias
     * @return
     * @throws TopoException
     */
    @Override
    public TopoTabletInfo getTablet(IContext ctx, Topodata.TabletAlias tabletAlias) throws TopoException {
        TopoConnection topoConnection = this.connForCell(ctx, tabletAlias.getCell());

        String tabletPath = pathForTabletAlias(TopoProto.tabletAliasString(tabletAlias));
        ConnGetResponse connGetResponse = topoConnection.get(ctx, tabletPath);
        Topodata.Tablet tablet;
        try {
            tablet = Topodata.Tablet.parseFrom(connGetResponse.getContents());
        } catch (InvalidProtocolBufferException e) {
            throw TopoException.wrap(e.getMessage());
        }
        return new TopoTabletInfo(connGetResponse.getVersion(), tablet);
    }

    @Override
    public CompletableFuture<TopoTabletInfo> getTabletFuture(IContext ctx, Topodata.TabletAlias tabletAlias) throws TopoException {
        TopoConnection topoConnection = this.connForCell(ctx, tabletAlias.getCell());
        String tabletPath = pathForTabletAlias(TopoProto.tabletAliasString(tabletAlias));
        return topoConnection.getFuture(ctx, tabletPath).thenApply(connGetResponse -> {
            Topodata.Tablet tablet;
            try {
                tablet = Topodata.Tablet.parseFrom(connGetResponse.getContents());
            } catch (InvalidProtocolBufferException e) {
                throw new CompletionException(TopoException.wrap(e.getMessage()));
            }
            return new TopoTabletInfo(connGetResponse.getVersion(), tablet);
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

class TopoCellsToAliasesMap {
    ReentrantLock lock;

    /**
     * cellsToAliases contains all cell->alias mappings
     */
    Map<String, String> cellsToAliases;

    public TopoCellsToAliasesMap(Map<String, String> cellsToAliases) {
        this.lock = new ReentrantLock(true);
        this.cellsToAliases = cellsToAliases;
    }
}

class WatchSrvKeyspaceTask implements Runnable {
    private static final Log logger = LogFactory.getLog(WatchSrvKeyspaceTask.class);

    private final WatchDataResponse watchDataResponse;

    private final BlockingQueue<WatchSrvKeyspaceData> change;

    private final String cell;

    private final String keyspace;

    public WatchSrvKeyspaceTask(WatchDataResponse watchDataResponse, BlockingQueue<WatchSrvKeyspaceData> change, String cell, String keyspace) {
        this.watchDataResponse = watchDataResponse;
        this.change = change;
        this.cell = cell;
        this.keyspace = keyspace;
    }

    @Override
    public void run() {
        while (true) {
            WatchData changeData;
            try {
                changeData = watchDataResponse.getChange().take();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
                SrvKeyspaceCollector.getErrorCounter().labels(keyspace, cell).inc();
                break;
            }

            TopoException topoEx = changeData.getTopoException();
            if (topoEx != null) {
                try {
                    this.change.put(new WatchSrvKeyspaceData(topoEx));
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(), e);
                }
                SrvKeyspaceCollector.getErrorCounter().labels(keyspace, cell).inc();
                break;
            }

            Topodata.SrvKeyspace srvKeyspace;
            try {
                srvKeyspace = Topodata.SrvKeyspace.parseFrom(changeData.getContents());
                SrvKeyspaceCollector.getCounter().labels(keyspace, cell).inc();
            } catch (InvalidProtocolBufferException e) {
                watchDataResponse.getWatcher().close();
                try {
                    this.change.put(new WatchSrvKeyspaceData(TopoException.wrap(
                        String.format("%s, error unpacking SrvKeyspace object", e.getMessage()))));
                } catch (InterruptedException ex) {
                    logger.error(ex.getMessage(), ex);
                }
                SrvKeyspaceCollector.getErrorCounter().labels(keyspace, cell).inc();
                break;
            }

            try {
                this.change.put(new WatchSrvKeyspaceData(srvKeyspace));
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
                SrvKeyspaceCollector.getErrorCounter().labels(keyspace, cell).inc();
                break;
            }
        }
    }
}
