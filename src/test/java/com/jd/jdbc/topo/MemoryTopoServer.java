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

import com.jd.jdbc.context.IContext;
import com.jd.jdbc.context.VtBackgroundContext;
import com.jd.jdbc.topo.topoproto.TopoProto;
import io.vitess.proto.Topodata;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.Getter;

import static com.jd.jdbc.topo.TopoExceptionCode.NODE_EXISTS;
import static com.jd.jdbc.topo.TopoExceptionCode.NO_NODE;
import static com.jd.jdbc.topo.TopoServer.TABLET_FILE;

@AllArgsConstructor
public class MemoryTopoServer implements TopoConnection {

    private final String cell;

    private final String serverAddr;

    private final MemoryTopoFactory factory;

    private static final String ELECTIONS_PATH = "elections";

    @Override
    public List<DirEntry> listDir(IContext ctx, String dirPath, boolean isFull, boolean withSerializable) throws TopoException {

        boolean isRoot = "".equals(dirPath) || "/".equals(dirPath);

        Node node = this.factory.nodeByPath(this.cell, dirPath);
        if (node == null) {
            throw TopoException.wrap(NO_NODE, dirPath);
        }

        if (!node.isDirectory()) {
            throw TopoException.wrap("node " + dirPath + " in cell " + this.cell + " is not a directory");
        }

        List<DirEntry> result = new ArrayList<>(node.getChildren().size());
        for (Map.Entry<String, Node> childMap : node.getChildren().entrySet()) {
            DirEntry e = new DirEntry();
            e.setName(childMap.getKey());
            if (isFull) {
                e.setDirEntryType(DirEntryType.TYPE_FILE);
                if (childMap.getValue().isDirectory()) {
                    e.setDirEntryType(DirEntryType.TYPE_DIRECTORY);
                }
                if (isRoot && childMap.getKey().equals(ELECTIONS_PATH)) {
                    e.setEphemeral(true);
                }
            }
            result.add(e);
        }
        Topo.dirEntriesToStringArray(result);
        return result;
    }

    @Override
    public CompletableFuture<List<DirEntry>> listDirFuture(IContext ctx, String dirPath, Boolean isFull) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return listDir(ctx, dirPath, isFull, false);
            } catch (TopoException e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    @Override
    public Version create(IContext ctx, String filePath, byte[] contents) throws TopoException {
        byte[] contentsCopy = contents;
        if (contentsCopy == null) {
            contentsCopy = new byte[] {};
        }

        String[] dirFile = pathSplit(filePath);
        String dir = dirFile[0];
        String file = dirFile[1];

        Node parent = this.factory.getOrCreatePath(this.cell, dir);
        if (parent == null) {
            throw TopoException.wrap("trying to creat file " + filePath + " in cell " + this.cell + " in a path that contains files");
        }

        if (parent.getChildren().containsKey(file)) {
            throw TopoException.wrap(NODE_EXISTS, file);
        }

        Node node = this.factory.newFile(file, contentsCopy, parent);
        parent.getChildren().put(file, node);
        return node;
    }

    private String[] pathSplit(String path) {
        int i = lastSlash(path);

        String[] strings = new String[] {path.substring(0, i + 1), path.substring(i + 1)};
        return strings;
    }

    private int lastSlash(String path) {
        int i = path.length() - 1;
        for (; i >= 0; i--) {
            if (path.charAt(i) == '/') {
                return i;
            }
        }
        return -1;
    }

    @Override
    public ConnGetResponse get(IContext ctx, String filePath, boolean ignoreNoNode) throws TopoException {
        Node node = this.factory.nodeByPath(cell, filePath);
        if (node == null) {
            throw TopoException.wrap(NO_NODE, filePath);
        }
        if (node.getContents() == null) {
            throw TopoException.wrap("cannot Get() directory " + filePath + " in cell " + cell);
        }

        ConnGetResponse response = new ConnGetResponse();
        response.setContents(node.getContents());
        response.setVersion(node);
        return response;
    }

    @Override
    public List<ConnGetResponse> getTabletsByCell(IContext ctx, String filePath) throws TopoException {
        Node node = this.factory.nodeByPath(cell, filePath);
        if (node == null) {
            throw TopoException.wrap(NO_NODE, filePath);
        }
        if (node.getChildren() == null || node.getChildren().isEmpty()) {
            throw TopoException.wrap("cannot Get() directory " + filePath + " in cell " + cell);
        }
        List<ConnGetResponse> connGetResponseList = new ArrayList<>(node.getChildren().size());
        ConnGetResponse connGetResponse;
        for (Map.Entry<String, Node> childMap : node.getChildren().entrySet()) {
            connGetResponse = new ConnGetResponse();
            connGetResponse.setContents(childMap.getValue().getChildren().get(TABLET_FILE).getContents());
            connGetResponse.setVersion(childMap.getValue().getChildren().get(TABLET_FILE));
            connGetResponseList.add(connGetResponse);
        }
        return connGetResponseList;
    }

    @Override
    public CompletableFuture<ConnGetResponse> getFuture(IContext ctx, String filePath) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return get(ctx, filePath);
            } catch (TopoException e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    @Override
    public void watchSrvKeyspace(IContext ctx, String cell, String keyspace) throws TopoException {

    }

    @Override
    public void close() {

    }

    public static void addCellInMemoryTopo(TopoServer topoServer, String cell) throws TopoException {
        TopoConnection globalCell = topoServer.globalCell;
        globalCell.create(new VtBackgroundContext(), Topo.pathForCellInfo(cell), null);
        TopoFactory factory = topoServer.topoFactory;
        if (factory instanceof MemoryTopoFactory) {
            MemoryTopoFactory memoryTopoFactory = (MemoryTopoFactory) factory;
            memoryTopoFactory.getCells().put(cell, memoryTopoFactory.newDirectory(cell, null));
        }
    }

    public static void deleteCellInMemoryTopo(IContext ctx, TopoServer topoServer, String cell) throws TopoException {
        TopoConnection globalCell = topoServer.globalCell;
        TopoFactory factory = topoServer.topoFactory;
        if (factory instanceof MemoryTopoFactory) {
            MemoryTopoFactory memoryTopoFactory = (MemoryTopoFactory) factory;
            memoryTopoFactory.deleteNode(cell);
            memoryTopoFactory.getCells().remove(cell);
        }
        List<String> allCells = topoServer.getAllCells(ctx);
    }

    // createTablet creates a new tablet and all associated paths for the
    // replication graph.
    public static void createTablet(IContext ctx, TopoServer topoServer, Topodata.Tablet tablet) throws TopoException {
        TopoConnection conn = topoServer.connForCell(ctx, tablet.getAlias().getCell());
        byte[] data = tablet.toByteArray();
        //Topodata.Tablet testTablet = Topodata.Tablet.parseFrom(data);
        String tabletPath = Topo.pathForTabletAlias(TopoProto.tabletAliasString(tablet.getAlias()));
        conn.create(ctx, tabletPath, data);
    }

    @AllArgsConstructor
    @Getter
    public static class Node implements Version {

        private final String name;

        private final long version;

        private final byte[] contents;

        private final Map<String, Node> children;

        private final Node parent;

        private boolean isLocked;

        private final AtomicInteger nextIndex = new AtomicInteger(0);

        @Override
        public String string() {
            return Long.toString(this.version);
        }

        public boolean isDirectory() {
            return this.children != null && !this.children.isEmpty();
        }
    }
}
