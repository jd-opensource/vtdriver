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

import com.jd.jdbc.context.VtBackgroundContext;
import static com.jd.jdbc.topo.TopoExceptionCode.NO_NODE;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Synchronized;
import org.apache.commons.lang3.RandomUtils;

@Getter
public class MemoryTopoFactory implements TopoFactory {

    private static final String GLOBAL_CELL = "global";

    private final Map<String, MemoryTopoServer.Node> cells;

    private long generation;

    private MemoryTopoFactory() {
        cells = new ConcurrentHashMap<>();
        generation = RandomUtils.nextLong();
    }

    public static ServerWithFactory newServerAndFactory(String... cells) throws TopoException {
        MemoryTopoFactory factory = new MemoryTopoFactory();
        factory.getCells().put(GLOBAL_CELL, factory.newDirectory(GLOBAL_CELL, null));
        TopoServer topoServer = Topo.newWithFactory(factory, "", "");

        for (String cell : cells) {
            TopoConnection globalCell = topoServer.globalCell;
            globalCell.create(new VtBackgroundContext(), Topo.pathForCellInfo(cell), null);
            factory.getCells().put(cell, factory.newDirectory(cell, null));
        }
        return new ServerWithFactory(topoServer, factory);
    }

    public MemoryTopoServer.Node newFile(String name, byte[] contents, MemoryTopoServer.Node parent) {
        return new MemoryTopoServer.Node(name, getNextVersion(), contents, new ConcurrentHashMap<>(), parent, false);
    }

    public MemoryTopoServer.Node newDirectory(String name, MemoryTopoServer.Node parent) {
        return newFile(name, null, parent);
    }

    public long getNextVersion() {
        this.generation++;
        return this.generation;
    }

    public MemoryTopoServer.Node nodeByPath(String cell, String filePath) {
        MemoryTopoServer.Node node = this.cells.get(cell);
        if (node == null) {
            return null;
        }

        String[] parts = filePath.split("/");
        for (String part : parts) {
            if ("".equals(part)) {
                continue;
            }
            if (node.getChildren() == null || node.getChildren().isEmpty()) {
                return null;
            }
            MemoryTopoServer.Node child = node.getChildren().get(part);
            if (child == null) {
                return null;
            }
            node = child;
        }
        return node;
    }

    public void deleteNode(String deleteCell) throws TopoException {
        String cell = "global";
        String filePath = "cells";
        MemoryTopoServer.Node node = nodeByPath(cell, filePath);
        if (node == null) {
            throw TopoException.wrap(NO_NODE, filePath);
        }

        if (!node.isDirectory()) {
            throw TopoException.wrap("node " + filePath + " in cell " + cell + " is not a directory");
        }
        node.getChildren().remove(deleteCell);
    }

    public MemoryTopoServer.Node getOrCreatePath(String cell, String filePath) {
        MemoryTopoServer.Node node = this.cells.get(cell);
        if (node == null) {
            return null;
        }

        String[] parts = filePath.split("/");
        for (String part : parts) {
            if ("".equals(part)) {
                continue;
            }
            if (node.getChildren() == null) {
                return null;
            }
            MemoryTopoServer.Node child = node.getChildren().get(part);
            if (child == null) {
                child = this.newDirectory(part, node);
                node.getChildren().put(part, child);
            }
            node = child;
        }
        return node;
    }

    @Override
    public Boolean hasGlobalReadOnlyCell(String name, String id) {
        return false;
    }

    @Override
    @Synchronized
    public TopoConnection create(String cell, String serverAddr, String root) throws TopoException {

        if (!this.cells.containsKey(cell)) {
            throw TopoException.wrap(TopoExceptionCode.NO_NODE, cell);
        }
        return new MemoryTopoServer(cell, serverAddr, this);
    }

    @AllArgsConstructor
    @Getter
    public static class ServerWithFactory {

        private final TopoServer topoServer;

        private final TopoFactory factory;
    }
}
