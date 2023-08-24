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
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class TopoStatsConnection implements TopoConnection {
    String cell;

    TopoConnection topoConnection;

    public TopoStatsConnection(String cell, TopoConnection topoConnection) {
        this.cell = cell;
        this.topoConnection = topoConnection;
    }

    @Override
    public List<DirEntry> listDir(IContext ctx, String dirPath, boolean isFull, boolean withSerializable) throws TopoException {
        return this.topoConnection.listDir(ctx, dirPath, isFull, withSerializable);
    }

    @Override
    public CompletableFuture<List<DirEntry>> listDirFuture(IContext ctx, String dirPath, Boolean isFull) {
        return this.topoConnection.listDirFuture(ctx, dirPath, isFull);
    }

    @Override
    public Version create(IContext ctx, String filePath, byte[] contents) throws TopoException {
        return this.topoConnection.create(ctx, filePath, contents);
    }

    @Override
    public ConnGetResponse get(IContext ctx, String filePath, boolean ignoreNoNode) throws TopoException {
        return this.topoConnection.get(ctx, filePath, ignoreNoNode);
    }

    @Override
    public List<ConnGetResponse> getTabletsByCell(IContext ctx, String filePath) throws TopoException {
        return this.topoConnection.getTabletsByCell(ctx, filePath);
    }

    @Override
    public CompletableFuture<ConnGetResponse> getFuture(IContext ctx, String filePath) {
        return this.topoConnection.getFuture(ctx, filePath);
    }

    @Override
    public void watchSrvKeyspace(IContext ctx, String cell, String keyspace) throws TopoException {
        this.topoConnection.watchSrvKeyspace(ctx, cell, keyspace);
    }

    @Override
    public void close() {

    }
}
