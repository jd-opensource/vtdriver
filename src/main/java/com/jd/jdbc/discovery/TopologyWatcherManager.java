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

package com.jd.jdbc.discovery;

import com.jd.jdbc.context.IContext;
import com.jd.jdbc.topo.TopoServer;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public enum TopologyWatcherManager {
    INSTANCE;

    private final Lock lock = new ReentrantLock();

    private Map<String, TopologyWatcher> cellTopologyWatcherMap = null;

    TopologyWatcherManager() {
        cellTopologyWatcherMap = new HashMap<>();
    }

    public void startWatch(IContext ctx, TopoServer topoServer, String cell, Set<String> keySpaces) {
        lock.lock();
        try {
            if (!cellTopologyWatcherMap.containsKey(cell)) {
                TopologyWatcher topologyWatcher = new TopologyWatcher(topoServer, cell, keySpaces);
                topologyWatcher.start(ctx);
                cellTopologyWatcherMap.put(cell, topologyWatcher);
            }
        } finally {
            lock.unlock();
        }
    }

    public void watch(IContext ctx, String cell, Set<String> keySpaces) throws SQLException {
        lock.lock();
        try {
            if (!cellTopologyWatcherMap.containsKey(cell)) {
                throw new SQLException("topo watcher for cell " + cell + " is not started");
            }
            cellTopologyWatcherMap.get(cell).watchKeyspace(ctx, keySpaces);
        } finally {
            lock.unlock();
        }
    }
}
