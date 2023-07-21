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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public enum TopologyWatcherManager {
    /**
     * INSTANCE
     */
    INSTANCE;

    private final Lock lock = new ReentrantLock();

    private Map<String, TopologyWatcher> cellTopologyWatcherMap = null;

    TopologyWatcherManager() {
        cellTopologyWatcherMap = new ConcurrentHashMap<>(16);
    }

    public void startWatch(IContext ctx, TopoServer topoServer, String cell, String tabletKeyspace) {
        lock.lock();
        try {
            if (!cellTopologyWatcherMap.containsKey(cell)) {
                TopologyWatcher topologyWatcher = new TopologyWatcher(topoServer, cell, tabletKeyspace);
                topologyWatcher.start(ctx);
                cellTopologyWatcherMap.put(cell, topologyWatcher);
            }
        } finally {
            lock.unlock();
        }
    }

    public void watch(IContext ctx, String cell, String tabletKeyspace) {
        if (!cellTopologyWatcherMap.containsKey(cell)) {
            throw new RuntimeException("topo watcher for cell " + cell + " is not started");
        }
        cellTopologyWatcherMap.get(cell).watchKeyspace(ctx, tabletKeyspace);
    }

    public void close() {
        for (Map.Entry<String, TopologyWatcher> entry : cellTopologyWatcherMap.entrySet()) {
            TopologyWatcher topologyWatcher = entry.getValue();
            topologyWatcher.close();
        }
        cellTopologyWatcherMap.clear();
    }
}
