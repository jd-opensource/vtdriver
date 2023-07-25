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

package com.jd.jdbc.topo.etcd2topo;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.options.WatchOption;
import java.nio.charset.StandardCharsets;

public class EtcdWatcher implements Runnable {
    private final String key;

    private final WatchOption watchOption;

    private final Watch.Listener listener;

    private Watch.Watcher watcher;

    private final Client client;

    public EtcdWatcher(String key, WatchOption watchOption, Watch.Listener listener, Client client) {
        this.key = key;
        this.watchOption = watchOption;
        this.listener = listener;
        this.client = client;
    }

    @Override
    public void run() {
        this.watcher = client.getWatchClient().watch(buildByteSequenceKey(key), watchOption, this.listener);
    }

    /**
     * stop this task
     */
    public void stop() {
        this.watcher.close();
    }

    private ByteSequence buildByteSequenceKey(String key) {
        return ByteSequence.from(key, StandardCharsets.UTF_8);
    }
}