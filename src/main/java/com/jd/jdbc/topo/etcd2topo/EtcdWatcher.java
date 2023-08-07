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