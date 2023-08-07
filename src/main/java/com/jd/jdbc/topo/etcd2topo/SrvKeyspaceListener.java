package com.jd.jdbc.topo.etcd2topo;

import com.jd.jdbc.Executor;
import com.jd.jdbc.monitor.SrvKeyspaceCollector;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.topo.TopoServer;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import io.vitess.proto.Topodata;
import java.util.Objects;
import java.util.Optional;

public class SrvKeyspaceListener implements Watch.Listener {
    private static final Log LOGGER = LogFactory.getLog(Executor.class);

    private final String cell;

    private final String keyspace;

    public SrvKeyspaceListener(String cell, String keyspace) {
        this.cell = cell;
        this.keyspace = keyspace;
    }

    @Override
    public void onNext(WatchResponse response) {
        for (WatchEvent event : response.getEvents()) {
            switch (event.getEventType()) {
                case PUT:
                    try {
                        ByteSequence byteSequence = Optional.ofNullable(event.getKeyValue().getValue()).orElse(ByteSequence.EMPTY);
                        if (Objects.equals(byteSequence, ByteSequence.EMPTY)) {
                            LOGGER.error("SrvKeyspace Information missing " + "keyspace = " + keyspace + ",cell = " + cell);
                            SrvKeyspaceCollector.getErrorCounter().labels(keyspace, cell).inc();
                            break;
                        }
                        byte[] bytes = byteSequence.getBytes();
                        Topodata.SrvKeyspace srvKeyspace = Topodata.SrvKeyspace.parseFrom(bytes);
                        TopoServer.updateSrvKeyspaceCache(keyspace, srvKeyspace);
                        SrvKeyspaceCollector.getCounter().labels(keyspace, cell).inc();
                        LOGGER.info("SrvKeyspaceListener watch put key:" + event.getKeyValue().getKey() + ",srvKeyspace = " + srvKeyspace);
                    } catch (Exception e) {
                        SrvKeyspaceCollector.getErrorCounter().labels(keyspace, cell).inc();
                        LOGGER.error("put event error " + "keyspace = " + keyspace + ",cell = " + cell, e);
                    }
                    break;
                case DELETE:
                    LOGGER.error("delete key:" + event.getKeyValue().getKey());
                    break;
                default:
                    LOGGER.error("unexpected event received");
                    break;
            }
        }
    }

    @Override
    public void onError(Throwable throwable) {
        LOGGER.error("onError " + " keyspace = " + keyspace + ",cell = " + cell, throwable);
        SrvKeyspaceCollector.getErrorCounter().labels(keyspace, cell).inc();
    }

    @Override
    public void onCompleted() {
        LOGGER.error("onCompleted " + " keyspace = " + keyspace + ",cell = " + cell);
    }
}