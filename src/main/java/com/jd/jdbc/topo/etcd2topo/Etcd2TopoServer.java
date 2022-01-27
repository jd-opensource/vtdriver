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

import com.jd.jdbc.context.IContext;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.topo.Topo;
import com.jd.jdbc.topo.TopoConnection;
import com.jd.jdbc.topo.TopoException;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import io.etcd.jetcd.watch.WatchEvent;
import io.vitess.proto.Vtrpc;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

import static com.jd.jdbc.topo.TopoExceptionCode.NO_NODE;

public class Etcd2TopoServer implements TopoConnection {
    private static final Log logger = LogFactory.getLog(Etcd2TopoServer.class);

    private static final String SEPARATOR = "/";

    private static final String SEPARATOR_DUAL = "//";

    private static final String END_TAG_OF_RANGE_SEARCH = "1";

    private Client client;

    private String root;

    /**
     * @param ctx
     * @param dirPath
     * @param isFull
     * @return
     * @throws TopoException
     */
    @Override
    public List<DirEntry> listDir(IContext ctx, String dirPath, boolean isFull, boolean withSerializable) throws TopoException {
        String nodePath = String.format("%s%s%s%s", this.root, SEPARATOR, dirPath, SEPARATOR);
        if (SEPARATOR_DUAL.equals(nodePath)) {
            nodePath = "/";
        }

        ByteSequence sequence = ByteSequence.from(nodePath, StandardCharsets.UTF_8);
        GetOption option = GetOption.newBuilder()
            .withPrefix(ByteSequence.from(sequence.getBytes()))
            .withSortField(GetOption.SortTarget.KEY)
            .withSortOrder(GetOption.SortOrder.ASCEND)
            .withKeysOnly(true)
            .withSerializable(withSerializable)
            .build();
        CompletableFuture<GetResponse> future = this.client.getKVClient().get(sequence, option);
        GetResponse response;
        try {
            response = future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw TopoException.wrap(e.getMessage());
        }
        if (response.getKvs().isEmpty()) {
            throw TopoException.wrap(NO_NODE, nodePath);
        }

        int prefixLen = nodePath.length();
        List<DirEntry> result = new ArrayList<>(16);
        for (KeyValue kv : response.getKvs()) {
            String p = kv.getKey().toString(StandardCharsets.UTF_8);

            if (!p.startsWith(nodePath)) {
                throw TopoException.wrap("etcd request returned success, but response is missing required data");
            }
            p = p.substring(prefixLen);
            DirEntryType t = DirEntryType.TYPE_FILE;
            if (p.contains("/")) {
                p = p.substring(0, p.indexOf("/"));
                t = DirEntryType.TYPE_DIRECTORY;
            }

            if (result.isEmpty() || !p.equalsIgnoreCase(result.get(result.size() - 1).getName())) {
                DirEntry dirEntry = new DirEntry();
                dirEntry.setName(p);
                if (isFull) {
                    dirEntry.setDirEntryType(t);
                    if (kv.getLease() != 0) {
                        dirEntry.setEphemeral(true);
                    }
                }
                result.add(dirEntry);
            }
        }
        return result;
    }

    @Override
    public CompletableFuture<List<DirEntry>> listDirFuture(IContext ctx, String dirPath, Boolean isFull) {
        String nodePath = String.format("%s%s%s%s", this.root, SEPARATOR, dirPath, SEPARATOR);
        if (SEPARATOR_DUAL.equals(nodePath)) {
            nodePath = "/";
        }

        ByteSequence sequence = ByteSequence.from(nodePath, StandardCharsets.UTF_8);
        GetOption option = GetOption.newBuilder()
            .withPrefix(ByteSequence.from(sequence.getBytes()))
            .withSortField(GetOption.SortTarget.KEY)
            .withSortOrder(GetOption.SortOrder.ASCEND)
            .withKeysOnly(true)
            .withSerializable(true)
            .build();
        String finalNodePath = nodePath;
        return this.client.getKVClient().get(sequence, option).thenApply(response -> {
            if (response.getKvs().isEmpty()) {
                throw new CompletionException(TopoException.wrap(NO_NODE, finalNodePath));
            }

            int prefixLen = finalNodePath.length();
            List<DirEntry> result = new ArrayList<>(16);
            for (KeyValue kv : response.getKvs()) {
                String p = kv.getKey().toString(StandardCharsets.UTF_8);

                if (!p.startsWith(finalNodePath)) {
                    throw new CompletionException(TopoException.wrap("etcd request returned success, but response is missing required data"));
                }
                p = p.substring(prefixLen);
                DirEntryType t = DirEntryType.TYPE_FILE;
                if (p.contains("/")) {
                    p = p.substring(0, p.indexOf("/"));
                    t = DirEntryType.TYPE_DIRECTORY;
                }

                if (result.isEmpty() || !p.equalsIgnoreCase(result.get(result.size() - 1).getName())) {
                    DirEntry dirEntry = new DirEntry();
                    dirEntry.setName(p);
                    if (isFull) {
                        dirEntry.setDirEntryType(t);
                        if (kv.getLease() != 0) {
                            dirEntry.setEphemeral(true);
                        }
                    }
                    result.add(dirEntry);
                }
            }
            return result;
        });
    }

    /**
     * @param ctx
     * @param filePath
     * @param contents
     * @return
     * @throws TopoException
     */
    @Override
    public Version create(IContext ctx, String filePath, byte[] contents) throws TopoException {
        return null;
    }

    /**
     * @param ctx
     * @param filePath
     * @param contents
     * @param version
     * @return
     * @throws TopoException
     */
    @Override
    public Version update(IContext ctx, String filePath, byte[] contents, Version version) throws TopoException {
        return null;
    }

    /**
     * @param ctx
     * @param filePath
     * @return
     * @throws TopoException
     */
    @Override
    public ConnGetResponse get(IContext ctx, String filePath, boolean ignoreNoNode) throws TopoException {
        String nodePath = String.format("%s%s%s", this.root, SEPARATOR, filePath);
        ByteSequence sequence = ByteSequence.from(nodePath, StandardCharsets.UTF_8);
        GetOption option = GetOption.newBuilder().withSerializable(true).build();
        CompletableFuture<GetResponse> future = this.client.getKVClient().get(sequence, option);
        GetResponse response;
        try {
            response = future.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e);
            throw TopoException.wrap(e.getMessage());
        }
        if (response.getKvs().size() != 1) {
            if (ignoreNoNode) {
                ConnGetResponse connGetResponse = new ConnGetResponse();
                connGetResponse.setContents(new byte[] {});
                return connGetResponse;
            }
            throw TopoException.wrap(NO_NODE, nodePath);
        }
        ConnGetResponse connGetResponse = new ConnGetResponse();
        connGetResponse.setContents(response.getKvs().get(0).getValue().getBytes());
        connGetResponse.setVersion(new Etcd2Version(response.getKvs().get(0).getModRevision()));
        return connGetResponse;
    }

    @Override
    public List<ConnGetResponse> getTabletsByCell(IContext ctx, String filePath) throws TopoException {
        String beginTabletsPath = String.format("%s%s%s", this.root, SEPARATOR, filePath);
        String endTabletsPath = beginTabletsPath + END_TAG_OF_RANGE_SEARCH;
        ByteSequence beginSequence = ByteSequence.from(beginTabletsPath, StandardCharsets.UTF_8);
        ByteSequence endSequence = ByteSequence.from(endTabletsPath, StandardCharsets.UTF_8);
        GetOption option = GetOption.newBuilder().withRange(endSequence).build();
        CompletableFuture<GetResponse> future = this.client.getKVClient().get(beginSequence, option);
        GetResponse response;
        try {
            response = future.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e);
            throw TopoException.wrap(e.getMessage());
        }
        if (response.getKvs().size() < 1) {
            throw TopoException.wrap(NO_NODE, beginTabletsPath);
        }
        List<ConnGetResponse> connGetResponseList = new ArrayList<>(response.getKvs().size());
        ConnGetResponse connGetResponse;
        for (KeyValue kv : response.getKvs()) {
            connGetResponse = new ConnGetResponse();
            connGetResponse.setContents(kv.getValue().getBytes());
            connGetResponse.setVersion(new Etcd2Version(kv.getModRevision()));
            connGetResponseList.add(connGetResponse);
        }
        return connGetResponseList;
    }

    @Override
    public CompletableFuture<ConnGetResponse> getFuture(IContext ctx, String filePath) {
        String nodePath = String.format("%s%s%s", this.root, SEPARATOR, filePath);
        ByteSequence sequence = ByteSequence.from(nodePath, StandardCharsets.UTF_8);
        GetOption option = GetOption.newBuilder().withSerializable(true).build();
        return this.client.getKVClient().get(sequence, option).thenApply(response -> {
            if (response.getKvs().size() != 1) {
                throw new CompletionException(TopoException.wrap(NO_NODE, nodePath));
            }
            ConnGetResponse connGetResponse = new ConnGetResponse();
            connGetResponse.setContents(response.getKvs().get(0).getValue().getBytes());
            connGetResponse.setVersion(new Etcd2Version(response.getKvs().get(0).getModRevision()));
            return connGetResponse;
        });
    }

    /**
     * @param ctx
     * @param filePath
     * @param version
     * @throws TopoException
     */
    @Override
    public void delete(IContext ctx, String filePath, Version version) throws TopoException {

    }

    /**
     * @param ctx
     * @param dirPath
     * @param contents
     * @return
     * @throws TopoException
     */
    @Override
    public LockDescriptor lock(IContext ctx, String dirPath, String contents) throws TopoException {
        return null;
    }

    /**
     * @param ctx
     * @param filePath
     * @return
     */
    @Override
    public Topo.WatchDataResponse watch(IContext ctx, String filePath) throws TopoException {
        String nodePath = String.format("%s%s%s", this.root, SEPARATOR, filePath);
        ByteSequence key = ByteSequence.from(nodePath, StandardCharsets.UTF_8);
        GetOption option = GetOption.newBuilder().withSerializable(true).build();
        CompletableFuture<GetResponse> future = this.client.getKVClient().get(key, option);
        GetResponse initial;
        try {
            initial = future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw TopoException.wrap(e.getMessage());
        }
        if (initial.getKvs().size() != 1) {
            throw TopoException.wrap(NO_NODE, nodePath);
        }
        Topo.WatchData watchData = new Topo.WatchData();
        watchData.setContents(initial.getKvs().get(0).getValue().getBytes());
        watchData.setVersion(new Etcd2Version(initial.getKvs().get(0).getModRevision()));

        BlockingQueue<Topo.WatchData> notification = new LinkedBlockingQueue<>(1);

        long revision = initial.getHeader().getRevision();
        WatchOption watchOption = WatchOption.newBuilder().withRevision(revision).build();
        Watch.Watcher watcher = this.client.getWatchClient().watch(key, watchOption, Watch.listener(
            watchResponse -> {
                cancel:
                for (WatchEvent event : watchResponse.getEvents()) {
                    switch (event.getEventType()) {
                        case PUT:
                            byte[] bytes = event.getKeyValue().getValue().getBytes();
                            Etcd2Version etcd2Version = new Etcd2Version(event.getKeyValue().getVersion());
                            try {
                                notification.put(new Topo.WatchData(bytes, etcd2Version));
                            } catch (InterruptedException e) {
                                logger.error(e.getMessage(), e);
                                break cancel;
                            }
                            break;
                        case DELETE:
                            try {
                                notification.put(new Topo.WatchData(TopoException.wrap(NO_NODE, nodePath)));
                            } catch (InterruptedException e) {
                                logger.error(e.getMessage(), e);
                            }
                            break;
                        default:
                            try {
                                notification.put(new Topo.WatchData(TopoException.wrap(Vtrpc.Code.INTERNAL,
                                    String.format("unexpected event received: %s", event))));
                            } catch (InterruptedException e) {
                                logger.error(e.getMessage(), e);
                            }
                            break;
                    }
                }
            }, throwable -> {
                try {
                    notification.put(new Topo.WatchData(TopoException.wrap(throwable.getMessage())));
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(), e);
                }
            }));

        Topo.WatchDataResponse watchDataResponse = new Topo.WatchDataResponse();
        watchDataResponse.setCurrent(watchData);
        watchDataResponse.setChange(notification);
        watchDataResponse.setWatcher(watcher);
        return watchDataResponse;
    }

    /**
     * @param id
     * @param name
     * @return
     * @throws TopoException
     */
    @Override
    public MasterParticipation newMasterParticipation(String id, String name) throws TopoException {
        return null;
    }

    @Override
    public void close() {
        this.client.close();
        this.client = null;
        this.root = null;
    }

    public void setClient(Client client) {
        this.client = client;
    }

    public void setRoot(String root) {
        this.root = root;
    }
}
