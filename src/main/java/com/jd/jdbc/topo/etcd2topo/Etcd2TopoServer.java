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

import com.jd.jdbc.common.util.MapUtil;
import com.jd.jdbc.context.IContext;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.topo.Topo;
import com.jd.jdbc.topo.TopoConnection;
import com.jd.jdbc.topo.TopoException;
import com.jd.jdbc.topo.TopoExceptionCode;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.WatchOption;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Etcd2TopoServer implements TopoConnection {
    private static final Log logger = LogFactory.getLog(Etcd2TopoServer.class);

    private static final String SEPARATOR = "/";

    private static final String SEPARATOR_DUAL = "//";

    private static final String END_TAG_OF_RANGE_SEARCH = "1";

    private static final int DEFALUT_TIMEOUT = 3;

    private static final ConcurrentMap<String, Watch.Watcher> WATCHER_MAP = new ConcurrentHashMap<>(16);

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
        String nodePath = this.root + SEPARATOR + dirPath + SEPARATOR;
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
            response = future.get(DEFALUT_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error(e.getMessage(), e);
            throw TopoException.wrap(e.getMessage());
        }
        if (response.getKvs().isEmpty()) {
            throw TopoException.wrap(TopoExceptionCode.NO_NODE, nodePath);
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
        String nodePath = this.root + SEPARATOR + dirPath + SEPARATOR;
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
                throw new CompletionException(TopoException.wrap(TopoExceptionCode.NO_NODE, finalNodePath));
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

    @Override
    public Version create(IContext ctx, String filePath, byte[] contents) throws TopoException {
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
        String nodePath = this.root + SEPARATOR + filePath;
        ByteSequence sequence = ByteSequence.from(nodePath, StandardCharsets.UTF_8);
        GetOption option = GetOption.newBuilder().withSerializable(true).build();
        CompletableFuture<GetResponse> future = this.client.getKVClient().get(sequence, option);
        GetResponse response;
        try {
            response = future.get(DEFALUT_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error(e.getMessage(), e);
            throw TopoException.wrap(e.getMessage());
        }
        if (response.getKvs().size() != 1) {
            if (ignoreNoNode) {
                ConnGetResponse connGetResponse = new ConnGetResponse();
                connGetResponse.setContents(new byte[] {});
                return connGetResponse;
            }
            throw TopoException.wrap(TopoExceptionCode.NO_NODE, nodePath);
        }
        ConnGetResponse connGetResponse = new ConnGetResponse();
        connGetResponse.setContents(response.getKvs().get(0).getValue().getBytes());
        return connGetResponse;
    }

    @Override
    public List<ConnGetResponse> getTabletsByCell(IContext ctx, String filePath) throws TopoException {
        String beginTabletsPath = this.root + SEPARATOR + filePath;
        String endTabletsPath = beginTabletsPath + END_TAG_OF_RANGE_SEARCH;
        ByteSequence beginSequence = ByteSequence.from(beginTabletsPath, StandardCharsets.UTF_8);
        ByteSequence endSequence = ByteSequence.from(endTabletsPath, StandardCharsets.UTF_8);
        GetOption option = GetOption.newBuilder().withRange(endSequence).build();
        CompletableFuture<GetResponse> future = this.client.getKVClient().get(beginSequence, option);
        GetResponse response;
        try {
            response = future.get(DEFALUT_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            logger.error(e.getMessage(), e);
            throw TopoException.wrap(e.getMessage());
        }
        if (response.getKvs().size() < 1) {
            throw TopoException.wrap(TopoExceptionCode.NO_NODE, beginTabletsPath);
        }
        List<ConnGetResponse> connGetResponseList = new ArrayList<>(response.getKvs().size());
        ConnGetResponse connGetResponse;
        for (KeyValue kv : response.getKvs()) {
            connGetResponse = new ConnGetResponse();
            connGetResponse.setContents(kv.getValue().getBytes());
            connGetResponseList.add(connGetResponse);
        }
        return connGetResponseList;
    }

    @Override
    public CompletableFuture<ConnGetResponse> getFuture(IContext ctx, String filePath) {
        String nodePath = this.root + SEPARATOR + filePath;
        ByteSequence sequence = ByteSequence.from(nodePath, StandardCharsets.UTF_8);
        GetOption option = GetOption.newBuilder().withSerializable(true).build();
        return this.client.getKVClient().get(sequence, option).thenApply(response -> {
            if (response.getKvs().size() != 1) {
                throw new CompletionException(TopoException.wrap(TopoExceptionCode.NO_NODE, nodePath));
            }
            ConnGetResponse connGetResponse = new ConnGetResponse();
            connGetResponse.setContents(response.getKvs().get(0).getValue().getBytes());
            return connGetResponse;
        });
    }

    @Override
    public void watchSrvKeyspace(IContext ctx, String cell, String keyspace) throws TopoException {
        String nodePath = this.root + SEPARATOR + Topo.pathForSrvKeyspaceFile(keyspace);
        if (WATCHER_MAP.containsKey(nodePath)) {
            return;
        }

        // get revision
        ByteSequence key = ByteSequence.from(nodePath, StandardCharsets.UTF_8);
        GetOption option = GetOption.newBuilder().build();
        CompletableFuture<GetResponse> future = this.client.getKVClient().get(key, option);
        GetResponse initial;
        try {
            initial = future.get(DEFALUT_TIMEOUT, TimeUnit.SECONDS);
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            throw TopoException.wrap(e.getMessage());
        }
        long revision = initial.getHeader().getRevision();

        // watch SrvKeyspace
        WatchOption watchOption = WatchOption.newBuilder().withRevision(revision).build();
        SrvKeyspaceListener listener = new SrvKeyspaceListener(cell, keyspace);
        MapUtil.computeIfAbsent(WATCHER_MAP, nodePath, watch -> client.getWatchClient().watch(buildByteSequenceKey(nodePath), watchOption, listener));
    }

    private ByteSequence buildByteSequenceKey(String key) {
        return ByteSequence.from(key, StandardCharsets.UTF_8);
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
