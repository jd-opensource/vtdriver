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

import com.jd.jdbc.topo.TopoConnection;
import com.jd.jdbc.topo.TopoException;
import com.jd.jdbc.topo.TopoFactory;
import com.jd.jdbc.util.threadpool.impl.TabletNettyExecutorService;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.Util;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

public class Etcd2TopoFactory implements TopoFactory {

    private String clientCertPath = "";

    private String clientKeyPath = "";

    private String serverCaPath = "";

    /**
     * @param name
     * @param id
     * @return
     */
    @Override
    public Boolean hasGlobalReadOnlyCell(String name, String id) {
        return false;
    }

    /**
     * @param cell
     * @param serverAddr
     * @param root
     * @return
     * @throws TopoException
     */
    @Override
    public TopoConnection create(String cell, String serverAddr, String root) throws TopoException {
        return newServer(serverAddr, root);
    }

    /**
     * @param serverAddr
     * @param root
     * @return
     * @throws TopoException
     */
    private Etcd2TopoServer newServer(String serverAddr, String root) throws TopoException {
        return newServerWithOpts(serverAddr, root, clientCertPath, clientKeyPath, serverCaPath);
    }

    /**
     * @param serverAddr
     * @param root
     * @param certPath
     * @param keyPath
     * @param caPath
     * @return
     * @throws TopoException
     */
    private Etcd2TopoServer newServerWithOpts(String serverAddr, String root, String certPath, String keyPath, String caPath) throws TopoException {
        String[] serverAddrs = serverAddr.split(",");
        String[] result = new String[serverAddrs.length];
        for (int i = 0; i < serverAddrs.length; i++) {
            if (!serverAddrs[i].startsWith("http://")) {
                result[i] = String.format("http://%s", serverAddrs[i]);
            } else {
                result[i] = serverAddrs[i];
            }
        }
        List<URI> endpoints = Util.toURIs(Arrays.asList(result));
        Client client = Client.builder().endpoints(endpoints)
            .keepaliveTimeout(Duration.ofSeconds(10L))
            .keepaliveTime(Duration.ofSeconds(10L))
            .keepaliveWithoutCalls(true)
            .executorService(TabletNettyExecutorService.getNettyExecutorService()).build();

        Etcd2TopoServer etcd2TopoServer = new Etcd2TopoServer();
        etcd2TopoServer.setClient(client);
        etcd2TopoServer.setRoot(root);
        return etcd2TopoServer;
    }

    public void setClientCertPath(String clientCertPath) {
        this.clientCertPath = clientCertPath;
    }

    public void setClientKeyPath(String clientKeyPath) {
        this.clientKeyPath = clientKeyPath;
    }

    public void setServerCaPath(String serverCaPath) {
        this.serverCaPath = serverCaPath;
    }
}
