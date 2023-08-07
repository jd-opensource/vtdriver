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

package com.jd.jdbc.srvtopo;

import com.jd.jdbc.context.IContext;
import com.jd.jdbc.topo.TopoServer;
import java.util.function.Function;
import vschema.Vschema;

public interface SrvTopoServer {
    /**
     * @return
     * @throws SrvTopoException
     */
    TopoServer getTopoServer();

    /**
     * @param ctx
     * @param cell
     * @param keyspace
     * @return
     */
    ResilientServer.GetSrvKeyspaceResponse getSrvKeyspace(IContext ctx, String cell, String keyspace);

    /**
     * @param ctx
     * @param cell
     * @param callback
     * @throws SrvTopoException
     */
    void watchSrvVschema(IContext ctx, String cell, Function<Vschema.SrvVSchema, Void> callback) throws SrvTopoException;
}
