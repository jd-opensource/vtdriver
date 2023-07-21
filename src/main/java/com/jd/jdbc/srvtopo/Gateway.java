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
import com.jd.jdbc.queryservice.IQueryService;
import io.vitess.proto.Topodata;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

/**
 * A Gateway is the query processing module for each shard,
 * which is used by ScatterConn.
 */
public abstract class Gateway {

    /**
     * the query service that this Gateway wraps around
     */
    @Getter
    @Setter
    private IQueryService queryService;

    /**
     * QueryServiceByAlias returns a QueryService
     *
     * @param alias
     * @return
     */
    public abstract IQueryService queryServiceByAlias(Topodata.TabletAlias alias);

    /**
     * WaitForTablets asks the gateway to wait for the provided
     * tablets types to be available. It the context is canceled
     * before the end, it should return ctx.Err().
     * The error returned will have specific effects:
     * - nil: keep going with startup.
     * - context.DeadlineExceeded: log a warning that we didn't get
     * all tablets, and keep going with startup.
     * - any other error: log.Fatalf out.
     *
     * @param ctx
     * @param cell
     * @param keyspace
     * @param tabletTypeList
     * @throws Exception
     */
    public abstract void waitForTablets(IContext ctx, String cell, String keyspace, List<Topodata.TabletType> tabletTypeList) throws Exception;
}
