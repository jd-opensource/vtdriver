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

package com.jd.jdbc.topo;

import com.jd.jdbc.context.IContext;
import io.vitess.proto.Topodata;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface TopoTablet {

    /**
     * @param ctx
     * @param tabletAlias
     * @return
     * @throws TopoException
     */
    TopoTabletInfo getTablet(IContext ctx, Topodata.TabletAlias tabletAlias) throws TopoException;

    CompletableFuture<TopoTabletInfo> getTabletFuture(IContext ctx, Topodata.TabletAlias tabletAlias) throws TopoException;

    /**
     * @param ctx
     * @param cell
     * @return
     * @throws TopoException
     */
    List<Topodata.TabletAlias> getTabletsByCell(IContext ctx, String cell) throws TopoException;

    CompletableFuture<List<Topodata.TabletAlias>> getTabletsByCellFuture(IContext ctx, String cell) throws TopoException;
}
