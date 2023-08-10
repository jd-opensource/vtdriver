/*
Copyright 2021 JD Project Authors.

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

package com.jd.jdbc.api.handler;

import com.jd.jdbc.Executor;
import com.jd.jdbc.VSchemaManager;
import com.jd.jdbc.api.VtHttpHandler;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.topo.TopoException;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

public class VtVschemaRefreshHandler extends VtHttpHandler {
    private static final Log LOGGER = LogFactory.getLog(VtVschemaRefreshHandler.class);

    private final Map<String, VSchemaManager> vSchemaManagerMap;

    public VtVschemaRefreshHandler(Map<String, VSchemaManager> vSchemaManagerMap) {
        this.vSchemaManagerMap = vSchemaManagerMap;
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        try {
            Map<String, String> params = queryToMap(httpExchange.getRequestURI().getQuery());
            boolean refreshFlag = false;
            if (!params.containsKey("target")) {
                super.success(httpExchange, "Refresh vschema no target");
                return;
            }
            StringBuilder resMessage = new StringBuilder();
            String value = params.get("target");
            if (value.equals("all")) {
                refreshFlag = true;
                for (VSchemaManager vsm : new HashSet<>(vSchemaManagerMap.values())) {
                    vsm.refreshVschema();
                }
                resMessage.append("Refresh vschema for all keyspace success");
            } else {
                String[] keyspaces = value.split(",");
                resMessage.append("Refresh vschema: ");
                for (String ks : keyspaces) {
                    if (vSchemaManagerMap.containsKey(ks)) {
                        vSchemaManagerMap.get(ks).refreshVschema(ks);
                        refreshFlag = true;
                        resMessage.append(ks).append(" success.");
                    } else {
                        resMessage.append(ks).append(" not found.");
                    }
                }
            }
            if (Executor.getInstanceNoInit() != null && refreshFlag) {
                Executor.getInstanceNoInit().getPlans().clear();
            }
            super.success(httpExchange, resMessage.toString());
        } catch (TopoException e) {
            LOGGER.error("Refresh vschema failure", e);
            super.failure(httpExchange, e.getMessage());
        }
    }
}
