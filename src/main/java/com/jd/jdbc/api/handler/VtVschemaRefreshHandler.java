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

import com.jd.jdbc.VSchemaManager;
import com.jd.jdbc.api.VtHttpHandler;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.topo.TopoException;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;

public class VtVschemaRefreshHandler extends VtHttpHandler {
    private static final Log LOGGER = LogFactory.getLog(VtVschemaRefreshHandler.class);

    private final VSchemaManager vSchemaManager;

    public VtVschemaRefreshHandler(VSchemaManager vSchemaManager) {
        this.vSchemaManager = vSchemaManager;
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        try {
            this.vSchemaManager.refreshVschema();
            LOGGER.info("Refresh vschema success!");
            super.success(httpExchange);
        } catch (TopoException e) {
            LOGGER.error("Refresh vschema failure!", e);
            super.failure(httpExchange, e.getMessage());
        }
    }
}
