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
import com.jd.jdbc.api.VtApiServer;
import com.jd.jdbc.api.VtApiStatusResponse;
import com.jd.jdbc.api.VtHttpHandler;
import com.jd.jdbc.util.NetUtil;
import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.util.Map;

public class VtStatusHandler extends VtHttpHandler {
    private final String rootPrefix;

    private final Map<String, VSchemaManager> vSchemaManagerMap;

    public VtStatusHandler(String rootPrefix, Map<String, VSchemaManager> vSchemaManagerMap) {
        this.rootPrefix = rootPrefix;
        this.vSchemaManagerMap = vSchemaManagerMap;
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        String pathPrefix = "http://" + NetUtil.getLocalAdder() + ":" + VtApiServer.port + rootPrefix;
        String keyspaces = String.join(", ", vSchemaManagerMap.keySet());
        VtApiStatusResponse vtApiStatusResponse = new VtApiStatusResponse("OK", pathPrefix + "/status", pathPrefix + "/vschema/refresh", keyspaces);
        super.success(httpExchange, vtApiStatusResponse.toString());
    }
}
