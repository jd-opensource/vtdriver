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

package com.jd.jdbc.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.jd.jdbc.util.JsonUtil;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import lombok.Data;
import org.junit.Assert;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class VtApiServerTest extends TestSuite {
    public static String execCurl(String[] cmds) {
        ProcessBuilder process = new ProcessBuilder(cmds);
        Process p;
        try {
            p = process.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line);
                builder.append(System.getProperty("line.separator"));
            }
            return builder.toString();

        } catch (IOException e) {
            e.printStackTrace();
            Assert.assertNull(e);
        }
        return null;
    }

    @Test
    public void case01() throws SQLException {
        try (Connection conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS))) {
            Assert.assertNotNull(conn);
            String response = execCurl(new String[] {"curl", "-XGET", "http://127.0.0.1:15002"
                + VtApiServer.uniquePrefix + "/status"});
            StatusResponse statusResponse = JsonUtil.parseObject(response, StatusResponse.class);

            Assert.assertNotNull(statusResponse);
            Assert.assertEquals(statusResponse.status, "OK");
            Assert.assertFalse(statusResponse.apis.isEmpty());
        }
    }

    @Test
    public void case02() throws SQLException {
        try (Connection conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS))) {
            Assert.assertNotNull(conn);
            String response = execCurl(new String[] {"curl", "-XGET", "http://127.0.0.1:15002"
                + VtApiServer.uniquePrefix + "/vschema/refresh"});

            Assert.assertNotNull(response);
            Assert.assertEquals(VtApiServerResponse.SUCCESS.getMessage(), response.trim());
        }
    }

    //	@Test
    public void case03() throws SQLException {
        int port = 15006;
        System.setProperty("API_SERVER_PORT", String.valueOf(port));
        try (Connection conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS))) {
            Assert.assertNotNull(conn);
            String response = execCurl(new String[] {"curl", "-XGET", "http://127.0.0.1:" + 15006 + VtApiServer.uniquePrefix + "/status"});
            StatusResponse statusResponse = JsonUtil.parseObject(response, StatusResponse.class);

            Assert.assertNotNull(statusResponse);
            Assert.assertEquals(statusResponse.status, "OK");
            Assert.assertFalse(statusResponse.apis.isEmpty());
        }
    }

    @Data
    protected static class StatusResponse {
        @JsonProperty("Status")
        protected String status;

        @JsonProperty("Path Prefix")
        protected String pathPrefix;

        @JsonProperty("APIs")
        protected List<VtApi> apis;
    }
}
