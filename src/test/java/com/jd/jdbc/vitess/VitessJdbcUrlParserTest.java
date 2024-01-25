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

package com.jd.jdbc.vitess;

import com.jd.jdbc.discovery.TopologyWatcherManager;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import testsuite.TestSuite;
import testsuite.internal.environment.TestSuiteEnv;

import static com.jd.jdbc.vitess.VitessJdbcUrlParser.JDBC_VITESS_PREFIX;
import static testsuite.internal.TestSuiteShardSpec.TWO_SHARDS;

public class VitessJdbcUrlParserTest extends TestSuite {
    private TestSuiteEnv env = Driver.of(TWO_SHARDS);

    private String schema = getKeyspace(env);

    private String role = "rw";

    private String user = getUser(env);

    private String password = getPassword(env);


    private Integer socketTimeout;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private VitessConnection conn;

    private void init() throws SQLException {
        TopologyWatcherManager.INSTANCE.resetScheduledExecutor();

        String connecturlionUrl = getConnectionUrl(env);

        URI uri = null;
        try {
            uri = new URI(connecturlionUrl.substring(JDBC_VITESS_PREFIX.length()));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
        String path = uri.getPath();
        connecturlionUrl = connecturlionUrl.replace(path, "/" + schema);
        String urlUser = "";
        String urlPasswd = "";
        String urlRole = "";
        String parameters = uri.getQuery();
        parameters = StringUtils.replaceEach(parameters, new String[] {":", "&nbsp;"}, new String[] {"", " "});
        String[] parameterPairs = parameters.split("&");
        for (String parameterPair : parameterPairs) {
            String[] pair = parameterPair.trim().split("=");
            if (pair.length == 2) {
                if (pair[0].equals("user")) {
                    urlUser = pair[1];
                } else if (pair[0].equals("password")) {
                    urlPasswd = pair[1];
                } else if (pair[0].equals("role")) {
                    urlRole = pair[1];
                }
            }
        }
        connecturlionUrl = connecturlionUrl.replace(urlUser, user).replace(urlPasswd, password);
        if (!urlRole.equals("")) {
            connecturlionUrl = connecturlionUrl.replace(urlRole, role);
        } else {
            connecturlionUrl += "&role=" + role;
        }

        if (socketTimeout != null) {
            connecturlionUrl += "&socketTimeout=" + socketTimeout;
        }
        this.conn = (VitessConnection) DriverManager.getConnection(connecturlionUrl);

        try (Statement stmt = this.conn.createStatement()) {
            stmt.executeUpdate("delete from plan_test");
        }
    }

    @Test
    public void testUserParam() throws SQLException {
        user = "";
        thrown.expect(SQLException.class);
        thrown.expectMessage("no user or password: '/" + schema + "'");
        init();
    }

    @Test
    public void testPasswdParam() throws SQLException {
        password = "";
        thrown.expect(SQLException.class);
        thrown.expectMessage("no user or password: '/" + schema + "'");
        init();
    }

    @Test
    public void testSchemaParam() throws SQLException {
        schema = "";
        thrown.expect(SQLException.class);
        thrown.expectMessage(" database name can not null");
        init();
    }

    @Test
    public void testRoleParam() throws SQLException {
        role = "aa";
        thrown.expect(SQLException.class);
        thrown.expectMessage("'role=" + role + "' " + "error in jdbc url");
        init();
    }

    @Test
    public void testSocketTimeout() throws SQLException {
        socketTimeout = -1;
        init();
        Integer getSocketTimeout = Integer.valueOf(conn.getProperties().getProperty("socketTimeout"));
        if (!getSocketTimeout.equals(1000)) {
            Assert.fail("testSocketTimeout is fail");
        }
        socketTimeout = 0;
        init();
        getSocketTimeout = Integer.valueOf(conn.getProperties().getProperty("socketTimeout"));
        if (!getSocketTimeout.equals(socketTimeout)) {
            Assert.fail("testSocketTimeout is fail");
        }
        socketTimeout = 500;
        init();
        getSocketTimeout = Integer.valueOf(conn.getProperties().getProperty("socketTimeout"));
        if (!getSocketTimeout.equals(socketTimeout)) {
            Assert.fail("testSocketTimeout is fail");
        }
    }
}
