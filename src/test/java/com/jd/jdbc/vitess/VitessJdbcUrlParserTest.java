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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import testsuite.TestSuite;
import testsuite.internal.environment.TestSuiteEnv;

import static com.jd.jdbc.vitess.VitessJdbcUrlParser.JDBC_VITESS_PREFIX;
import static testsuite.internal.TestSuiteShardSpec.TWO_SHARDS;

public class VitessJdbcUrlParserTest extends TestSuite {
    TestSuiteEnv env = Driver.of(TWO_SHARDS);

    String schema = getKeyspace(env);

    String role = "rw";

    String user = getUser(env);

    String password = getPassword(env);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private Connection conn;

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

        this.conn = DriverManager.getConnection(connecturlionUrl);
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
}
