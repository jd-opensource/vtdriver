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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class KeyspaceNotExistTest extends TestSuite {
    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Test
    public void case05() throws SQLException {
        String keyspace = getKeyspace(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        String connectionUrl = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        String keyspaceNotExist = keyspace + "not_exist";
        connectionUrl = connectionUrl.replaceAll(keyspace, keyspaceNotExist);
        thrown.expect(SQLSyntaxErrorException.class);
        thrown.expectMessage("Unknown database '" + keyspaceNotExist + "'");
        try (Connection connection = DriverManager.getConnection(connectionUrl);
             Statement stmt = connection.createStatement();) {
            stmt.executeQuery("select 1");
        }
    }
}
