/*
Copyright 2021 JD Project Authors. Licensed under Apache-2.0.

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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class TimezoneConfigureTest extends TestSuite {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testServerTimezone() throws SQLException {
        String url = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        Connection conn = DriverManager.getConnection(url);
        conn.close();
    }

    @Test
    public void testNoServerTimezone() throws SQLException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("serverTimezone is not found in jdbc url");
        String url = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        url = url.replaceAll("&serverTimezone=[^&]*", "");
        Connection conn = DriverManager.getConnection(url);
        conn.close();
    }

    @Test
    public void testParameterNoValue() throws SQLException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("serverTimezone is not found in jdbc url");
        String url = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        url = url.replaceAll("&serverTimezone=[^&]*", "&serverTimezone=");
        Connection conn = DriverManager.getConnection(url);
        conn.close();
    }

    @Test
    public void testInvalidServerTimezone() throws SQLException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("invalid serverTimezone in jdbc url");
        String url = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        url = url.replaceAll("&serverTimezone=[^&]*", "&serverTimezone=ST");
        Connection conn = DriverManager.getConnection(url);
        conn.close();
    }
}
