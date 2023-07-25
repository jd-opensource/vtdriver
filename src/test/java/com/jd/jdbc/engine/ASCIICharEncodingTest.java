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

package com.jd.jdbc.engine;

import java.sql.Connection;
import java.sql.SQLException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.environment.DriverEnv;

/*
-Dfile.encoding=US-ASCII
Preferences -> Editor -> File Encodings -> Project Encoding
 */
public class ASCIICharEncodingTest extends TestSuite {
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private Connection conn;

    @Test
    public void testAsciiUtf8() throws SQLException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Only supports utf8 encoding, please check characterEncoding in jdbcurl and file.encoding in environment variable,characterEncoding = UTF-8, file.encoding=US-ASCII");
        init("UTF-8");
    }

    @Test
    public void testAsciiGbk() throws SQLException {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Only supports utf8 encoding, please check characterEncoding in jdbcurl and file.encoding in environment variable,characterEncoding = GBK, file.encoding=US-ASCII");
        init("GBK");
    }

    public void init(String charEncoding) throws SQLException {
        DriverEnv driverEnv = TestSuite.Driver.of(TestSuiteShardSpec.TWO_SHARDS, charEncoding);
        this.conn = getConnection(driverEnv);
    }
}
