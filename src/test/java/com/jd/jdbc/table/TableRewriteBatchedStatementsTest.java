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

package com.jd.jdbc.table;

import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.Test;
import testsuite.internal.TestSuiteShardSpec;

public class TableRewriteBatchedStatementsTest extends TableAutoGeneratedKeysTest {

    @Override
    protected void getConn() throws SQLException {
        String url = getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS)) + "&rewriteBatchedStatements=true";
        conn = DriverManager.getConnection(url);
    }

    @Test
    @Override
    public void test11StatementExecuteBatch() throws Exception {
        thrown.expect(java.sql.SQLFeatureNotSupportedException.class);
        thrown.expectMessage("unsupported multiquery");
        super.test11StatementExecuteBatch();
    }

    @Test
    @Override
    public void test12StatementExecuteBatch() throws Exception {
        thrown.expect(java.sql.SQLFeatureNotSupportedException.class);
        thrown.expectMessage("unsupported multiquery");
        super.test12StatementExecuteBatch();
    }

    @Test
    @Override
    public void test20PreparedStatementExecuteBatch() throws Exception {
        thrown.expect(java.sql.SQLFeatureNotSupportedException.class);
        thrown.expectMessage("unsupported multiquery");
        super.test20PreparedStatementExecuteBatch();
    }

    @Test
    @Override
    public void test21PreparedStatementExecuteBatch() throws Exception {
        thrown.expect(java.sql.SQLFeatureNotSupportedException.class);
        thrown.expectMessage("unsupported multiquery");
        super.test21PreparedStatementExecuteBatch();
    }

    @Test
    @Override
    public void test22PreparedStatementExecuteBatch() throws Exception {
        thrown.expect(java.sql.SQLFeatureNotSupportedException.class);
        thrown.expectMessage("unsupported multiquery");
        super.test22PreparedStatementExecuteBatch();
    }

    @Test
    @Override
    public void test23SetNull() throws Exception {
        super.test23SetNull();
    }
}
