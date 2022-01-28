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

package com.jd.jdbc.table.unshard;

import com.jd.jdbc.table.TransactionTest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import testsuite.internal.TestSuiteShardSpec;

public class TransactionUnShardTest extends TransactionTest {

    @Override
    protected void getConn() throws SQLException {
        baseUrl = getConnectionUrl(Driver.of(TestSuiteShardSpec.NO_SHARDS));
        Connection conn_0 = DriverManager.getConnection(baseUrl + "&queryParallelNum=0");
        Connection conn_1 = DriverManager.getConnection(baseUrl + "&queryParallelNum=1");
        Connection conn_8 = DriverManager.getConnection(baseUrl + "&queryParallelNum=2");
        this.connectionList = new ArrayList<>();
        this.connectionList.add(conn_0);
        this.connectionList.add(conn_1);
        this.connectionList.add(conn_8);
    }
}
