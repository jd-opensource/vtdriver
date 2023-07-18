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

package com.jd.jdbc.key;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class BinaryHashUtilTest extends TestSuite {

    @Test
    public void getShardByVindex() throws SQLException {
        String hashvalue = BinaryHashUtil.getShardByVindex(ShardEnum.TWO_SHARDS, "hashvalue");
        System.out.println(hashvalue);// -80
    }

    @Test
    public void getShardByVindexByKeyspace() throws SQLException {
        Connection connection = DriverManager.getConnection(getConnectionUrl(Driver.of(TestSuiteShardSpec.TWO_SHARDS)));
        String hashvalue = BinaryHashUtil.getShardByVindex(connection.getCatalog(), "hashvalue");
        System.out.println(hashvalue);// -80
    }
}