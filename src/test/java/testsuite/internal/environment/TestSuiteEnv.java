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

package testsuite.internal.environment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import lombok.Setter;
import testsuite.internal.TestSuiteConn;
import testsuite.internal.TestSuiteShardSpec;

public abstract class TestSuiteEnv implements TestSuiteConn {
    protected TestSuiteShardSpec shardSpec;

    @Setter
    protected String encoding;

    public TestSuiteEnv(TestSuiteShardSpec shardSpec) {
        this.shardSpec = shardSpec;
    }

    @Override
    public Connection getConnection(String url) throws SQLException {
        try {
            return DriverManager.getConnection(url);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
