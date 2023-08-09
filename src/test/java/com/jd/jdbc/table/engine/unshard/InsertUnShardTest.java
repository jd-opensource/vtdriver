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

package com.jd.jdbc.table.engine.unshard;

import com.jd.jdbc.table.engine.InsertTest;
import java.sql.SQLException;
import testsuite.internal.TestSuiteShardSpec;

public class InsertUnShardTest extends InsertTest {

    @Override
    protected String getUrl() {
        return getConnectionUrl(Driver.of(TestSuiteShardSpec.NO_SHARDS)) + "&useAffectedRows=false";
    }

    @Override
    protected String getUser() {
        return getUser(Driver.of(TestSuiteShardSpec.NO_SHARDS));
    }

    @Override
    protected String getPassword() {
        return getPassword(Driver.of(TestSuiteShardSpec.NO_SHARDS));
    }

    @Override
    protected void insert() throws SQLException, NoSuchFieldException, IllegalAccessException {
        insert(false, false);
    }
}
