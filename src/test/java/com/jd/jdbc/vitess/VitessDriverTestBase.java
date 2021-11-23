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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import lombok.Data;
import testsuite.TestSuite;

public class VitessDriverTestBase extends TestSuite {

    public TestResultSet execute(PreparedStatement stmt) throws SQLException {
        TestResultSet testRet = new TestResultSet();

        boolean ret = stmt.execute();
        do {
            if (ret) {
                ResultSet result = stmt.getResultSet();
                //while (result.next()) {
                testRet.ret = result;
                return testRet;
                //}
            } else {
                int count = stmt.getUpdateCount();
                if (count == -1) {
                    break;
                }
                testRet.affectRows = count;
            }
            ret = stmt.getMoreResults();
        } while (true);

        return testRet;
    }

    @Data
    public class TestResultSet {
        public ResultSet ret;

        public int affectRows;
    }

}
