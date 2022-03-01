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

package com.jd.jdbc.table.engine;

import com.jd.jdbc.table.TableTestUtil;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.testcase.TestSuiteCase;

public class DeleteTest extends TestSuite {
    protected Connection conn;

    protected List<DeleteTest.TestCase> testCaseList;

    @Before
    public void init() throws SQLException, IOException {
        getConn();
        testCaseList = initCase("src/test/resources/engine/tableengine/delete_case.json", DeleteTest.TestCase.class, conn.getCatalog());
        testCaseList.addAll(initCase("src/test/resources/engine/tableengine/delete_case_upperCase.json", DeleteTest.TestCase.class, conn.getCatalog()));
    }

    protected void getConn() throws SQLException {
        conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
    }

    @After
    public void clean() throws Exception {
        closeConnection(conn);
        TableTestUtil.setDefaultTableConfig();
    }

    @Test
    public void test01() throws Exception {
        // 分片分表键一致
        TableTestUtil.setSplitTableConfig("engine/tableengine/split-table_1.yml");
        delete();
    }

    @Test
    public void test02() throws Exception {
        // 分片分表键不一致
        TableTestUtil.setSplitTableConfig("engine/tableengine/split-table_2.yml");
        delete();
    }

    public void delete() throws SQLException, NoSuchFieldException, IllegalAccessException {
        int count = 0;
        for (DeleteTest.TestCase testCase : testCaseList) {
            count++;
            printInfo("\nNo." + count + " - From File:   " + testCase.getFile());
            printComment("comment: " + testCase.getComment());
            try (Statement stmt = conn.createStatement()) {
                printNormal("initSql:");
                for (String initSql : testCase.getInitSql()) {
                    printNormal("\t" + initSql);
                    stmt.execute(initSql);
                }

                printNormal("query: \n\t" + testCase.getQuery());
                if (testCase.getException() != null) {
                    printNormal("expected exception: \n\t" + testCase.getException());
                }

                int deleteCount;
                try {
                    if (testCase.getDeleteVar().length > 0) {
                        try (PreparedStatement pstmt = conn.prepareStatement(testCase.getQuery())) {
                            for (int i = 1; i <= testCase.getDeleteVar().length; i++) {
                                pstmt.setObject(i, testCase.getDeleteVar()[i - 1]);
                            }
                            deleteCount = pstmt.executeUpdate();
                        }
                    } else {
                        deleteCount = stmt.executeUpdate(testCase.getQuery());
                    }
                } catch (Exception e) {
                    Assert.assertEquals(printFail("exception: " + testCase.getException()), testCase.getException(), e.getClass().getName());
                    Assert.assertTrue(printFail("wrong errorMessage,error message: " + e.getMessage()), e.getMessage().contains(testCase.getErrorMessage()));
                    printOk();
                    continue;
                }
                Assert.assertTrue(printFail("no exception thrown: " + testCase.getException()), testCase.getException() == null || "".equals(testCase.getException()));

                printNormal("verifySql: \n\t" + testCase.getVerifySql());
                printNormal("expected deleteCount: " + testCase.getDeleteCount());
                printNormal("actual deleteCount: " + deleteCount);

                Assert.assertEquals(testCase.getDeleteCount().intValue(), deleteCount);

                ResultSet rs = stmt.executeQuery(testCase.getVerifySql());
                Assert.assertTrue(rs.next());
                Assert.assertEquals(printFail("[FAIL] sql:" + testCase.getQuery()), testCase.getVerifyResult().intValue(), rs.getInt(1));
                Assert.assertFalse(rs.next());
                printOk();
            }
        }
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected static class TestCase extends TestSuiteCase {
        private String[] initSql;

        private Object[] deleteVar;

        private Integer deleteCount;

        private String verifySql;

        private Integer verifyResult;

        private String exception;

        private String errorMessage;
    }

}
