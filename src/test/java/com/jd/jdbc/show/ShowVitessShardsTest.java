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

package com.jd.jdbc.show;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.TestSuiteShardSpec;

public class ShowVitessShardsTest extends TestSuite {

    private final String showNoShard = "show vitess_shards";

    private final String[] vitessNoShard = {"0"};

    private final String[] vitess2shards = {"-80", "80-"};

    private List<TestCase> testCases2shard;

    private Connection singleShardsConn;

    private Connection v2shardsConn;

    @Before
    public void init() throws SQLException {
        initConnection();
    }

    public void initConnection() throws SQLException {
        this.singleShardsConn = getConnection(Driver.of(TestSuiteShardSpec.NO_SHARDS));
        this.v2shardsConn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
    }

    public void initData() {
        testCases2shard = new ArrayList<>();
        testCases2shard.add(new TestCase("SHOW VITESS_SHARDS", "========== 2 shards upper case shards test =========="));
        testCases2shard.add(new TestCase("SHOW VITESS_SHARDS;", "========== 2 shards upper case end with \";\" shards test =========="));
        testCases2shard.add(new TestCase("/*COMMENT*/SHOW VITESS_SHARDS", "========== 2 shards upper case start with \"COMMENT\"  shards test =========="));
        testCases2shard.add(new TestCase("/*COMMENT*/SHOW VITESS_SHARDS;", "========== 2 shards upper case start with \"COMMENT\" and end with \";\" shards test =========="));
        testCases2shard.add(new TestCase("show vitess_shards", "========== 2 shards lower case shards test =========="));
        testCases2shard.add(new TestCase("show vitess_shards;", "========== 2 shards lower case end with \";\" shards test =========="));
        testCases2shard.add(new TestCase("/* comment*/ show vitess_shards", "========== 2 shards lower case start with \"comment\"  shards test =========="));
        testCases2shard.add(new TestCase("/* comment*/ show vitess_shards;", "========== 2 shards lower case start with \"comment\" and end with \";\" shards test =========="));
    }

    private void assertResurt(ResultSet rs, String[] expectRs) throws SQLException {
        for (String shard : expectRs) {
            rs.next();
            String keyspace = rs.getString(1);
            String shardExpr = rs.getString(2);
            System.out.println("keyspace:" + keyspace + "; shards:" + shardExpr);
            Assert.assertEquals(shard, shardExpr);
        }
    }

    private void stmtExcute(Connection conn, List<TestCase> testCaseList, String[] expectRs) throws SQLException {
        ResultSet rs;
        try (Statement stmt = conn.createStatement()) {
            for (TestCase testCase : testCaseList) {
                printInfo(testCase.getComment());
                printInfo(testCase.getQuery());
                rs = stmt.executeQuery(testCase.getQuery());
                assertResurt(rs, expectRs);
                boolean hasRs = stmt.execute(testCase.getQuery());
                if (!hasRs) {
                    Assert.fail();
                }
                rs = stmt.getResultSet();
                assertResurt(rs, expectRs);
            }
        }
    }

    private void preStmtExcute(Connection conn, List<TestCase> testCaseList, String[] expectRs) throws SQLException {
        ResultSet rs;
        for (TestCase testCase : testCaseList) {
            try (PreparedStatement stmt = conn.prepareStatement(testCase.getQuery())) {
                printInfo(testCase.getComment());
                printInfo(testCase.getQuery());
                rs = stmt.executeQuery();
                assertResurt(rs, expectRs);
                boolean hasRs = stmt.execute();
                if (!hasRs) {
                    Assert.fail();
                }
                rs = stmt.getResultSet();
                assertResurt(rs, expectRs);
            }
        }
    }

    @Test
    public void test1() throws SQLException {
        initData();
        System.out.println("========== Statement Execute Test ==========");
        stmtExcute(v2shardsConn, testCases2shard, vitess2shards);

        // test prepared execute
        System.out.println("========== Prepared Statement Execute Test ==========");
        preStmtExcute(v2shardsConn, testCases2shard, vitess2shards);
    }

    @Test
    public void testStatementShowNoShard() throws SQLException {
        try (Statement stmt = this.singleShardsConn.createStatement()) {
            System.out.println("========== No Shard Test ==========");
            ResultSet rs = stmt.executeQuery(showNoShard);
            for (String shard : vitessNoShard) {
                rs.next();
                String keyspace = rs.getString(1);
                String shardExpr = rs.getString(2);
                System.out.println("keyspace:" + keyspace + "; shards:" + shardExpr);
                Assert.assertEquals(shard, shardExpr);
            }
        }
    }

    @After
    public void close() throws SQLException {
        if (this.v2shardsConn != null) {
            this.v2shardsConn.close();
        }
    }

    @Data
    class TestCase {

        private String query;

        private String comment;

        public TestCase(String query, String comment) {
            this.query = query;
            this.comment = comment;
        }
    }
}