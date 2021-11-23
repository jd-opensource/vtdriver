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

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.testcase.TestSuiteCase;

public class SelectEngineTest extends SelectTestSuite {

    protected static List<TestCase> testCaseList;

    @BeforeClass
    public static void init() throws SQLException, IOException {
        conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        testCaseList = new ArrayList<>();
    }

    public static void loadData() throws IOException, SQLException {
        testCaseList.clear();
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/selectengine/aggr_cases.json", TestCase.class));
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/selectengine/filter_cases.json", TestCase.class));
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/selectengine/calculate_dual_case.json", TestCase.class));
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/selectengine/postprocess_cases.json", TestCase.class));
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/selectengine/from_cases.json", TestCase.class));
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/selectengine/memory_sort_cases.json", TestCase.class));
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/selectengine/select_cases.json", TestCase.class));
        replaceKeyspace(testCaseList);
    }

    public static void loadDeepPaginationData() throws IOException, SQLException {
        testCaseList.clear();
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/selectengine/deepPagination.json", TestCase.class));
        replaceKeyspace(testCaseList);
    }

    protected static void replaceKeyspace(List<TestCase> caseList) throws SQLException {
        String defaultks = conn.getCatalog();

        for (TestCase testCase : caseList) {
            String query = testCase.getQuery().replaceAll(":ks", defaultks);
            testCase.setQuery(query);
        }
    }

    @AfterClass
    public static void close() throws SQLException {
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    public void selectTest() throws SQLException, NoSuchFieldException, IllegalAccessException, IOException, NoSuchMethodException {
        loadData();
        execute(testCaseList, false);
    }

    @Test
    public void streamSelectTest() throws SQLException, NoSuchFieldException, IllegalAccessException, IOException, NoSuchMethodException {
        loadData();
        execute(testCaseList, true);
    }

    @Test
    public void deepPaginationSelectTest() throws SQLException, NoSuchFieldException, IllegalAccessException, IOException, NoSuchMethodException {
        loadDeepPaginationData();
        execute(testCaseList, false);
    }

    @Test
    public void deepPaginationStreamSelectTest() throws SQLException, NoSuchFieldException, IllegalAccessException, IOException, NoSuchMethodException {
        loadDeepPaginationData();
        execute(testCaseList, true);
    }

    @Override
    protected List<ResultRow> rsToResultList(ResultSet rs, Object object) throws SQLException, NoSuchFieldException, IllegalAccessException {
        TestCase testCase = (TestCase) object;
        List<ResultRow> queryResult = new ArrayList<>();
        while (rs.next()) {
            ResultRow row = new ResultRow();
            for (TestSuiteCase.Field field : testCase.getFields()) {
                String fieldName = field.getName();
                String colName = "".equals(field.getAlias()) ? fieldName : field.getAlias();
                Integer colId = field.getColumnIndex();
                if (colName.equals("f_bit")) {
                    row.getClass().getField(fieldName).set(row, colId == null ? rs.getBoolean(colName) : rs.getBoolean(colId));
                } else {
                    row.getClass().getField(fieldName).set(row, colId == null ? rs.getObject(colName) : rs.getObject(colId));
                }
            }
            queryResult.add(row);
        }
        return queryResult;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected static class TestCase extends SelectTestSuite.TestCase {
        private String[] initSql;

        private ResultRow[] verifyResult;

        private String exception;

        private String errorMessage;
    }

    @Data
    public static class ResultRow extends SelectTestSuite.ResultRow implements Comparable<ResultRow> {
        public Long f_tinyint;

        public Integer f_utinyint;

        public Integer f_smallint;

        public Integer f_usmallint;

        public Integer f_midint;

        public Integer f_umidint;

        public Integer f_int;

        public Long f_uint;

        public Long f_bigint;

        public BigDecimal f_ubigint;

        public Float f_float;

        public Double f_double;

        public BigDecimal f_decimal;

        public Date f_date;

        public Time f_time;

        public Timestamp f_datetime;

        public Timestamp f_timestamp;

        public Boolean f_bit; // bit(64)

        public String f_varchar;

        public String f_text;

        public String table_schema;

        public String host;

        public String mysql_version;

        public Date password_last_changed;

        public String plugin;

        public Long count;

        public String trim;

        public BigDecimal sum;

        public Integer min;

        public Integer max;

        @Override
        public int compareTo(ResultRow o) {
            if (this.hashCode() < o.hashCode()) {
                return -1;
            } else if (this.hashCode() > o.hashCode()) {
                return 1;
            }
            return 0;
        }

        @SneakyThrows
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            Field[] fields = this.getClass().getDeclaredFields();
            for (Field field : fields) {
                field.setAccessible(true);
                Object res = field.get(this);
                if (res != null) {
                    sb.append(field.getName())
                        .append(":")
                        .append(res)
                        .append(" ");
                }
            }
            return sb.toString();
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects
                .hashCode(f_tinyint, f_utinyint, f_smallint, f_midint, f_umidint,
                    f_int, f_uint, f_bigint, f_ubigint, f_float, f_double, f_decimal,
                    f_date, f_time, f_datetime, f_timestamp, f_bit, f_varchar, f_text,
                    count, trim, sum, min, max);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ResultRow)) {
                return false;
            }

            ResultRow o = (ResultRow) other;
            return java.util.Objects.equals(f_tinyint, o.f_tinyint) &&
                java.util.Objects.equals(f_utinyint, o.f_utinyint) &&
                java.util.Objects.equals(f_smallint, o.f_smallint) &&
                java.util.Objects.equals(f_midint, o.f_midint) &&
                java.util.Objects.equals(f_umidint, o.f_umidint) &&
                java.util.Objects.equals(f_int, o.f_int) &&
                java.util.Objects.equals(f_uint, o.f_uint) &&
                java.util.Objects.equals(f_bigint, o.f_bigint) &&
                java.util.Objects.equals(f_ubigint, o.f_ubigint) &&
                java.util.Objects.equals(f_float, o.f_float) &&
                java.util.Objects.equals(f_double, o.f_double) &&
                java.util.Objects.equals(f_decimal, o.f_decimal) &&
                java.util.Objects.equals(f_date, o.f_date) &&
                java.util.Objects.equals(f_time, o.f_time) &&
                java.util.Objects.equals(f_datetime, o.f_datetime) &&
                java.util.Objects.equals(f_timestamp, o.f_timestamp) &&
                java.util.Objects.equals(f_bit, o.f_bit) &&
                java.util.Objects.equals(f_varchar, o.f_varchar) &&
                java.util.Objects.equals(f_text, o.f_text) &&
                java.util.Objects.equals(count, o.count) &&
                java.util.Objects.equals(trim, o.trim) &&
                java.util.Objects.equals(min, o.min) &&
                java.util.Objects.equals(max, o.max) &&
                java.util.Objects.equals(sum, o.sum);
        }
    }
}