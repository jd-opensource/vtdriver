package com.jd.jdbc.engine.join;

import com.jd.jdbc.engine.SelectTestSuite;
import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.testcase.TestSuiteCase;

public class JoinTypeTest extends SelectTestSuite {

    protected static List<TestCase> testCaseList;

    public static void initTestCase() throws IOException {
        testCaseList.addAll(iterateExecFile("src/test/resources/engine/join/datatype/datatype_join_test.json", TestCase.class));
    }

    protected static void initTestData() {
        String insertSql1 = "insert into all_type_test (" +
            "`tinyint`, `u_tinyint`, `tinyint_1`, `u_tinyint_1`, `smallint`, `u_smallint`, `mediumint`, `u_mediumint`," +
            "`int`, `u_int`, `bigint`, `u_bigint`, `bit_1`, `bit_64`, `float`, `u_float`, " +
            "`double`, `u_double`, `char`,`varchar`," +
            "`tinytext`,`text`,`mediumtext`,`longtext`,`json`,`binary`,`varbinary`,`tinyblob`," +
            "`blob`,`mediumblob`,`longblob`,`date`,`time_1`,`time_3`, `year`,`datetime_3`," +
            "`datetime_6`,`timestamp_3`,`timestamp_6`) values (" +
            "1, 1, 1, 1, 11, 11, 1111, 1111, " +
            "1111, 1111, 111111111111, 111111111111, true, true, 0.111111, 0.111111, " +
            "0.1111111111, 0.1111111111, 'david', 'david', " +
            "'this is a text', 'this is a text', 'this is a text', 'this is a text', null , x'9fad5e9eefdfb449', x'9fad5e9eefdfb449', " +
            "x'89504E470D0A1A0A0000000D494844520000001000000010080200000090916836000000017352474200AECE1CE90000000467414D410000B18F0BFC6105000000097048597300000EC300000EC301C76FA8640000001E49444154384F6350DAE843126220493550F1A80662426C349406472801006AC91F1040F796BD0000000049454E44AE426082', " +
            "x'89504E470D0A1A0A0000000D494844520000001000000010080200000090916836000000017352474200AECE1CE90000000467414D410000B18F0BFC6105000000097048597300000EC300000EC301C76FA8640000001E49444154384F6350DAE843126220493550F1A80662426C349406472801006AC91F1040F796BD0000000049454E44AE426082'," +
            "x'89504E470D0A1A0A0000000D494844520000001000000010080200000090916836000000017352474200AECE1CE90000000467414D410000B18F0BFC6105000000097048597300000EC300000EC301C76FA8640000001E49444154384F6350DAE843126220493550F1A80662426C349406472801006AC91F1040F796BD0000000049454E44AE426082'," +
            "x'89504E470D0A1A0A0000000D494844520000001000000010080200000090916836000000017352474200AECE1CE90000000467414D410000B18F0BFC6105000000097048597300000EC300000EC301C76FA8640000001E49444154384F6350DAE843126220493550F1A80662426C349406472801006AC91F1040F796BD0000000049454E44AE426082'," +
            " '2021-01-01', now(), now(), 2021, now(), now(), now(), now())";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("delete from all_type_test;");
            stmt.execute(insertSql1);
        } catch (SQLException e) {
            Assert.fail("Failed: error init sql");
        }
    }

    @Before
    public void init() throws SQLException, IOException {
        conn = getConnection(Driver.of(TestSuiteShardSpec.TWO_SHARDS));
        testCaseList = new ArrayList<>();
        initTestCase();
        initTestData();
    }

    @After
    public void cleanup() throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("delete from all_type_test;");
        }
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    public void testDataTypeJoin() throws SQLException, IllegalAccessException, NoSuchFieldException, NoSuchMethodException {
        execute(testCaseList, false);
    }

    @Override
    protected List<? extends ResultRow> rsToResultList(ResultSet rs, Object tclass) throws SQLException, NoSuchFieldException, IllegalAccessException {
        TestCase testCase = (TestCase) tclass;
        List<ResultRow> testResultList = new ArrayList<>();
        TestSuiteCase.Field[] fields = testCase.getFields();
        while (rs.next()) {
            ResultRow testResult = new ResultRow();
            for (int j = 0; j < fields.length; j++) {
                TestSuiteCase.Field field = fields[j];
                String fieldName = "".equals(field.getAlias()) ? field.getName() : field.getAlias();
                String javaType = field.getJavaType();
                switch (javaType) {
                    case "Integer":
                        int intValue = rs.getInt(j + 1);
                        if ("id".equalsIgnoreCase(fieldName)) {
                            testResult.setId(intValue);
                        }
                }
            }
            testResultList.add(testResult);
        }
        return testResultList;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected static class TestCase extends SelectTestSuite.TestCase {

        private ResultRow[] verifyResult;

        private String exception;

        private String errorMessage;
    }

    @Data
    protected static class ResultRow extends SelectTestSuite.ResultRow implements Comparable<ResultRow> {
        // column fields in ResultSet
        private Integer id;

        @Override
        public int compareTo(ResultRow o) {
            if (this.hashCode() == o.hashCode()) {
                return 0;
            } else if (this.hashCode() > o.hashCode()) {
                return 1;
            }
            return -1;
        }

        @Override
        public int hashCode() {
            return com.google.common.base.Objects
                .hashCode(id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof ResultRow)) {
                return false;
            }

            ResultRow that = (ResultRow) o;
            return Objects.equals(that.id, id);
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
    }
}
