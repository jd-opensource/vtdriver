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

package testsuite;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.netty.util.internal.StringUtil;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import testsuite.internal.TestSuiteShardSpec;
import testsuite.internal.environment.DriverEnv;
import testsuite.internal.environment.TestSuiteEnv;
import testsuite.internal.testcase.TestSuiteCase;
import testsuite.util.printer.TestSuitePrinter;

public abstract class TestSuite extends TestSuitePrinter {

    /**
     * <p>
     * get Connection by <code>TestSuiteEnv</code>
     * </p>
     *
     * @param env {@link TestSuiteEnv}
     * @return {@link Connection}
     * @throws SQLException when errors occurred
     */
    protected static Connection getConnection(TestSuiteEnv env) throws SQLException {
        return env.getDevConnection();
    }

    public static void closeConnection(Connection... conns) {
        for (Connection conn : conns) {
            closeConnection(conn);
        }
    }

    public static void closeConnection(List<Connection> connectionList) {
        for (Connection connection : connectionList) {
            closeConnection(connection);
        }
    }

    public static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException throwables) {
                printFail(throwables.getMessage());
                throw new RuntimeException(throwables);
            }
        }
    }

    public static boolean compareResultSets(ResultSet resultSet1, ResultSet resultSet2) throws SQLException {
        if (resultSet1 == null || resultSet2 == null) {
            return false;
        }
        ResultSetMetaData metaData1 = resultSet1.getMetaData();
        ResultSetMetaData metaData2 = resultSet2.getMetaData();
        int columnCount1 = metaData1.getColumnCount();
        int columnCount2 = metaData2.getColumnCount();
        if (columnCount1 != columnCount2) {
            return false;
        }
        for (int i = 1; i <= columnCount1; i++) {
            String columnName1 = metaData1.getColumnName(i);
            String columnName2 = metaData2.getColumnName(i);
            int columnType1 = metaData1.getColumnType(i);
            int columnType2 = metaData2.getColumnType(i);
            if (!columnName1.equals(columnName2) || columnType1 != columnType2) {
                return false;
            }
        }
        while (resultSet1.next() && resultSet2.next()) {
            for (int i = 1; i <= columnCount1; i++) {
                Object value1 = resultSet1.getObject(i);
                Object value2 = resultSet2.getObject(i);
                if (resultSet1.wasNull() && resultSet2.wasNull()) {
                    // Both values are null, continue to next column
                    continue;
                }
                if (resultSet1.wasNull() || resultSet2.wasNull() || !value1.equals(value2)) {
                    return false;
                }
            }
        }
        return !resultSet1.next() && !resultSet2.next();
    }

    protected static ExecutorService getThreadPool(int num, int max) {
        ExecutorService pool = new ThreadPoolExecutor(num, max,
            60, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactory() {
                private final AtomicInteger threadNumber = new AtomicInteger(1);

                @Override
                public Thread newThread(final Runnable r) {
                    return new Thread(r, threadNumber.getAndIncrement() + "");
                }
            });
        return pool;
    }

    protected static <T extends TestSuiteCase> List<T> iterateExecFile(String filename, Class<T> caseClazz) throws IOException {
        BufferedReader br = Files.newBufferedReader(Paths.get(filename), StandardCharsets.UTF_8);
        StringBuilder content = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            content.append(line.trim());
        }
        if (StringUtil.isNullOrEmpty(content.toString())) {
            return Collections.emptyList();
        }
        ObjectMapper objectMapper = new ObjectMapper();
        TypeFactory typeFactory = objectMapper.getTypeFactory();
        List<T> testCaseList = objectMapper.readValue(content.toString(),
            typeFactory.constructCollectionType(List.class, caseClazz));
        testCaseList.forEach(testCase -> testCase.setFile(filename));
        return testCaseList;
    }

    protected static String getConnectionUrl(TestSuiteEnv env) {
        return env.getDevConnectionUrl();
    }

    protected <T extends TestSuiteCase> List<T> initCase(String filename, Class<T> caseClazz, String keyspace) throws IOException {
        List<T> testCaseList = iterateExecFile(filename, caseClazz);
        replaceKeyspace(testCaseList, keyspace);
        return testCaseList;
    }

    protected void replaceKeyspace(List<? extends TestSuiteCase> caseList, String keyspace) {
        for (TestSuiteCase testCase : caseList) {
            String query = testCase.getQuery().replaceAll(":KS|:ks", keyspace);
            testCase.setQuery(query);
        }
    }

    public void sleep(long second) {
        try {
            TimeUnit.SECONDS.sleep(second);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected String getKeyspace(TestSuiteEnv env) {
        return env.getKeyspace();
    }

    public void sleepMillisSeconds(long second) {
        try {
            TimeUnit.MILLISECONDS.sleep(second);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected String getUser(TestSuiteEnv env) {
        return env.getUser();
    }

    protected String getPassword(TestSuiteEnv env) {
        return env.getPassword();
    }

    protected String getCell(TestSuiteEnv env) {
        return env.getCell();
    }

    protected static class Driver {
        public static DriverEnv of(TestSuiteShardSpec shardSpec) {
            return new DriverEnv(shardSpec);
        }

        public static DriverEnv of(TestSuiteShardSpec shardSpec, String charEncoding) {
            return new DriverEnv(shardSpec, charEncoding);
        }
    }

    public static void printResult(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 0; i < columnCount; i++) {
            System.out.print(metaData.getColumnLabel(i + 1) + " ");
        }
        System.out.println();
        int count = 0;
        while (resultSet.next()) {
            for (int i = 0; i < columnCount; i++) {
                System.out.print(resultSet.getObject(i + 1) + " ");
            }
            count++;
            System.out.println();
        }
        System.out.println("count:" + count);
    }
}
