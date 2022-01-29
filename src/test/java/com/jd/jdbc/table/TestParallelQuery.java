package com.jd.jdbc.table;

import com.jd.jdbc.session.SafeSession;
import com.jd.jdbc.vitess.VitessConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import testsuite.TestSuite;

import static testsuite.internal.TestSuiteShardSpec.TWO_SHARDS;

public class TestParallelQuery extends TestSuite {

    @Test
    public void testSetParallelParams() {
        String baseUrl = getConnectionUrl(Driver.of(TWO_SHARDS));
        try (Connection conn = DriverManager.getConnection(baseUrl + "&queryParallelNum=0&vtMaximumPoolSize=30")) {
            int maxParallelNum = SafeSession.newSafeSession((VitessConnection) conn).getMaxParallelNum();
            Assert.assertEquals(1, maxParallelNum);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            Assert.fail();
        }

        try (Connection conn = DriverManager.getConnection(baseUrl)) {
            int maxParallelNum = SafeSession.newSafeSession((VitessConnection) conn).getMaxParallelNum();
            Assert.assertEquals(1, maxParallelNum);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            Assert.fail();
        }

        try (Connection conn = DriverManager.getConnection(baseUrl + "&queryParallelNum=8&vtMaximumPoolSize=30")) {
            int maxParallelNum = SafeSession.newSafeSession((VitessConnection) conn).getMaxParallelNum();
            Assert.assertEquals(8, maxParallelNum);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    @Ignore
    public void testSplitTableParallelQuery() {
        RandomStringUtils.random(10, 65, 123, true, true);
        String query = "SELECT count(1) FROM pop_ware_ware ";

        String baseUrl = getConnectionUrl(Driver.of(TWO_SHARDS));
        baseUrl += "&queryParallelNum=1&vtMaximumPoolSize=30";
        try (Connection conn = DriverManager.getConnection(baseUrl);
             Statement stmt = conn.createStatement()) {
            Long currentTime = System.currentTimeMillis();
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                System.out.println("count: " + rs.getInt(1));
            }
            Long endTime = System.currentTimeMillis();
            System.out.println(endTime - currentTime);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
