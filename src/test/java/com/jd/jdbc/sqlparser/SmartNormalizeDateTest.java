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

package com.jd.jdbc.sqlparser;

import com.jd.jdbc.sqlparser.ast.SQLStatement;
import com.jd.jdbc.sqlparser.dialect.mysql.SmartNormalizer;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.VtRestoreVisitor;
import java.sql.SQLException;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

public class SmartNormalizeDateTest {
    static String sql = "select * from dd where d_date = '2020-12-10' and d_time = '09:21:58' and d_timestamp = '2020-12-10 09:24:14' " +
        "and d_datetime = '2020-12-10 09:24:15' and d_year = '2020';";

    static String questionSql = "select * from dd where d_date = ? and d_time = ? and d_timestamp = ? " +
        "and d_datetime = ? and d_year = ?;";

    @Test
    public void test() throws SQLException {
        SQLStatement stmt = SQLUtils.parseSingleMysqlStatement(sql);
        SmartNormalizer.SmartNormalizerResult result = SmartNormalizer.normalize(stmt, Collections.emptyMap(), null);

        StringBuilder output = new StringBuilder();
        VtRestoreVisitor vtRestoreVisitor = new VtRestoreVisitor(output, result.getBindVariableMap(), null);
        result.getStmt().accept(vtRestoreVisitor);
        Assert.assertEquals(sql, output.toString());
    }
}