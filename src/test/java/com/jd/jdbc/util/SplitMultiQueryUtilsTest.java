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

package com.jd.jdbc.util;

import com.google.common.base.Splitter;
import com.jd.jdbc.sqlparser.utils.SplitMultiQueryUtils;
import java.io.IOException;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.Assert;
import org.junit.Test;
import testsuite.TestSuite;
import testsuite.internal.testcase.TestSuiteCase;

public class SplitMultiQueryUtilsTest extends TestSuite {
    private List<TestCase> testCaseList;

    @Test
    public void test01() {
        StringBuilder sql = new StringBuilder();
        int count = 100000;
        for (int i = 0; i < 10; i++) {
            sql.append("select * from user_extra;");
        }
        String inStr = sql.toString();
        long begin, end;
        begin = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Splitter.on(";").omitEmptyStrings().trimResults().splitToList(inStr);
        }
        end = System.currentTimeMillis();
        printNormal("VtDriver Splitter.on(\";\"): ");
        printInfo((end - begin) + "mm");
        begin = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            SplitMultiQueryUtils.splitMulti(inStr);
        }
        end = System.currentTimeMillis();
        printNormal("Custom split method: ");
        printInfo((end - begin) + "mm");
        begin = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            Splitter.on(";").omitEmptyStrings().trimResults().splitToList(inStr);
        }
        end = System.currentTimeMillis();
        printNormal("VtDriver Splitter.on(\";\")");
        printInfo((end - begin) + "mm");
        begin = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            SplitMultiQueryUtils.splitMulti(inStr);
        }
        end = System.currentTimeMillis();
        printNormal("Custom split method: ");
        printInfo((end - begin) + "mm");
    }

    @Test
    public void test02() throws IOException {
        initTestCase();
        int i = 0;
        List<String> rss;
        for (TestCase testCase : testCaseList) {
            i++;
            printNormal("No." + i);
            printNormal("Input:");
            printInfo(testCase.getInitSql());
            rss = SplitMultiQueryUtils.splitMulti(testCase.getInitSql());
            Assert.assertEquals(testCase.getVerifyResult().size(), rss.size());
            printNormal("Output:");
            for (int j = 0; j < rss.size(); j++) {
                printInfo("expected:" + testCase.getVerifyResult().get(j));
                printInfo("actual  :" + rss.get(j));
                Assert.assertEquals(testCase.getVerifyResult().get(j), rss.get(j));
            }
        }
    }

    protected void initTestCase() throws IOException {
        testCaseList = iterateExecFile("src/test/resources/util/splitmultiqueryutils.json", TestCase.class);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    protected static class TestCase extends TestSuiteCase {
        private String initSql;

        private List<String> verifyResult;

        private String errorMessage;
    }
}
