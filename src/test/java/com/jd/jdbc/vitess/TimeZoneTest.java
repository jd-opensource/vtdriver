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

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.TimeZone;
import org.junit.Before;

public class TimeZoneTest extends TimeTest {
    @Before
    @Override
    public void init() throws SQLException {
        // 设置本地时区
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        // 设置一个随机生成的(错误的)serverTimezone
        serverTimezone = TimeTestUtil.getRandomTimeZone();

        String url = getConnectionUrl(Driver.of(testsuite.internal.TestSuiteShardSpec.TWO_SHARDS));
        url = url.replaceAll("serverTimezone=[^&]*", "serverTimezone=" + serverTimezone.getID());
        printNormal("url: " + url);
        conn = DriverManager.getConnection(url);
    }
}
