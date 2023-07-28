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

package com.jd.jdbc.table;

import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.tindexes.SplitTableUtil;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Assert;
import org.junit.Test;

public class SplitTableUtilTest {

    @Test
    public void getActualTableNames() {
        int start = RandomUtils.nextInt();
        for (int i = start; i < start + 10; i++) {
            String actualTableName = SplitTableUtil.getActualTableName("vtdriver-split-table.yml", "commerce", "table_engine_test", i);
            Assert.assertTrue("actualTableName should not empty", StringUtils.isNotEmpty(actualTableName));
        }
    }

    @Test
    public void getActualTableName() {
        String actualTableName = SplitTableUtil.getActualTableName("commerce", "table_engine_test", RandomUtils.nextInt());
        Assert.assertTrue("actualTableName should not empty", StringUtils.isNotEmpty(actualTableName));
    }

    @Test
    public void getShardingColumnName() {
        String shardingColumnName = SplitTableUtil.getShardingColumnName("commerce", "table_engine_test");
        Assert.assertTrue("getShardingColumnName error", "f_key".equalsIgnoreCase(shardingColumnName));
    }

    @Test
    public void getShardingColumnName2() {
        String shardingColumnName = SplitTableUtil.getShardingColumnName("commerce", "table_engine_test3");
        Assert.assertNull(shardingColumnName);
    }

    @Test
    public void getShardingColumnName3() {
        String shardingColumnName = SplitTableUtil.getShardingColumnName("commerce2", "table_engine_test");
        Assert.assertNull(shardingColumnName);
    }

    @Test
    public void getShardingColumnName4() {
        String shardingColumnName = SplitTableUtil.getShardingColumnName("commerce3", "table_engine_test3");
        Assert.assertNull(shardingColumnName);
    }
}