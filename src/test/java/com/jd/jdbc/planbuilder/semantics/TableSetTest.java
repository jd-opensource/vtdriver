/*
Copyright 2023 JD Project Authors. Licensed under Apache-2.0.

Copyright 2022 The Vitess Authors.

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

package com.jd.jdbc.planbuilder.semantics;

import static com.jd.jdbc.planbuilder.semantics.TableSet.singleTableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

public class TableSetTest {
    private final TableSet f1 = singleTableSet(0);

    private final TableSet f2 = singleTableSet(1);

    private final TableSet f3 = singleTableSet(2);

    private final TableSet f12 = f1.merge(f2);

    private final TableSet f123 = f12.merge(f3);

    @Test
    public void testTableOffset() {
        Assert.assertEquals(0, f1.tableOffset());
        Assert.assertEquals(1, f2.tableOffset());
        Assert.assertEquals(2, f3.tableOffset());
    }

    @Test
    public void testLargeOffset() {
        for (int tid = 0; tid < 1024; tid++) {
            TableSet ts = singleTableSet(tid);
            Assert.assertEquals(tid, ts.tableOffset());
        }
    }

    @Test
    public void testTableSetConstituents() {
        Assert.assertEquals(Arrays.asList(f1, f2, f3), f123.constituents());
        Assert.assertEquals(Arrays.asList(f1, f2), f12.constituents());
        Assert.assertEquals(Arrays.asList(f1, f3), f1.merge(f3).constituents());
        Assert.assertEquals(Arrays.asList(f2, f3), f2.merge(f3).constituents());
        Assert.assertEquals(new ArrayList<>(), new TableSet().constituents());
    }

    @Test
    public void testIsOverlapping() {
        Assert.assertTrue(f12.isOverlapping(f12));
        Assert.assertTrue(f1.isOverlapping(f12));
        Assert.assertTrue(f12.isOverlapping(f1));
        Assert.assertFalse(f3.isOverlapping(f12));
        Assert.assertFalse(f12.isOverlapping(f3));
    }

    @Test
    public void testIsSolvedBy() {
        Assert.assertTrue(f1.isSolvedBy(f12));
        Assert.assertFalse(f12.isSolvedBy(f1));
        Assert.assertFalse(f3.isSolvedBy(f12));
        Assert.assertFalse(f12.isSolvedBy(f3));
    }

    @Test
    public void testConstituents() {
        Assert.assertEquals(Arrays.asList(f1, f2, f3), f123.constituents());
        Assert.assertEquals(Arrays.asList(f1, f2), f12.constituents());
        Assert.assertEquals(Arrays.asList(f1, f3), f1.merge(f3).constituents());
        Assert.assertEquals(Arrays.asList(f2, f3), f2.merge(f3).constituents());
        Assert.assertTrue(new TableSet().constituents().isEmpty());
    }

    @Test
    public void testLargeTablesConstituents() {
        final int GapSize = 32;

        TableSet ts = new TableSet();
        List<TableSet> expected = new ArrayList<>();
        int table = 0;

        for (int t = 0; t < 256; t++) {
            table += new Random().nextInt(GapSize) + 1;
            expected.add(singleTableSet(table));
            ts.addTable(table);
        }
        Assert.assertEquals(expected, ts.constituents());
    }

    @Test
    public void testTableSetLargeMergeInPlace() {
        final int SetRange = 256;
        final int Blocks = 64;

        TableSet[] tablesets = new TableSet[Blocks];
        for (int i = 0; i < Blocks; i++) {
            tablesets[i] = new TableSet();
            int setrng = i * SetRange;

            for (int tid = 0; tid < SetRange; tid++) {
                tablesets[i].addTable(setrng + tid);
            }
        }
        TableSet result = new TableSet();
        for (TableSet ts : tablesets) {
            result.mergeInPlace(ts);
        }
        List<TableSet> expected = new ArrayList<>();
        for (int tid = 0; tid < SetRange * Blocks; tid++) {
            expected.add(singleTableSet(tid));
        }
        Assert.assertEquals(expected, result.constituents());
    }

    @Test
    public void testTabletSetLargeMerge() {
        final int SetRange = 256;
        final int Blocks = 64;

        TableSet[] tablesets = new TableSet[64];

        for (int i = 0; i < tablesets.length; i++) {
            TableSet ts = new TableSet();
            int setrng = i * SetRange;

            for (int tid = 0; tid < SetRange; tid++) {
                ts.addTable(setrng + tid);
            }

            tablesets[i] = ts;
        }

        TableSet result = new TableSet();
        for (TableSet ts : tablesets) {
            result = result.merge(ts);
        }

        List<TableSet> expected = new ArrayList<>();
        for (int tid = 0; tid < SetRange * Blocks; tid++) {
            expected.add(singleTableSet(tid));
        }
        Assert.assertEquals(expected, result.constituents());
    }

    @Test
    public void testTableSetKeepOnly() {

        TableSet ts1, ts2, result;

        // Test case 1
        ts1 = singleTableSet(1).merge(singleTableSet(2)).merge(singleTableSet(3));
        ts2 = singleTableSet(1).merge(singleTableSet(3)).merge(singleTableSet(4));
        result = singleTableSet(1).merge(singleTableSet(3));
        ts1.keepOnly(ts2);
        Assert.assertEquals("both small test is fail", result, ts1);

        // Test case 2
        ts1 = singleTableSet(1428).merge(singleTableSet(2432)).merge(singleTableSet(3412));
        ts2 = singleTableSet(1428).merge(singleTableSet(3412)).merge(singleTableSet(4342));
        result = singleTableSet(1428).merge(singleTableSet(3412));
        ts1.keepOnly(ts2);
        Assert.assertEquals("both large test is fail", result, ts1);

        // Test case 3
        ts1 = singleTableSet(1).merge(singleTableSet(2)).merge(singleTableSet(3));
        ts2 = singleTableSet(1).merge(singleTableSet(3)).merge(singleTableSet(4342));
        result = singleTableSet(1).merge(singleTableSet(3));
        ts1.keepOnly(ts2);
        Assert.assertEquals("ts1 small ts2 large test is fail", result, ts1);

        // Test case 4
        ts1 = singleTableSet(1).merge(singleTableSet(2771)).merge(singleTableSet(3));
        ts2 = singleTableSet(1).merge(singleTableSet(3)).merge(singleTableSet(4));
        result = singleTableSet(1).merge(singleTableSet(3));
        ts1.keepOnly(ts2);
        Assert.assertEquals("ts1 large ts2 small test is fail", result, ts1);
    }

    @Test
    public void testRemoveInPlace() {
        class TableSetTestCase {
            String name;

            TableSet ts1;

            TableSet ts2;

            TableSet result;

            public TableSetTestCase(String name, TableSet ts1, TableSet ts2, TableSet result) {
                this.name = name;
                this.ts1 = ts1;
                this.ts2 = ts2;
                this.result = result;
            }
        }
        List<TableSetTestCase> testCases = Arrays.asList(
            new TableSetTestCase(
                "both small",
                singleTableSet(1).merge(singleTableSet(2)).merge(singleTableSet(3)),
                singleTableSet(1).merge(singleTableSet(5)).merge(singleTableSet(4)),
                singleTableSet(2).merge(singleTableSet(3))
            ),
            new TableSetTestCase(
                "both large",
                singleTableSet(1428).merge(singleTableSet(2432)).merge(singleTableSet(3412)),
                singleTableSet(1424).merge(singleTableSet(2432)).merge(singleTableSet(4342)),
                singleTableSet(1428).merge(singleTableSet(3412))
            ),
            new TableSetTestCase(
                "ts1 small ts2 large",
                singleTableSet(1).merge(singleTableSet(2)).merge(singleTableSet(3)),
                singleTableSet(14).merge(singleTableSet(2)).merge(singleTableSet(4342)),
                singleTableSet(1).merge(singleTableSet(3))
            ),
            new TableSetTestCase(
                "ts1 large ts2 small",
                singleTableSet(1).merge(singleTableSet(2771)).merge(singleTableSet(3)),
                singleTableSet(1).merge(singleTableSet(3)).merge(singleTableSet(4)),
                singleTableSet(2771)
            )
        );

        for (TableSetTestCase testCase : testCases) {
            testCase.ts1.removeInPlace(testCase.ts2);
            Assert.assertEquals(testCase.name + " test is fail", testCase.result, testCase.ts1);
        }
    }

}