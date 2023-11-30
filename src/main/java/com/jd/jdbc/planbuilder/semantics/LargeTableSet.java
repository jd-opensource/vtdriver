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

import java.util.Arrays;
import java.util.function.Consumer;
import lombok.Data;
@Data
class LargeTableSet {


    private long[] tables;

    public LargeTableSet(long[] tables) {
        this.tables = tables;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LargeTableSet that = (LargeTableSet) o;
        return Arrays.equals(tables, that.tables);
    }

    @Override
    public int hashCode() {
        return tables.hashCode();
    }

    @Override
    public LargeTableSet clone() {
        return new LargeTableSet(this.tables.clone());
    }

    public static LargeTableSet newLargeTableSet(Long small, int tableidx) {
        int chunk = tableidx / 64;
        int offset = tableidx % 64;
        long[] tables = new long[chunk + 1];
        tables[0] = small;
        tables[chunk] |= 1L << offset;
        return new LargeTableSet(tables);
    }

    public int popcount() {
        int count = 0;
        for (Long t : tables) {
            count += Long.bitCount(t);
        }
        return count;
    }

    public LargeTableSet merge(LargeTableSet other) {
        long[] smallTables = null;
        long[] largeTables = null;
        int m = 0;
        if (this.tables.length >= other.tables.length) {
            smallTables = other.tables;
            largeTables = this.tables;
        } else {
            smallTables = this.tables;
            largeTables = other.tables;
        }

        long[] merged = new long[largeTables.length];

        while (m < smallTables.length) {
            merged[m] = smallTables[m] | largeTables[m];
            m++;
        }
        while (m < largeTables.length) {
            merged[m] = largeTables[m];
            m++;
        }
        return new LargeTableSet(merged);
    }

    public LargeTableSet mergeSmall(long small) {
        long[] merged = new long[this.tables.length];
        System.arraycopy(tables, 0, merged, 0, this.tables.length);
        merged[0] |= small;
        return new LargeTableSet(merged);
    }

    public void mergeSmallInPlace(Long small) {
        this.tables[0] |= small;
    }

    public void mergeInPlace(LargeTableSet other) {
        if (other.tables.length > this.tables.length) {
            long[] merged = new long[other.tables.length];
            System.arraycopy(tables, 0, merged, 0, this.tables.length);
            this.tables = merged;
        }

        for (int i = 0; i < other.tables.length; i++) {
            this.tables[i] |= other.tables[i];
        }
    }

    public int tableOffset() {
        int offset = 0;
        boolean found = false;

        for (int chunk = 0; chunk < tables.length; chunk++) {
            long t = tables[chunk];
            if (t != 0) {
                if (found || Long.bitCount(t) != 1) {
                    return -1;
                }
                offset = chunk * 64 + Long.numberOfTrailingZeros(t);
                found = true;
            }
        }
        return offset;
    }

    public void forEachTable(Consumer tableSetCallback) {
        for (int i = 0; i < tables.length; i++) {
            long bitset = tables[i];
            while (bitset != 0) {
                long t = bitset & -bitset;
                int r = Long.numberOfTrailingZeros(bitset);
                tableSetCallback.accept(i * 64 + r);
                bitset ^= t;
            }
        }
    }

    public Boolean containsSmall(long small) {
        return (small & this.tables[0]) == small;
    }

    public Boolean isContainedBy(LargeTableSet b) {
        if (this.tables.length > b.tables.length) {
            return false;
        }

        for (int i = 0; i < this.tables.length; i++) {
            long t = this.tables[i];
            if ((t & b.tables[i]) != t) {
                return false;
            }
        }
        return true;
    }

    public Boolean overlapsSamll(long small) {
        return (tables[0] & small) != 0;
    }

    public Boolean overlaps(LargeTableSet b) {
        int min = Math.min(this.tables.length, b.tables.length);
        for (int t = 0; t < min; t++) {
            if ((this.tables[t] & b.tables[t]) != 0) {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds the given table index to this largeTableSet.
     *
     * @param tableidx the index of the table to add
     */
    public void add(int tableidx) {
        int chunk = tableidx / 64;
        int offset = tableidx % 64;

        if (tables.length <= chunk) {
            long[] newTables = Arrays.copyOf(tables, chunk + 1);
            tables = newTables;
        }

        tables[chunk] |= 1L << offset;
    }

}
