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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.Setter;

/**
 * TableSet is how a set of tables is expressed.
 * Tables get unique bits assigned in the order that they are encountered during semantic analysis.
 * This TableSet implementation is optimized for sets of less than 64 tables, but can grow to support an arbitrary
 * large amount of tables.
 */
@Getter
@Setter
public class TableSet {
    private long small;

    private LargeTableSet large;

    public TableSet(long small, LargeTableSet large) {
        this.small = small;
        this.large = large;
    }

    public TableSet() {
    }

    @Override
    public TableSet clone() {
        return new TableSet(this.small, this.large.clone());
    }

    /**
     * SingleTableSet creates a TableSet that contains only the given table
     *
     * @param tableidx
     * @return
     */
    public static TableSet singleTableSet(int tableidx) {
        if (tableidx < 64) {
            long smll = 1L << tableidx;
            return new TableSet(smll, null);
        }
        return new TableSet(0, newLargeTableSet(0L, tableidx));
    }

    /**
     * EmptyTableSet creates an empty TableSet
     *
     * @return
     */
    public static TableSet emptyTableSet() {
        return new TableSet(0, null);
    }


    /**
     * NumberOfTables returns the number of bits set
     *
     * @return
     */
    public int numberOfTables() {
        if (this.large == null) {
            return Long.bitCount(this.small);
        }
        return this.large.popcount();
    }

    // Merge creates a TableSet that contains both inputs
    public TableSet merge(TableSet other) {
        TableSet tableSet = new TableSet();
        if (this.large == null && other.large == null) {
            tableSet.setSmall(this.small | other.small);
        } else if (this.large == null) {
            tableSet.setLarge(other.large.mergeSmall(this.small));
        } else if (other.large == null) {
            tableSet.setLarge(this.large.mergeSmall(other.small));
        } else {
            tableSet.setLarge(this.large.merge(other.large));
        }
        return tableSet;
    }


    /**
     * MergeInPlace merges all the tables in `other` into this TableSet
     *
     * @param other
     */
    public void mergeInPlace(TableSet other) {
        if (this.large == null && other.large == null) {
            this.small |= other.small;
        } else if (this.large == null) {
            this.large = other.large.mergeSmall(this.small);
        } else if (other.large == null) {
            this.large.mergeSmallInPlace(other.small);
        } else {
            this.large.mergeInPlace(other.large);
        }
    }

    // MergeTableSets merges all the given TableSet into a single one
    public static TableSet mergeTableSets(TableSet... tss) {
        TableSet res = new TableSet(0, null);
        for (TableSet ts : tss) {
            res.mergeInPlace(ts);
        }
        return res;
    }

    // ForEachTable calls the given callback with the indices for all tables in this TableSet
    private void forEachTable(Consumer tableSetCallback) {
        if (this.large == null) {
            long bitset = this.small;
            while (bitset != 0) {
                long t = bitset & -bitset;
                tableSetCallback.accept(Long.numberOfTrailingZeros(bitset));
                bitset ^= t;
            }
        } else {
            this.large.forEachTable(tableSetCallback);
        }
    }

    // Constituents returns a slice with the indices for all tables in this TableSet
    public List<TableSet> constituents() {
        List<TableSet> result = new ArrayList<>();
        forEachTable(t -> result.add(TableSet.singleTableSet((Integer) t)));
        return result;
    }

    /**
     * returns true if `ts` and `other` contain the same tables
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableSet other = (TableSet) o;
        return this.isSolvedBy(other) && other.isSolvedBy(this);
    }

    @Override
    public int hashCode() {
        if (large != null) {
            return Objects.hash(small, large.hashCode());
        } else {
            return Objects.hash(small);
        }
    }

    @Override
    public String toString() {
        StringJoiner res = new StringJoiner(",", TableSet.class.getSimpleName() + "{", "}");
        List<Integer> list = new ArrayList<>();
        forEachTable(t -> list.add((Integer) t));
        for (Integer s : list) {
            res.add(s.toString());
        }
        return res.toString();
    }

    /**
     * TableOffset returns the offset in the Tables array from TableSet
     *
     * @return
     */
    public int tableOffset() {
        if (this.large == null) {
            if (Long.bitCount(this.small) != 1) {
                return -1;
            }
            return Long.numberOfTrailingZeros(this.small);
        }
        return this.large.tableOffset();
    }

    /**
     * IsSolvedBy returns true if all of `ts` is contained in `other`
     *
     * @return
     */
    public Boolean isSolvedBy(TableSet other) {
        if (this.large == null && other.large == null) {
            return (this.small & other.small) == this.small;
        } else if (this.large == null) {
            return other.large.containsSmall(this.small);
        } else if (other.large == null) {
            return false;
        } else {
            return this.large.isContainedBy(other.large);
        }
    }

    /**
     * IsOverlapping returns true if at least one table exists in both sets
     *
     * @param other
     * @return
     */
    public Boolean isOverlapping(TableSet other) {
        if (this.large == null && other.large == null) {
            return (this.small & other.small) != 0;
        } else if (this.large == null) {
            return other.large.overlapsSamll(this.small);
        } else if (other.large == null) {
            return this.large.overlapsSamll(other.small);
        } else {
            return this.large.overlaps(other.large);
        }
    }

    /**
     * AddTable adds the given table to this set
     *
     * @param tableidx
     */
    public void addTable(int tableidx) {
        if (large == null && tableidx < 64) {
            small |= 1L << tableidx;
        } else if (large == null) {
            large = newLargeTableSet(small, tableidx);
        } else {
            large.add(tableidx);
        }
    }

    /**
     * Creates a new largeTableSet with the given small table set and table index.
     *
     * @param small    the small table set
     * @param tableidx the index of the table to add
     * @return the new largeTableSet
     */
    private static LargeTableSet newLargeTableSet(long small, int tableidx) {
        int chunk = tableidx / 64;
        int offset = tableidx % 64;

        long[] tables = new long[chunk + 1];
        tables[0] = small;
        tables[chunk] |= 1L << offset;

        return new LargeTableSet(tables);
    }

    /**
     * // KeepOnly removes all the tables not in `other` from this TableSet
     * @param other
     */
    public void keepOnly(TableSet other) {
        if (this.large == null && other.large == null) {
            this.small &= other.small;
        } else if (this.large == null) {
            this.small &= other.large.getTables()[0];
        } else if (other.large == null) {
            this.small = this.large.getTables()[0] & other.small;
            this.large = null;
        } else {
            for (int i = 0; i < this.large.getTables().length; i++) {
                if (i >= other.large.getTables().length) {
                    this.large.setTables(Arrays.copyOf(this.large.getTables(), i));
                    break;
                }
                this.large.getTables()[i] &= other.large.getTables()[i];
            }
        }
    }

    // RemoveInPlace removes all the tables in `other` from this TableSet
    public void removeInPlace(TableSet other) {
        if (large == null && other.large == null) {
            small &= ~other.small;
        } else if (large == null) {
            for (int i = 0; i < other.large.getTables().length; i++) {
                small &= ~other.large.getTables()[i];
            }
        } else if (other.large == null) {
            large.getTables()[0] &= ~other.small;
        } else {
            for (int i = 0; i < Math.min(large.getTables().length, other.large.getTables().length); i++) {
                large.getTables()[i] &= ~other.large.getTables()[i];
            }
        }
    }
}
