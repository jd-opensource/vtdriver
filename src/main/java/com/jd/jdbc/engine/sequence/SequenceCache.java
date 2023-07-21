/*
Copyright 2021 JD Project Authors. Licensed under Apache-2.0.

Copyright 2019 The Vitess Authors.

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

package com.jd.jdbc.engine.sequence;

import com.jd.jdbc.engine.Vcursor;
import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.srvtopo.ResolvedShard;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * insert sequence cache.
 */
public final class SequenceCache {

    private static final Log log = LogFactory.getLog(SequenceCache.class);

    private static final int DEFAULT_RETRY_TIMES = 100;

    private static final Map<String, Cache> CACHE_MAP = new ConcurrentHashMap<>();

    private static final Map<String, ReentrantLock> LOCK_MAP = new ConcurrentHashMap<>();

    private final ReentrantLock lock = new ReentrantLock();

    public static long[] getVtResultValue(VtResultSet vtResultSet) throws SQLException {
        if (vtResultSet == null) {
            throw new SQLException("getInsertId error");
        }
        List<List<VtResultValue>> rows = vtResultSet.getRows();
        if (rows == null || rows.isEmpty()) {
            throw new SQLException("getInsertId error");
        }
        List<VtResultValue> vtResultValues = rows.get(0);
        if (vtResultValues == null || vtResultValues.isEmpty()) {
            throw new SQLException("getInsertId error");
        }

        long[] sequenceInfo = new long[2];
        for (int i = 0; i < 2; i++) {
            VtResultValue vtResultValue = vtResultValues.get(i);
            if (vtResultValue == null) {
                throw new SQLException("getInsertId error");
            }
            sequenceInfo[i] = (long) vtResultValue.getValue();
        }

        return sequenceInfo;
    }

    private Cache getCache(Vcursor vCursor, ResolvedShard resolvedShard, String sequenceTableName) throws SQLException, InterruptedException {
        long[] sequenceInfo = querySequenceValue(vCursor, resolvedShard, sequenceTableName);
        if (log.isDebugEnabled()) {
            log.debug("sequence cache info, next:" + sequenceInfo[0] + ", cache:" + sequenceInfo[1]);
        }
        return new Cache(sequenceInfo[0], sequenceInfo[1]);
    }

    public long[] querySequenceValue(Vcursor vCursor, ResolvedShard resolvedShard, String sequenceTableName) throws SQLException, InterruptedException {

        int retryTimes = DEFAULT_RETRY_TIMES;
        while (retryTimes > 0) {
            String querySql = "select next_id, cache from " + sequenceTableName + " where id = 0";
            VtResultSet vtResultSet = (VtResultSet) vCursor.executeStandalone(querySql, new HashMap<>(), resolvedShard, false);
            long[] sequenceInfo = getVtResultValue(vtResultSet);

            long next = sequenceInfo[0];
            long cache = sequenceInfo[1];
            if (next == 0) {
                next = 1;
            }
            if (cache <= 0) {
                throw new SQLException("cache value in " + sequenceTableName + " is invalid! it should be greater than 0");
            }

            String updateSql = "update " + sequenceTableName + " set next_id = " + (next + cache) + " where next_id =" + sequenceInfo[0];
            VtRowList vtRowList = vCursor.executeStandalone(updateSql, new HashMap<>(), resolvedShard, false);
            if (vtRowList.getRowsAffected() == 1) {
                sequenceInfo[0] = next;
                return sequenceInfo;
            }
            retryTimes--;
            Thread.sleep(ThreadLocalRandom.current().nextInt(1, 6));
        }
        throw new SQLException("Update sequence cache failed within retryTimes: " + DEFAULT_RETRY_TIMES);
    }

    public long nextValue(Vcursor vCursor, ResolvedShard resolvedShard, String keyspace, String sequenceTableName) throws SQLException, InterruptedException {
        String cacheKey = keyspace + " " + sequenceTableName;
        if (!CACHE_MAP.containsKey(cacheKey)) {
            lock.lock();
            try {
                if (!CACHE_MAP.containsKey(cacheKey)) {
                    CACHE_MAP.put(cacheKey, getCache(vCursor, resolvedShard, sequenceTableName));
                    LOCK_MAP.put(cacheKey, new ReentrantLock());
                }
            } finally {
                lock.unlock();
            }
        }

        long value = CACHE_MAP.get(cacheKey).getAndIncrement();
        if (value == -1) {
            LOCK_MAP.get(cacheKey).lock();
            try {
                for (; ; ) {
                    if (CACHE_MAP.get(cacheKey).isOver()) {
                        CACHE_MAP.put(cacheKey, getCache(vCursor, resolvedShard, sequenceTableName));
                    }
                    value = CACHE_MAP.get(cacheKey).getAndIncrement();
                    if (value == -1) {
                        continue;
                    }
                    break;
                }
            } finally {
                LOCK_MAP.get(cacheKey).unlock();
            }
        }

        if (value < 0) {
            throw new SQLException("Sequence value overflow, value = " + value);
        }

        return value;
    }

    public List<Long> getSequences(Vcursor vCursor, ResolvedShard resolvedShard, String keyspace, String sequenceTableName, int count) throws SQLException {
        List<Long> sequences = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            try {
                sequences.add(nextValue(vCursor, resolvedShard, keyspace, sequenceTableName));
            } catch (SQLException | InterruptedException e) {
                throw new SQLException(
                    "failed to get sequences for keyspace:" + keyspace + ", tableName:" + sequenceTableName.substring(0, sequenceTableName.length() - 4) + ", errorMessage:" + e.getMessage());
            }
        }
        return sequences;
    }

    class Cache {
        private final AtomicLong value;

        private final long max;

        private volatile boolean isOver;

        Cache(final long start, final long limit) {
            this.value = new AtomicLong(start);
            this.max = start + limit;
            this.isOver = false;
        }

        public long getAndIncrement() throws SQLException {
            long next = value.getAndIncrement();
            if (next >= max) {
                isOver = true;
                return -1;
            }
            return next;
        }

        public boolean isOver() {
            return isOver;
        }
    }
}

