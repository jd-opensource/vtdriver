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

package com.jd.jdbc.pool;

import com.jd.jdbc.sqlparser.support.logging.Log;
import com.jd.jdbc.sqlparser.support.logging.LogFactory;
import java.sql.SQLException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import lombok.AllArgsConstructor;
import lombok.Data;

/*
 Numbered allows you to manage resources by tracking them with numbers.
 There are no interface restrictions on what you can track.
 */
public class Numbered {

    private static final Log logger = LogFactory.getLog(Numbered.class);

    ReentrantLock lock;

    Condition cond;

    Map<Long, NumberedWrapper> resources;

    public Numbered() {
        lock = new ReentrantLock();
        cond = lock.newCondition();
        resources = new HashMap<>();
    }

    // Register starts tracking a resource by the supplied id.
    // It does not lock the object.
    // It returns false if the id already exists.
    public boolean register(long id, StatefulConnection conn, boolean enforceTimeout) {

        NumberedWrapper resource = new NumberedWrapper(conn, false, "", Instant.now(), Instant.now(), enforceTimeout);
        lock.lock();
        try {
            if (resources.containsKey(id)) {
                return false;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("register conn: " + id + ", " + conn.getConn());
            }
            resources.put(id, resource);
            return true;
        } finally {
            lock.unlock();
        }
    }

    public void unregister(long id) {
        lock.lock();
        try {
            if (resources.containsKey(id)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("unregister conn: " + id + ", " + resources.get(id).getConn().getConn());
                }
                resources.remove(id);
            }

            if (resources.isEmpty()) {
                cond.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    public StatefulConnection get(long id, String purpose) throws SQLException {
        lock.lock();
        try {
            if (!resources.containsKey(id)) {
                throw new SQLException("id " + id + " not found");
            }

            NumberedWrapper resource = resources.get(id);
            if (resource.inUse) {
                throw new SQLException("id " + id + " is in use " + resource.purpose);
            }

            resource.inUse = true;
            resource.purpose = purpose;

            if (logger.isDebugEnabled()) {
                logger.debug("lock conn: " + id + ", for " + purpose);
            }

            return resource.conn;
        } finally {
            lock.unlock();
        }
    }

    // Put unlocks a resource for someone else to use.
    void put(long id, boolean updateTime) {
        lock.lock();
        try {
            NumberedWrapper resource = resources.getOrDefault(id, null);
            if (resource != null) {
                resource.inUse = false;
                resource.purpose = "";
                logger.debug("unlock conn: " + id);
                if (updateTime) {
                    resource.timeUsed = Instant.now();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Data
    @AllArgsConstructor
    static class NumberedWrapper {
        StatefulConnection conn;

        boolean inUse;

        String purpose;

        Instant timeCreated;

        Instant timeUsed;

        boolean enforceTimeout;
    }
}
