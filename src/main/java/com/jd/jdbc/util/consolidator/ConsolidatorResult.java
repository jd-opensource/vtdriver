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

package com.jd.jdbc.util.consolidator;

import com.jd.jdbc.Executor;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ConsolidatorResult {
    private Result result;

    private ReentrantReadWriteLock executing;

    public ConsolidatorResult(Result result) {
        this.result = result;
        this.executing = new ReentrantReadWriteLock();
    }

    public void writeLockLock() {
        executing.writeLock().lock();
    }

    public void writeLockUnLock() {
        executing.writeLock().unlock();
    }

    public void readLockLock() {
        executing.readLock().lock();
    }

    public void readLockUnLock() {
        executing.readLock().unlock();
    }

    public void incrementQueryCounter() {
        result.getQueryCounter().incrementAndGet();
    }

    public void decrementQueryCounter() {
        result.getQueryCounter().decrementAndGet();
    }

    public SQLException getSQLException() {
        return result.getE();
    }

    public void setSQLException(SQLException sqlException) {
        result.setE(sqlException);
    }

    public int getQueryCount() {
        return result.getQueryCounter().get();
    }

    public Executor.ExecuteResponse getExecuteResponse() {
        return result.getExecuteResponse();
    }

    public void setExecuteResponse(Executor.ExecuteResponse executeResponse) {
        result.setExecuteResponse(executeResponse);
    }
}