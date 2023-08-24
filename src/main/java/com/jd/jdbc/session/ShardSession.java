/*
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

package com.jd.jdbc.session;

import io.vitess.proto.Query;
import io.vitess.proto.Topodata;
import lombok.Getter;
import lombok.Setter;

public final class ShardSession {
    @Setter
    private long reservedId;

    private long transactionId;

    @Getter
    private Query.Target target;

    @Getter
    private Topodata.TabletAlias tabletAlias;

    public ShardSession(long reservedId, long transactionId, Query.Target target, Topodata.TabletAlias tabletAlias) {
        this.reservedId = reservedId;
        this.transactionId = transactionId;
        this.target = target;
        this.tabletAlias = tabletAlias;
    }

    public long getReservedId() {
        return reservedId;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void clearTransactionId() {
        this.transactionId = 0L;
    }

    public void clearReservedId() {
        this.reservedId = 0L;
    }
}