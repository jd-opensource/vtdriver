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

package com.jd.jdbc.queryservice;

import com.google.protobuf.MessageOrBuilder;
import com.jd.jdbc.sqltypes.VtResultSetMessage;
import java.sql.SQLException;
import lombok.AllArgsConstructor;
import lombok.Data;

public interface IInner {
    InnerResult DEFAULT_RETRY = new InnerResult(true, null, null);

    InnerResult run(IQueryService qs) throws SQLException;

    @Data
    @AllArgsConstructor
    class InnerResult {
        boolean retry;

        MessageOrBuilder responses;

        VtResultSetMessage resultSetMessage;

        public InnerResult(boolean retry, MessageOrBuilder responses) {
            this.retry = retry;
            this.responses = responses;
        }

        public InnerResult(boolean retry, VtResultSetMessage resultSetMessage) {
            this.retry = retry;
            this.resultSetMessage = resultSetMessage;
        }
    }
}
