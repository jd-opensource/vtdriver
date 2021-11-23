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

package com.jd.jdbc.sqltypes;

import lombok.Getter;

public enum VtSqlStatementType {
    /**
     *
     */
    StmtSelect(0),
    StmtStream(1),
    StmtInsert(2),
    StmtReplace(3),
    StmtUpdate(4),
    StmtDelete(5),
    StmtDDL(6),
    StmtBegin(7),
    StmtCommit(8),
    StmtRollback(9),
    StmtSet(10),
    StmtShow(11),
    StmtUse(12),
    StmtOther(13),
    StmtUnknown(14),
    StmtComment(15),
    StmtPriv(16),
    StmtExplain(17),
    StmtCreate(18),
    StmtAlter(19),
    StmtDrop(20),
    StmtTruncate(21),
    StmtRename(22),
    StmtSavepoint(23),
    StmtSRollback(24),
    StmtRelease(25),
    StmtPlan(26);

    @Getter
    private final Integer value;

    VtSqlStatementType(Integer value) {
        this.value = value;
    }
}
