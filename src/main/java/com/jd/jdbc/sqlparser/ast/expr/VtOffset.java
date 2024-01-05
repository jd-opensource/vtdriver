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

package com.jd.jdbc.sqlparser.ast.expr;

import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLExprImpl;
import com.jd.jdbc.sqlparser.dialect.mysql.visitor.MySqlOutputVisitor;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqlparser.visitor.SQLASTVisitor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class VtOffset extends SQLExprImpl {

    private int value;

    private String original = "";

    public VtOffset() {
    }

    public VtOffset(int value, String origin) {
        this.value = value;
        this.original = origin;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VtOffset)) {
            return false;
        }

        VtOffset that = (VtOffset) o;

        return (this.value == that.value) && (this.original.equals(that.original));
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(value) + original.hashCode();
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor instanceof MySqlOutputVisitor) {
            MySqlOutputVisitor mySqlOutputVisitor = (MySqlOutputVisitor) visitor;
            mySqlOutputVisitor.print(this.toString());
        }
    }

    @Override
    public SQLExpr clone() {
        VtOffset x = new VtOffset(value, original);
        x.setParent(this.parent);
        return x;
    }

    @Override
    public String toString() {
        if (StringUtils.isEmpty(this.original)) {
            return "OFFSET(" + value + ")";
        } else {
            return "OFFSET(" + value + ", '" + original + "')";
        }
    }
}
