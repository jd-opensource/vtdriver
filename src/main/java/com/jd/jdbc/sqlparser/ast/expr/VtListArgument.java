/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jd.jdbc.sqlparser.ast.expr;

import com.jd.jdbc.sqlparser.visitor.SQLASTVisitor;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;


@Getter
@Setter
public class VtListArgument extends SQLListExpr {
    private byte[] value;

    public VtListArgument(byte[] value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof VtListArgument)) {
            return false;
        }

        VtListArgument that = (VtListArgument) o;

        return Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
    }
}
