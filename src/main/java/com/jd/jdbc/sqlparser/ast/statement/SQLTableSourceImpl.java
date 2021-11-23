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

package com.jd.jdbc.sqlparser.ast.statement;

import com.jd.jdbc.sqlparser.SQLUtils;
import com.jd.jdbc.sqlparser.ast.SQLExpr;
import com.jd.jdbc.sqlparser.ast.SQLHint;
import com.jd.jdbc.sqlparser.ast.SQLObjectImpl;
import com.jd.jdbc.sqlparser.utils.FnvHash;

import java.util.ArrayList;
import java.util.List;

public abstract class SQLTableSourceImpl extends SQLObjectImpl implements SQLTableSource {
    protected String alias;
    protected List<SQLHint> hints;
    protected SQLExpr flashback;
    protected long aliasHashCod64;

    public SQLTableSourceImpl() {

    }

    public SQLTableSourceImpl(String alias) {
        this.alias = alias;
    }

    @Override
    public String getAlias() {
        return this.alias;
    }

    @Override
    public void setAlias(String alias) {
        this.alias = alias;
        this.aliasHashCod64 = 0L;
    }

    public int getHintsSize() {
        if (hints == null) {
            return 0;
        }

        return hints.size();
    }

    @Override
    public List<SQLHint> getHints() {
        if (hints == null) {
            hints = new ArrayList<SQLHint>(2);
        }
        return hints;
    }

    public void setHints(List<SQLHint> hints) {
        this.hints = hints;
    }

    public SQLTableSource clone() {
        throw new UnsupportedOperationException(this.getClass().getName());
    }

    @Override
    public String computeAlias() {
        return alias;
    }

    @Override
    public SQLExpr getFlashback() {
        return flashback;
    }

    @Override
    public void setFlashback(SQLExpr flashback) {
        if (flashback != null) {
            flashback.setParent(this);
        }
        this.flashback = flashback;
    }

    @Override
    public boolean containsAlias(String alias) {
        if (SQLUtils.nameEquals(this.alias, alias)) {
            return true;
        }

        return false;
    }

    @Override
    public long aliasHashCode64() {
        if (aliasHashCod64 == 0
                && alias != null) {
            aliasHashCod64 = FnvHash.hashCode64(alias);
        }
        return aliasHashCod64;
    }

    @Override
    public SQLColumnDefinition findColumn(String columnName) {
        if (columnName == null) {
            return null;
        }

        long hash = FnvHash.hashCode64(alias);
        return findColumn(hash);
    }

    @Override
    public SQLColumnDefinition findColumn(long columnNameHash) {
        return null;
    }

    @Override
    public SQLTableSource findTableSourceWithColumn(String columnName) {
        if (columnName == null) {
            return null;
        }

        long hash = FnvHash.hashCode64(alias);
        return findTableSourceWithColumn(hash);
    }

    @Override
    public SQLTableSource findTableSourceWithColumn(long columnNameHash) {
        return null;
    }

    @Override
    public SQLTableSource findTableSource(String alias) {
        long hash = FnvHash.hashCode64(alias);
        return findTableSource(hash);
    }

    public SQLTableSource findTableSource(long alias_hash) {
        long hash = this.aliasHashCode64();
        if (hash != 0 && hash == alias_hash) {
            return this;
        }
        return null;
    }
}
