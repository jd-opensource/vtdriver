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

package com.jd.jdbc.engine;

import com.jd.jdbc.IExecute;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtRowList;
import io.vitess.proto.Query;
import java.sql.SQLException;

public class LimitStream implements IExecute.VtStream {
    private long queryCount = 0;

    private long queryOffset = 0;

    private IExecute.VtStream stream = null;

    private long offset = 0;

    private long count = 0;

    private Query.Field[] fields = null;

    public LimitStream(long queryCount, long queryOffset, IExecute.VtStream stream) {
        this.queryCount = queryCount;
        this.queryOffset = queryOffset;
        this.stream = stream;
    }

    private VtResultSet internalFetch(boolean wantFields) throws SQLException {
        VtResultSet fetched = (VtResultSet) stream.fetch(wantFields);
        if (fields == null) {
            fields = fetched.getFields();
        }
        return fetched;
    }

    @Override
    public VtRowList fetch(boolean wantFields) throws SQLException {
        VtResultSet vtResultSet = new VtResultSet();
        if (stream == null || count >= queryCount) {
            return vtResultSet;
        }

        while (offset < queryOffset) {
            VtResultSet fetched = internalFetch(wantFields);
            long fetchedRows = fetched.getRowsAffected();

            //no result
            if (fetchedRows == 0) {
                if (wantFields) {
                    vtResultSet.setFields(fields);
                }
                return vtResultSet;
            }

            //skip current result
            if (offset + fetchedRows <= queryOffset) {
                offset += fetchedRows;
                continue;
            }

            /*skip some rows ..
             case 1:
             offset       queryOffset
             |------------|-------------------------|
                          start                     end

             case 2:
             offset       queryOffset
             |------------|---------------|----------|
                          start            end
            */
            int start = (int) (queryOffset - offset);
            int end = (int) Math.min(start + queryCount, fetchedRows);
            VtResultSet tmp = new VtResultSet();
            tmp.setRows(fetched.getRows().subList(start, end));
            tmp.setRowsAffected(tmp.getRows().size());
            tmp.setFields(fetched.getFields());
            vtResultSet.appendResult(tmp);
            count += tmp.getRowsAffected();
            offset = queryOffset;

            return vtResultSet;
        }

        VtResultSet fetched = internalFetch(wantFields);
        long fetchedRows = fetched.getRowsAffected();

        //no result
        if (fetchedRows == 0) {
            if (wantFields) {
                vtResultSet.setFields(fields);
            }
            return vtResultSet;
        }

        /*return some rows ..
             case 1:
             |--------------------------------------|
             start                                  end

             case 2:
             |-----------------|---------------------|
             start             end
        */
        int end = (int) Math.min(queryCount - count, fetchedRows);
        VtResultSet tmp = new VtResultSet();
        tmp.setRows(fetched.getRows().subList(0, end));
        tmp.setRowsAffected(tmp.getRows().size());
        tmp.setFields(fetched.getFields());
        vtResultSet.appendResult(tmp);

        count += tmp.getRowsAffected();
        return vtResultSet;
    }

    @Override
    public void close() throws SQLException {
        if (stream != null) {
            stream.close();
            stream = null;
        }
    }
}
