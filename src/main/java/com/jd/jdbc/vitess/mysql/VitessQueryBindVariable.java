/*
Copyright 2021 JD Project Authors.

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

package com.jd.jdbc.vitess.mysql;

import com.google.protobuf.ByteString;
import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqltypes.SqlTypes;
import com.mysql.cj.ClientPreparedQueryBindValue;
import com.mysql.cj.ClientPreparedQueryBindings;
import com.mysql.cj.Session;
import io.vitess.proto.Query;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;

public class VitessQueryBindVariable {
    private final ClientPreparedQueryBindings inner;

    private final String charEncoding;

    public VitessQueryBindVariable(int parameterCount, Session sess, String encoding) {
        this.inner = new ClientPreparedQueryBindings(parameterCount, sess);
        this.charEncoding = encoding;
    }

    public Map<String, Query.BindVariable> getBindVariableMap() {
        Map<String, Query.BindVariable> bindVariableMap = new LinkedHashMap<>();

        ClientPreparedQueryBindValue[] bindValues = this.inner.getBindValues();
        for (int i = 0; i < bindValues.length; i++) {
            bindVariableMap.put(String.valueOf(i), (Query.BindVariable) bindValues[i].value);
        }

        return bindVariableMap;
    }

    public void clearBindValues() {
        this.inner.clearBindValues();
    }

    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        this.inner.setAsciiStream(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.BLOB);
    }

    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.inner.setAsciiStream(parameterIndex, x, length);
        this.convertToBindVariable(parameterIndex, Query.Type.TEXT);
    }

    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        this.inner.setBigDecimal(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.DECIMAL);
    }

    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        this.inner.setBinaryStream(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.BLOB);
    }

    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        this.inner.setBinaryStream(parameterIndex, x, length);
        this.convertToBindVariable(parameterIndex, Query.Type.BLOB);
    }

    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        this.inner.setBlob(parameterIndex, inputStream);
        this.convertToBindVariable(parameterIndex, Query.Type.BLOB);
    }

    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        this.inner.setBlob(parameterIndex, inputStream, length);
        this.convertToBindVariable(parameterIndex, Query.Type.BLOB);
    }

    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        this.inner.setBlob(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.BLOB);
    }

    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        this.inner.setBoolean(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.INT8);
    }

    public void setByte(int parameterIndex, byte x) throws SQLException {
        this.inner.setByte(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.INT8);
    }

    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        this.inner.setBytes(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.BINARY, x);
    }

    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        this.inner.setCharacterStream(parameterIndex, reader);
        this.convertToBindVariable(parameterIndex, Query.Type.TEXT);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        this.inner.setCharacterStream(parameterIndex, reader, length);
        this.convertToBindVariable(parameterIndex, Query.Type.TEXT);
    }

    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        this.inner.setCharacterStream(parameterIndex, reader, length);
        this.convertToBindVariable(parameterIndex, Query.Type.TEXT);
    }

    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        this.inner.setClob(parameterIndex, reader);
        this.convertToBindVariable(parameterIndex, Query.Type.TEXT);
    }

    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.inner.setClob(parameterIndex, reader, length);
        this.convertToBindVariable(parameterIndex, Query.Type.TEXT);
    }

    public void setClob(int parameterIndex, Clob x) throws SQLException {
        this.inner.setClob(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.TEXT);
    }

    public void setDate(int parameterIndex, Date x) throws SQLException {
        this.inner.setDate(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.DATE);
    }

    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        this.inner.setDate(parameterIndex, x, cal);
        this.convertToBindVariable(parameterIndex, Query.Type.DATE);
    }

    public void setDouble(int parameterIndex, double x) throws SQLException {
        this.inner.setDouble(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.FLOAT64);
    }

    public void setFloat(int parameterIndex, float x) throws SQLException {
        this.inner.setFloat(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.FLOAT32);
    }

    public void setInt(int parameterIndex, int x) throws SQLException {
        this.inner.setInt(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.INT32);
    }

    public void setLong(int parameterIndex, long x) throws SQLException {
        this.inner.setLong(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.INT64);
    }

    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        this.inner.setNCharacterStream(parameterIndex, value);
        this.convertToBindVariable(parameterIndex, Query.Type.TEXT);
    }

    public void setNCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        this.inner.setNCharacterStream(parameterIndex, reader, length);
        this.convertToBindVariable(parameterIndex, Query.Type.TEXT);
    }

    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        this.inner.setNClob(parameterIndex, reader);
        this.convertToBindVariable(parameterIndex, Query.Type.TEXT);
    }

    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        this.inner.setNClob(parameterIndex, reader, length);
        this.convertToBindVariable(parameterIndex, Query.Type.TEXT);
    }

    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        this.inner.setNClob(parameterIndex, value);
        this.convertToBindVariable(parameterIndex, Query.Type.TEXT);
    }

    public void setNString(int parameterIndex, String x) throws SQLException {
        this.inner.setNString(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.VARCHAR);
    }

    public synchronized void setNull(int parameterIndex) throws SQLException {
        this.inner.setNull(parameterIndex);
        this.convertToBindVariable(parameterIndex, Query.Type.NULL_TYPE);
    }

    public void setShort(int parameterIndex, short x) throws SQLException {
        this.inner.setShort(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.INT16);
    }

    public void setString(int parameterIndex, String x) throws SQLException {
        if (x == null) {
            this.setNull(parameterIndex);
        } else {
            byte[] bytes = StringUtils.getBytes(x, charEncoding);
            this.convertToBindVariable(parameterIndex, Query.Type.VARBINARY, bytes);
        }
    }

    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        this.inner.setTime(parameterIndex, x, cal);
        this.convertToBindVariable(parameterIndex, Query.Type.TIME);
    }

    public void setTime(int parameterIndex, Time x) throws SQLException {
        this.inner.setTime(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.TIME);
    }

    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        this.inner.setTimestamp(parameterIndex, x);
        this.convertToBindVariable(parameterIndex, Query.Type.TIMESTAMP);
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        this.inner.setTimestamp(parameterIndex, x, cal);
        this.convertToBindVariable(parameterIndex, Query.Type.TIMESTAMP);
    }

    public void setTimestamp(int parameterIndex, Timestamp x, Calendar targetCalendar, int fractionalLength) throws SQLException {
        this.inner.setTimestamp(parameterIndex, x, targetCalendar, fractionalLength);
        this.convertToBindVariable(parameterIndex, Query.Type.TIMESTAMP);
    }

    private void convertToBindVariable(int parameterIndex, Query.Type type) throws SQLException {
        this.convertToBindVariable(parameterIndex, type, null);
    }

    private void convertToBindVariable(int parameterIndex, Query.Type type, byte[] bytes) throws SQLException {
        ClientPreparedQueryBindValue bindValue = this.inner.getBindValues()[parameterIndex];
        if (bindValue.isNull()) {
            bindValue.value = SqlTypes.NULL_BIND_VARIABLE;
            return;
        }

        Query.BindVariable.Builder builder = Query.BindVariable.newBuilder().setType(type);
        Query.BindVariable bindVariable;
        switch (type) {
            case NULL_TYPE:
                bindVariable = builder.build();
                break;
            case BLOB:
            case TEXT:
                bindVariable = builder.setValue(ByteString.copyFrom(this.inner.getBytesRepresentation(parameterIndex))).build();
                break;
            case BINARY:
            case VARBINARY:
                bindVariable = builder.setValue(ByteString.copyFrom(bytes)).build();
                break;
            case DATE:
            case TIME:
            case TIMESTAMP:
                // DATE、TIME、TIMESTAMP类型需去除mysql驱动添加的单引号'
                byte[] bytes1 = Arrays.copyOfRange(bindValue.getByteValue(), 1, bindValue.getByteValue().length - 1);
                bindVariable = builder.setValue(ByteString.copyFrom(bytes1)).build();
                break;
            default:
                bindVariable = builder.setValue(ByteString.copyFrom(bindValue.getByteValue())).build();
                break;
        }
        bindValue.value = bindVariable;
    }
}
