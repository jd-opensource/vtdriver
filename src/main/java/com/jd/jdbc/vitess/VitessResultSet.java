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

package com.jd.jdbc.vitess;

import com.jd.jdbc.sqlparser.utils.StringUtils;
import com.jd.jdbc.sqltypes.VtResultSet;
import com.jd.jdbc.sqltypes.VtResultValue;
import com.jd.jdbc.sqltypes.VtRowList;
import com.jd.jdbc.vitess.mysql.VitessPropertyKey;
import com.jd.jdbc.vitess.resultset.ResultSetUtil;
import com.mysql.cj.conf.PropertyDefinitions;
import io.vitess.proto.Query;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

public class VitessResultSet extends AbstractVitessResultSet {

    private final VtRowList vtRowList;

    private final VitessConnection vitessConnection;

    // column is based on 1
    private int lastReadColumn;

    private int currentPositionInEntireResult = -1;

    private List<VtResultValue> row;

    private boolean checkVtRowListType;

    private Map<String, Integer> columnLabelMap = null;

    private Map<String, Integer> fullColumnNameMap = null;

    private Map<String, Integer> columnNameMap = null;

    public VitessResultSet(final VtRowList result, final VitessConnection vitessConnection) throws SQLException {
        this.vtRowList = result;
        this.vitessConnection = vitessConnection;
        buildColumnMap(result.getFields());
        if (result instanceof VtResultSet) {
            checkVtRowListType = true;
        }
    }

    void checkRowColumn(final int column) throws SQLException {
        if (null == row || row.isEmpty()) {
            throw new SQLException("empty result set");
        }

        int cols = row.size();
        if (column < 1 || column > cols) {
            throw new SQLException("current column is out of range: " + column + ", total column: " + cols);
        }
    }

    @Override
    public boolean next() throws SQLException {
        if (this.vtRowList != null && this.vtRowList.hasNext()) {
            this.row = this.vtRowList.next();
            ++currentPositionInEntireResult;
            return true;
        }
        if (currentPositionInEntireResult < vtRowList.getResultSetSize()) {
            ++currentPositionInEntireResult;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        if (vtRowList != null) {
            vtRowList.close();
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        if (lastReadColumn == 0) {
            throw new SQLException("You should get something before check nullability");
        }

        checkRowColumn(lastReadColumn);

        return row.get(lastReadColumn - 1).isNull();
    }

    @Override
    public String getString(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        VtResultValue vtResultValue = row.get(lastReadColumn - 1);
        return (String) ResultSetUtil.convertValue(vtResultValue, String.class);
    }

    @Override
    public String getString(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getString(lastReadColumn);
    }

    @Override
    public boolean getBoolean(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        return (boolean) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), boolean.class);
    }

    @Override
    public boolean getBoolean(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getBoolean(lastReadColumn);
    }

    @Override
    public byte getByte(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        return (byte) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), byte.class);
    }

    @Override
    public byte getByte(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getByte(lastReadColumn);
    }

    @Override
    public short getShort(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        return (short) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), short.class);
    }

    @Override
    public short getShort(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getShort(lastReadColumn);
    }

    @Override
    public int getInt(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        return (int) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), int.class);
    }

    @Override
    public int getInt(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getInt(lastReadColumn);
    }

    @Override
    public long getLong(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        return (long) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), long.class);
    }

    @Override
    public long getLong(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getLong(lastReadColumn);
    }

    @Override
    public float getFloat(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        return (float) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), float.class);
    }

    @Override
    public float getFloat(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getFloat(lastReadColumn);
    }

    @Override
    public double getDouble(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        return (double) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), double.class);
    }

    @Override
    public double getDouble(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getDouble(lastReadColumn);
    }

    @Override
    public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
        return getBigDecimal(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getBigDecimal(lastReadColumn);
    }

    @Override
    public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        return (BigDecimal) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), BigDecimal.class);
    }

    @Override
    public BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getBigDecimal(lastReadColumn);
    }

    @Override
    public byte[] getBytes(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        return (byte[]) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), byte[].class);
    }

    @Override
    public byte[] getBytes(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getBytes(lastReadColumn);
    }

    @Override
    public Date getDate(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        TimeZone stz = getDefaultTimeZone();
        PropertyDefinitions.ZeroDatetimeBehavior zeroDateTimeBehavior = getZeroDatetimeBehavior();
        return (Date) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), Date.class, stz, zeroDateTimeBehavior);
    }

    @Override
    public Date getDate(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getDate(lastReadColumn);
    }

    @Override
    public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
        if (null == cal) {
            return getDate(columnIndex);
        }
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        TimeZone stz = cal.getTimeZone();
        PropertyDefinitions.ZeroDatetimeBehavior zeroDateTimeBehavior = getZeroDatetimeBehavior();
        return (Date) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), Date.class, stz, zeroDateTimeBehavior);
    }

    @Override
    public Date getDate(final String columnLabel, final Calendar cal) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getDate(lastReadColumn, cal);
    }

    @Override
    public Time getTime(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        TimeZone stz = getServerTimeZone();
        PropertyDefinitions.ZeroDatetimeBehavior zeroDateTimeBehavior = getZeroDatetimeBehavior();
        return (Time) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), Time.class, stz, zeroDateTimeBehavior);
    }

    @Override
    public Time getTime(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getTime(lastReadColumn);
    }

    @Override
    public Time getTime(final int columnIndex, final Calendar cal) throws SQLException {
        if (null == cal) {
            return getTime(columnIndex);
        }
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        TimeZone stz = cal.getTimeZone();
        PropertyDefinitions.ZeroDatetimeBehavior zeroDateTimeBehavior = getZeroDatetimeBehavior();
        return (Time) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), Time.class, stz, zeroDateTimeBehavior);
    }

    @Override
    public Time getTime(final String columnLabel, final Calendar cal) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getTime(lastReadColumn, cal);
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        TimeZone stz = getServerTimeZone();
        PropertyDefinitions.ZeroDatetimeBehavior zeroDateTimeBehavior = getZeroDatetimeBehavior();
        return (Timestamp) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), Timestamp.class, stz, zeroDateTimeBehavior);
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getTimestamp(lastReadColumn);
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {
        if (null == cal) {
            return getTimestamp(columnIndex);
        }
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        TimeZone stz = cal.getTimeZone();
        PropertyDefinitions.ZeroDatetimeBehavior zeroDateTimeBehavior = getZeroDatetimeBehavior();
        return (Timestamp) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), Timestamp.class, stz, zeroDateTimeBehavior);
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel, final Calendar cal) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getTimestamp(lastReadColumn, cal);
    }

    @Override
    public InputStream getBinaryStream(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        byte[] bytes = row.get(lastReadColumn - 1).toBytes();
        return bytes == null ? null : new ByteArrayInputStream(bytes);
    }

    @Override
    public InputStream getBinaryStream(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getBinaryStream(lastReadColumn);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return new VitessResultSetMetaData(vtRowList);
    }

    @Override
    public Object getObject(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        VtResultValue vtResultValue = row.get(lastReadColumn - 1);
        if (vtResultValue.getVtType() == Query.Type.BIT && vtResultValue.getValue() instanceof Integer) {
            return getBoolean(lastReadColumn);
        } else if (vtResultValue.getVtType() == Query.Type.BIT && vtResultValue.getValue() instanceof Long) {
            return getBytes(lastReadColumn);
        } else if (vtResultValue.getVtType() == Query.Type.TIME) {
            return getTime(lastReadColumn);
        } else if (vtResultValue.getVtType() == Query.Type.TIMESTAMP || vtResultValue.getVtType() == Query.Type.DATETIME) {
            return getTimestamp(lastReadColumn);
        } else if (vtResultValue.getVtType() == Query.Type.DATE || vtResultValue.getVtType() == Query.Type.YEAR) {
            return getDate(lastReadColumn);
        }
        return vtResultValue.getValue();
    }

    @Override
    public Object getObject(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getObject(lastReadColumn);
    }

    @Override
    public Object getObject(final int columnIndex, final Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Object getObject(final String columnLabel, final Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    @Override
    public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        if (type == Date.class) {
            TimeZone stz = getDefaultTimeZone();
            PropertyDefinitions.ZeroDatetimeBehavior zeroDateTimeBehavior = getZeroDatetimeBehavior();
            return (T) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), type, stz, zeroDateTimeBehavior);
        } else if (type == Time.class
                || type == Timestamp.class
                || type == LocalDate.class
                || type == LocalTime.class
                || type == LocalDateTime.class) {
            TimeZone stz = getServerTimeZone();
            PropertyDefinitions.ZeroDatetimeBehavior zeroDateTimeBehavior = getZeroDatetimeBehavior();
            return (T) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), type, stz, zeroDateTimeBehavior);
        }
        return (T) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), type);
    }

    @Override
    public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getObject(lastReadColumn, type);
    }

    @Override
    public int findColumn(final String columnLabel) throws SQLException {
        int index = getIndexInMap(columnLabel);
        if (-1 == index) {
            throw new SQLException("Wrong column label: " + columnLabel);
        }
        return index;
    }

    @Override
    public Reader getCharacterStream(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        return (InputStreamReader) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), InputStreamReader.class);
    }

    @Override
    public Reader getCharacterStream(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getCharacterStream(lastReadColumn);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        if (checkVtRowListType) {
            return this.currentPositionInEntireResult < 0;
        }
        throw new SQLFeatureNotSupportedException("Streaming results are not supported");
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        if (checkVtRowListType) {
            return this.currentPositionInEntireResult >= vtRowList.getResultSetSize();
        }
        throw new SQLFeatureNotSupportedException("Streaming results are not supported");
    }

    @Override
    public boolean isFirst() throws SQLException {
        if (checkVtRowListType) {
            return this.currentPositionInEntireResult == 0;
        }
        throw new SQLFeatureNotSupportedException("Streaming results are not supported");
    }

    @Override
    public boolean isLast() throws SQLException {
        if (checkVtRowListType) {
            return this.currentPositionInEntireResult == (vtRowList.getResultSetSize() - 1);
        }
        throw new SQLFeatureNotSupportedException("Streaming results are not supported");
    }

    @Override
    public int getRow() throws SQLException {
        return this.currentPositionInEntireResult + 1;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }

    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    public Blob getBlob(final int columnIndex) throws SQLException {
        lastReadColumn = columnIndex;
        return (Blob) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), Blob.class);
    }

    @Override
    public Blob getBlob(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getBlob(lastReadColumn);
    }

    @Override
    public Clob getClob(final int columnIndex) throws SQLException {
        checkRowColumn(columnIndex);
        lastReadColumn = columnIndex;
        return (Clob) ResultSetUtil.convertValue(row.get(lastReadColumn - 1), Clob.class);
    }

    @Override
    public Clob getClob(final String columnLabel) throws SQLException {
        lastReadColumn = findColumn(columnLabel);
        return getClob(lastReadColumn);
    }

    @Override
    public int getHoldability() throws SQLException {
        return HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return vtRowList != null && vtRowList.isClosed();
    }

    private PropertyDefinitions.ZeroDatetimeBehavior getZeroDatetimeBehavior() {
        String zeroDatetimeBehavior = vitessConnection.getProperties().getProperty(VitessPropertyKey.ZERO_DATE_TIME_BEHAVIOR.getKeyName());
        if (zeroDatetimeBehavior == null) {
            return PropertyDefinitions.ZeroDatetimeBehavior.EXCEPTION;
        }
        return PropertyDefinitions.ZeroDatetimeBehavior.valueOf(zeroDatetimeBehavior);
    }

    private TimeZone getServerTimeZone() {
        return (TimeZone) vitessConnection.getServerSessionPropertiesMap().get(VitessPropertyKey.SERVER_TIMEZONE.getKeyName());
    }

    private TimeZone getDefaultTimeZone() {
        return (TimeZone) vitessConnection.getServerSessionPropertiesMap().get("DEFAULT_TIME_ZONE");
    }

    private void buildColumnMap(Query.Field[] fields) {
        if (fields == null) {
            return;
        }
        columnLabelMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        columnNameMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        fullColumnNameMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (int i = fields.length - 1; i >= 0; i--) {
            Query.Field field = fields[i];
            int index = i + 1;
            if (!StringUtils.isEmpty(field.getName())) {
                columnLabelMap.put(field.getName(), index);
            }
            if (!StringUtils.isEmpty(field.getOrgName())) {
                columnNameMap.put(field.getOrgName(), index);
            }
            if (!StringUtils.isEmpty(field.getTable()) && !StringUtils.isEmpty(field.getName())) {
                String key = field.getTable() + "." + field.getName();
                fullColumnNameMap.put(key, index);
            }
        }
    }

    private int getIndexInMap(final String columnLabel) {
        Integer index = columnLabelMap.get(columnLabel);
        if (index == null) {
            index = columnNameMap.get(columnLabel);
        }
        if (index == null) {
            index = fullColumnNameMap.get(columnLabel);
        }
        if (index != null) {
            return index;
        }
        return -1;
    }
}
