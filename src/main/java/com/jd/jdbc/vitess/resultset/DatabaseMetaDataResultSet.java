/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.jd.jdbc.vitess.resultset;

import com.jd.jdbc.util.SchemaUtil;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class DatabaseMetaDataResultSet extends AbstractDatabaseMetaDataResultSet {

    private static final String TABLE_CAT = "TABLE_CAT";

    private final int type;

    private final int concurrency;

    private final ResultSetMetaData resultSetMetaData;

    private final Map<String, Integer> indexMap;

    private final Iterator<DatabaseMetaDataObject> iterator;

    private final ResultSet resultSet;

    private volatile boolean closed;

    private DatabaseMetaDataObject currentDatabaseMetaDataObject;

    public DatabaseMetaDataResultSet(final ResultSet resultSet) throws SQLException {
        this.resultSet = resultSet;
        type = resultSet.getType();
        concurrency = resultSet.getConcurrency();
        resultSetMetaData = resultSet.getMetaData();
        indexMap = initIndexMap();
        iterator = initIterator(resultSet);
    }

    private Map<String, Integer> initIndexMap() throws SQLException {
        Map<String, Integer> result = new HashMap<>(resultSetMetaData.getColumnCount());
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            result.put(resultSetMetaData.getColumnLabel(i), i);
        }
        return result;
    }

    private Iterator<DatabaseMetaDataObject> initIterator(final ResultSet resultSet) throws SQLException {
        Collection<DatabaseMetaDataObject> result = new LinkedList<>();
        int tableCatIndex = indexMap.getOrDefault(TABLE_CAT, -1);
        while (resultSet.next()) {
            DatabaseMetaDataObject databaseMetaDataObject = generateDatabaseMetaDataObject(tableCatIndex, resultSet);
            if (databaseMetaDataObject != null) {
                result.add(databaseMetaDataObject);
            }
        }
        return result.iterator();
    }

    private DatabaseMetaDataObject generateDatabaseMetaDataObject(final int tableCatIndex, final ResultSet resultSet) throws SQLException {
        DatabaseMetaDataObject result = new DatabaseMetaDataObject(resultSetMetaData.getColumnCount());
        for (int i = 1; i <= indexMap.size(); i++) {
            if (tableCatIndex == i) {
                String tableCat = resultSet.getString(i);
                tableCat = SchemaUtil.getLogicSchema(tableCat);
                if (tableCat.equals("_vt")) {
                    return null;
                }
                result.addObject(tableCat);
            } else {
                result.addObject(resultSet.getObject(i));
            }
        }
        return result;
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        if (iterator.hasNext()) {
            currentDatabaseMetaDataObject = iterator.next();
            return true;
        }
        return false;
    }

    @Override
    public void close() throws SQLException {
        checkClosed();
        closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
        return getBigDecimal(columnIndex, true, scale);
    }

    @Override
    public BigDecimal getBigDecimal(final String columnLabel, final int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
        return getBigDecimal(columnIndex, false, 0);
    }

    private BigDecimal getBigDecimal(final int columnIndex, final boolean needScale, final int scale) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);
        return (BigDecimal) MetaDataResultSetUtil.convertBigDecimalValue(currentDatabaseMetaDataObject.getObject(columnIndex), needScale, scale);
    }

    @Override
    public BigDecimal getBigDecimal(final String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public String getString(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);
        return (String) MetaDataResultSetUtil.convertValue(currentDatabaseMetaDataObject.getObject(columnIndex), String.class);
    }

    @Override
    public String getString(final String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public String getNString(final int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(final String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    public boolean getBoolean(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);
        return (boolean) MetaDataResultSetUtil.convertValue(currentDatabaseMetaDataObject.getObject(columnIndex), boolean.class);
    }

    @Override
    public boolean getBoolean(final String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);
        return (byte) MetaDataResultSetUtil.convertValue(currentDatabaseMetaDataObject.getObject(columnIndex), byte.class);
    }

    @Override
    public byte getByte(final String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);
        return (short) MetaDataResultSetUtil.convertValue(currentDatabaseMetaDataObject.getObject(columnIndex), short.class);
    }

    @Override
    public short getShort(final String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);
        return (int) MetaDataResultSetUtil.convertValue(currentDatabaseMetaDataObject.getObject(columnIndex), int.class);
    }

    @Override
    public int getInt(final String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);
        return (long) MetaDataResultSetUtil.convertValue(currentDatabaseMetaDataObject.getObject(columnIndex), long.class);
    }

    @Override
    public long getLong(final String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);
        return (float) MetaDataResultSetUtil.convertValue(currentDatabaseMetaDataObject.getObject(columnIndex), float.class);
    }

    @Override
    public float getFloat(final String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);
        return (double) MetaDataResultSetUtil.convertValue(currentDatabaseMetaDataObject.getObject(columnIndex), double.class);
    }

    @Override
    public double getDouble(final String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public byte[] getBytes(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);
        return (byte[]) MetaDataResultSetUtil.convertValue(currentDatabaseMetaDataObject.getObject(columnIndex), byte[].class);
    }

    @Override
    public byte[] getBytes(final String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);
        return (Date) MetaDataResultSetUtil.convertValue(currentDatabaseMetaDataObject.getObject(columnIndex), Date.class);
    }

    @Override
    public Date getDate(final String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);
        return (Time) MetaDataResultSetUtil.convertValue(currentDatabaseMetaDataObject.getObject(columnIndex), Time.class);
    }

    @Override
    public Time getTime(final String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);
        return (Timestamp) MetaDataResultSetUtil.convertValue(currentDatabaseMetaDataObject.getObject(columnIndex), Timestamp.class);
    }

    @Override
    public Timestamp getTimestamp(final String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public URL getURL(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);
        return (URL) MetaDataResultSetUtil.convertValue(currentDatabaseMetaDataObject.getObject(columnIndex), URL.class);
    }

    @Override
    public URL getURL(final String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return resultSetMetaData;
    }

    @Override
    public Object getObject(final int columnIndex) throws SQLException {
        checkClosed();
        checkColumnIndex(columnIndex);
        return currentDatabaseMetaDataObject.getObject(columnIndex);
    }

    @Override
    public Object getObject(final String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public int findColumn(final String columnLabel) throws SQLException {
        checkClosed();
        if (!indexMap.containsKey(columnLabel)) {
            throw new SQLException("Can not find columnLabel" + columnLabel);
        }
        return indexMap.get(columnLabel);
    }

    @Override
    public int getType() throws SQLException {
        checkClosed();
        return type;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkClosed();
        return concurrency;
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return resultSet.getFetchDirection();
    }

    @Override
    public void setFetchDirection(final int direction) throws SQLException {
        resultSet.setFetchDirection(direction);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return resultSet.getFetchSize();
    }

    @Override
    public void setFetchSize(final int rows) throws SQLException {
        resultSet.setFetchSize(rows);
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet has closed.");
        }
    }

    private void checkColumnIndex(final int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > resultSetMetaData.getColumnCount()) {
            throw new SQLException(String.format("ColumnIndex %d out of range from %d to %d", columnIndex, 1, resultSetMetaData.getColumnCount()));
        }
    }

    private static final class DatabaseMetaDataObject {

        private final List<Object> objects;

        private DatabaseMetaDataObject(final int columnCount) {
            objects = new ArrayList<>(columnCount);
        }

        public void addObject(final Object object) {
            objects.add(object);
        }

        public Object getObject(final int index) {
            return objects.get(index - 1);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DatabaseMetaDataObject that = (DatabaseMetaDataObject) o;
            return Objects.equals(objects, that.objects);
        }

        @Override
        public int hashCode() {
            return Objects.hash(objects);
        }
    }
}
