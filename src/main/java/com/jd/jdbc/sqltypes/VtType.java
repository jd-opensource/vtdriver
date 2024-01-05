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

import io.vitess.proto.Query;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import static java.sql.Types.LONGVARBINARY;
import java.util.HashMap;
import java.util.Map;

public class VtType {
    private static final int flagIsIntegral = Query.Flag.ISINTEGRAL_VALUE;

    private static final int flagIsUnsigned = Query.Flag.ISUNSIGNED_VALUE;

    private static final int flagIsFloat = Query.Flag.ISFLOAT_VALUE;

    private static final int flagIsQuoted = Query.Flag.ISQUOTED_VALUE;

    private static final int flagIsText = Query.Flag.ISTEXT_VALUE;

    private static final int flagIsBinary = Query.Flag.ISBINARY_VALUE;

    private static final Map<String, Query.Type> TYPE_MAP = new HashMap<>();

    static {
        TYPE_MAP.put("NULL", Query.Type.NULL_TYPE);
        TYPE_MAP.put("TINYINT", Query.Type.INT8);
        TYPE_MAP.put("TINYINT UNSIGNED", Query.Type.UINT8);
        TYPE_MAP.put("SMALLINT", Query.Type.INT16);
        TYPE_MAP.put("SMALLINT UNSIGNED", Query.Type.UINT16);
        TYPE_MAP.put("MEDIUMINT", Query.Type.INT24);
        TYPE_MAP.put("MEDIUMINT UNSIGNED", Query.Type.UINT24);
        TYPE_MAP.put("INT", Query.Type.INT32);
        TYPE_MAP.put("INT UNSIGNED", Query.Type.UINT32);
        TYPE_MAP.put("BIGINT", Query.Type.INT64);
        TYPE_MAP.put("BIGINT UNSIGNED", Query.Type.UINT64);
        TYPE_MAP.put("FLOAT", Query.Type.FLOAT32);
        TYPE_MAP.put("FLOAT UNSIGNED", Query.Type.FLOAT32);
        TYPE_MAP.put("DOUBLE", Query.Type.FLOAT64);
        TYPE_MAP.put("DOUBLE UNSIGNED", Query.Type.FLOAT64);
        TYPE_MAP.put("TIMESTAMP", Query.Type.TIMESTAMP);
        TYPE_MAP.put("DATE", Query.Type.DATE);
        TYPE_MAP.put("TIME", Query.Type.TIME);
        TYPE_MAP.put("DATETIME", Query.Type.DATETIME);
        TYPE_MAP.put("YEAR", Query.Type.YEAR);
        TYPE_MAP.put("DECIMAL", Query.Type.DECIMAL);
        TYPE_MAP.put("DECIMAL UNSIGNED", Query.Type.DECIMAL);
        TYPE_MAP.put("TINYTEXT", Query.Type.TEXT);
        TYPE_MAP.put("TEXT", Query.Type.TEXT);
        TYPE_MAP.put("MEDIUMTEXT", Query.Type.TEXT);
        TYPE_MAP.put("LONGTEXT", Query.Type.TEXT);
        TYPE_MAP.put("TINYBLOB", Query.Type.BLOB);
        TYPE_MAP.put("BLOB", Query.Type.BLOB);
        TYPE_MAP.put("MEDIUMBLOB", Query.Type.BLOB);
        TYPE_MAP.put("LONGBLOB", Query.Type.BLOB);
        TYPE_MAP.put("VARCHAR", Query.Type.VARCHAR);
        TYPE_MAP.put("VARBINARY", Query.Type.VARBINARY);
        TYPE_MAP.put("CHAR", Query.Type.CHAR);
        TYPE_MAP.put("BINARY", Query.Type.BINARY);
        TYPE_MAP.put("BIT", Query.Type.BIT);
        TYPE_MAP.put("BOOLEAN", Query.Type.BIT);
        TYPE_MAP.put("ENUM", Query.Type.ENUM);
        TYPE_MAP.put("SET", Query.Type.SET);
        TYPE_MAP.put("GEOMETRY", Query.Type.GEOMETRY);
        TYPE_MAP.put("JSON", Query.Type.JSON);
    }

    public static boolean isIntegral(Query.Type typ) {
        return (typ.getNumber() & flagIsIntegral) == flagIsIntegral;
    }

    public static boolean isSigned(Query.Type typ) {
        return (typ.getNumber() & (flagIsIntegral | flagIsUnsigned)) == flagIsIntegral;
    }

    public static boolean isUnsigned(Query.Type typ) {
        return (typ.getNumber() & (flagIsIntegral | flagIsUnsigned)) == (flagIsIntegral | flagIsUnsigned);
    }

    public static boolean isFloat(Query.Type typ) {
        return (typ.getNumber() & flagIsFloat) == flagIsFloat;
    }

    public static boolean isQuoted(Query.Type typ) {
        return (typ.getNumber() & flagIsQuoted) == flagIsQuoted && typ.getNumber() != Query.Type.BIT_VALUE;
    }

    public static boolean isText(Query.Type typ) {
        return (typ.getNumber() & flagIsText) == flagIsText;
    }

    public static boolean isBinary(Query.Type typ) {
        return (typ.getNumber() & flagIsBinary) == flagIsBinary;
    }

    public static boolean isNumber(Query.Type typ) {
        return isIntegral(typ) || isFloat(typ) || typ.getNumber() == Query.Type.DECIMAL_VALUE;
    }

    public static boolean isDate(Query.Type t) {
        return t == Query.Type.DATETIME || t == Query.Type.DATE || t == Query.Type.TIMESTAMP || t == Query.Type.TIME;
    }

    public static Query.Type getQueryType(String columnTypeName) throws SQLException {
        Query.Type type = TYPE_MAP.get(columnTypeName);
        if (type == null) {
            throw new SQLException("unsupported type, columnTypeName= " + columnTypeName);
        }
        return type;
    }

    public enum DataTypeConverter {
        NULL_TYPE("NULL", Types.NULL, Object.class, false, 0, 0),

        BIT("BIT", Types.BIT, Boolean.class, false, 0, 0),

        INT8("TINYINT", Types.TINYINT, Byte.class, true, 4, 0,
            "TINYINT"),
        UINT8("TINYINT UNSIGNED", Types.TINYINT, Short.class, false, 3, 0),
        INT16("SMALLINT", Types.SMALLINT, Short.class, true, 6, 0,
            "SMALLINT"),
        UINT16("SMALLINT UNSIGNED", Types.SMALLINT, Integer.class, false, 5, 0),
        INT24("MEDIUMINT", Types.INTEGER, Integer.class, false, 5, 0),
        UINT24("MEDIUMINT UNSIGNED", Types.INTEGER, Integer.class, false, 5, 0),
        INT32("INT", Types.INTEGER, Integer.class, true, 11, 0,
            "INTEGER",
            "INT"),
        UINT32("INT UNSIGNED", Types.INTEGER, Long.class, false, 10, 0),
        INT64("BIGINT", Types.BIGINT, Long.class, true, 20, 0,
            "BIGINT"),
        UINT64("BIGINT UNSIGNED", Types.BIGINT, BigInteger.class, false, 19, 0),
        FLOAT32("FLOAT", Types.REAL, Float.class, false, 8, 8,
            "FLOAT"),
        FLOAT64("DOUBLE", Types.DOUBLE, Double.class, true, 17, 17,
            "DOUBLE"),
        TIMESTAMP("TIMESTAMP", Types.TIMESTAMP, Timestamp.class, false, 19, 0,
            "TIMESTAMP"),
        DATE("DATE", Types.DATE, java.sql.Date.class, false, 10, 0),
        TIME("TIME", Types.TIME, java.sql.Timestamp.class, false, 19, 0),
        YEAR("YEAR", Types.DATE, Date.class, false, 0, 0),
        DATETIME("DATETIME", Types.TIMESTAMP, Timestamp.class, false, 19, 0,
            "TIMESTAMP"),
        DECIMAL("DECIMAL", Types.DECIMAL, BigDecimal.class, true, 0, 0,
            "DEC"),
        CHAR("CHAR", Types.CHAR, char.class, true, 0, 0),
        TEXT("TEXT", Types.LONGVARCHAR, String.class, true, 0, 0, "MEDIUMTEXT",
            "TINYTEXT",
            "LONGTEXT"),
        JSON("JSON", Types.LONGVARCHAR, String.class, true, 0, 0),
        BINARY("BINARY", Types.BINARY, null, false, 255, 0),
        VARCHAR("VARCHAR", Types.VARCHAR, java.lang.String.class, false, 65535L, 0),
        VARBINARY("VARBINARY", Types.VARBINARY, null, false, 65535, 0),
        BLOB("BLOB", Types.VARBINARY, null, false, 0, 0, "LONGBLOB",
            "MEDIUMBLOB",
            "TINYBLOB",
            "BLOB"),
        GEOMETRY("GEOMETRY", Types.BINARY, null, false, 65535L, 0),
        ENUM("ENUM", Types.CHAR, String.class, false, 65535L, 0),
        SET("SET", Types.CHAR, String.class, false, 64L, 0),

        String(null, Types.VARCHAR, String.class, false, 0, 0),
        Unknown(null, Types.OTHER, String.class, false, 0, 0);


        private static final DataTypeConverter[] dataTypeConverters = values();

        private final int sqlType;

        private final Class<?> javaClass;

        private final boolean signed;

        private final long defaultPrecision;

        private final int defaultScale;

        private final String[] aliases;

        private final String mySqlTypeName;

        DataTypeConverter(String mySqlTypeName, int sqlType, Class<?> javaClass,
                          boolean signed, long defaultPrecision, int defaultScale,
                          String... aliases) {
            this.mySqlTypeName = mySqlTypeName;
            this.sqlType = sqlType;
            this.javaClass = javaClass;
            this.signed = signed;
            this.defaultPrecision = defaultPrecision;
            this.defaultScale = defaultScale;
            this.aliases = aliases;
        }

        public static DataTypeConverter fromTypeString(String typeString) {
            String s = typeString.trim();
            for (DataTypeConverter dataType : dataTypeConverters) {
                if (s.equalsIgnoreCase(dataType.name())) {
                    return dataType;
                }
                for (String alias : dataType.aliases) {
                    if (s.equalsIgnoreCase(alias)) {
                        return dataType;
                    }
                }
            }
            return DataTypeConverter.Unknown;
        }

        public static DataTypeConverter resolveDefaultArrayDataType(String typeName) {
            for (DataTypeConverter chDataType : dataTypeConverters) {
                if (chDataType.name().equals(typeName)) {
                    return chDataType;
                }
            }
            return DataTypeConverter.String;
        }

        public String getMySqlTypeName(int Precision, boolean isSign) {
            switch (this) {
                case FLOAT32:
                    if (!isSign) {
                        return "FLOAT UNSIGNED";
                    } else {
                        return "FLOAT";
                    }
                case FLOAT64:
                    if (!isSign) {
                        return "DOUBLE UNSIGNED";
                    } else {
                        return "DOUBLE";
                    }
                case DECIMAL:
                    if (!isSign) {
                        return "DECIMAL UNSIGNED";
                    } else {
                        return "DECIMAL";
                    }
                case BLOB:
                    switch (Precision) {
                        case 255:
                            return "TINYBLOB";
                        case 65535:
                            return "BLOB";
                        case 16777215:
                            return "MEDIUMBLOB";
                        case 2147483647:
                            return "LONGBLOB";
                    }
                case TEXT:
                    switch (Precision) {
                        case 63:
                            return "TINYTEXT";
                        case 16383:
                            return "TEXT";
                        case 536870911:
                            return "LONGTEXT";
                        case 4194303:
                            return "MEDIUMTEXT";
                    }
                default:
                    return mySqlTypeName;
            }
        }

        public int getSqlType(int Precision) {
            switch (this) {
                case BLOB:
                    switch (Precision) {
                        case 255:
                            return Types.VARBINARY;
                        case 65535:
                        case 16777215:
                            return Types.LONGVARBINARY;
                        case 2147483647:
                            return LONGVARBINARY;
                    }
                default:
                    return sqlType;


            }
        }

        public boolean isSigned() {
            return signed;
        }

        public Class<?> getJavaClass() {
            return javaClass;
        }

        public long getDefaultPrecision() {
            return defaultPrecision;
        }

        public int getDefaultScale() {
            return defaultScale;
        }
    }
}
