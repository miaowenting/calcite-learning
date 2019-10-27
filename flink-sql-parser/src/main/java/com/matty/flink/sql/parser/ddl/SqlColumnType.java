package com.matty.flink.sql.parser.ddl;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-08
 */
public enum SqlColumnType {
    BOOLEAN,
    TINYINT,
    SMALLINT,
    INT,
    INTEGER,
    BIGINT,
    REAL,
    FLOAT,
    DOUBLE,
    DECIMAL,
    DATE,
    TIME,
    TIMESTAMP,
    VARCHAR,
    VARBINARY,
    ANY,
    ARRAY,
    MAP,
    ROW,
    UNSUPPORTED;

    /** Returns the column type with the string representation. **/
    public static SqlColumnType getType(String type) {
        if (type == null) {
            return UNSUPPORTED;
        }
        try {
            return SqlColumnType.valueOf(type.toUpperCase());
        } catch (IllegalArgumentException var1) {
            return UNSUPPORTED;
        }
    }

    /** Returns true if this type is unsupported. **/
    public boolean isUnsupported() {
        return this.equals(UNSUPPORTED);
    }
}
