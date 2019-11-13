package com.matty.flink.sql.converter.type.logical;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-09
 */
public enum LogicalTypeFamily {
    PREDEFINED,

    CONSTRUCTED,

    USER_DEFINED,

    CHARACTER_STRING,

    BINARY_STRING,

    NUMERIC,

    EXACT_NUMERIC,

    APPROXIMATE_NUMERIC,

    DATETIME,

    TIME,

    TIMESTAMP,

    INTERVAL,

    COLLECTION,

    EXTENSION
}
