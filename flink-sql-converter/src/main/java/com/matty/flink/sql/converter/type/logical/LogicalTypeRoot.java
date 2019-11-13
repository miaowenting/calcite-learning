package com.matty.flink.sql.converter.type.logical;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-09
 */
public enum LogicalTypeRoot {
    CHAR(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.CHARACTER_STRING),

    VARCHAR(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.CHARACTER_STRING),

    BOOLEAN(
            LogicalTypeFamily.PREDEFINED),

    BINARY(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.BINARY_STRING),

    VARBINARY(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.BINARY_STRING),

    DECIMAL(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.NUMERIC,
            LogicalTypeFamily.EXACT_NUMERIC),

    TINYINT(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.NUMERIC,
            LogicalTypeFamily.EXACT_NUMERIC),

    SMALLINT(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.NUMERIC,
            LogicalTypeFamily.EXACT_NUMERIC),

    INTEGER(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.NUMERIC,
            LogicalTypeFamily.EXACT_NUMERIC),

    BIGINT(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.NUMERIC,
            LogicalTypeFamily.EXACT_NUMERIC),

    FLOAT(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.NUMERIC,
            LogicalTypeFamily.APPROXIMATE_NUMERIC),

    DOUBLE(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.NUMERIC,
            LogicalTypeFamily.APPROXIMATE_NUMERIC),

    DATE(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.DATETIME),

    TIME_WITHOUT_TIME_ZONE(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.DATETIME,
            LogicalTypeFamily.TIME),

    TIMESTAMP_WITHOUT_TIME_ZONE(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.DATETIME,
            LogicalTypeFamily.TIMESTAMP),

    TIMESTAMP_WITH_TIME_ZONE(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.DATETIME,
            LogicalTypeFamily.TIMESTAMP),

    TIMESTAMP_WITH_LOCAL_TIME_ZONE(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.DATETIME,
            LogicalTypeFamily.TIMESTAMP,
            LogicalTypeFamily.EXTENSION),

    INTERVAL_YEAR_MONTH(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.INTERVAL),

    INTERVAL_DAY_TIME(
            LogicalTypeFamily.PREDEFINED,
            LogicalTypeFamily.INTERVAL),

    ARRAY(
            LogicalTypeFamily.CONSTRUCTED,
            LogicalTypeFamily.COLLECTION),

    MULTISET(
            LogicalTypeFamily.CONSTRUCTED,
            LogicalTypeFamily.COLLECTION),

    MAP(
            LogicalTypeFamily.CONSTRUCTED,
            LogicalTypeFamily.EXTENSION),

    ROW(
            LogicalTypeFamily.CONSTRUCTED),

    DISTINCT_TYPE(
            LogicalTypeFamily.USER_DEFINED),

    STRUCTURED_TYPE(
            LogicalTypeFamily.USER_DEFINED),

    NULL(
            LogicalTypeFamily.EXTENSION),

    ANY(
            LogicalTypeFamily.EXTENSION),

    SYMBOL(
            LogicalTypeFamily.EXTENSION),

    UNRESOLVED(
            LogicalTypeFamily.EXTENSION);

    private final Set<LogicalTypeFamily> families;

    LogicalTypeRoot(LogicalTypeFamily firstFamily, LogicalTypeFamily... otherFamilies) {
        this.families = Collections.unmodifiableSet(EnumSet.of(firstFamily, otherFamilies));
    }

    public Set<LogicalTypeFamily> getFamilies() {
        return families;
    }
}
