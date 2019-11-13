package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.table.api.ValidationException;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-09
 */
public final class TimeType extends LogicalType {

    public static final int MIN_PRECISION = 0;

    public static final int MAX_PRECISION = 9;

    public static final int DEFAULT_PRECISION = 0;

    private static final String FORMAT = "TIME(%d)";

    private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(
            java.sql.Time.class.getName(),
            java.time.LocalTime.class.getName(),
            Integer.class.getName(),
            Long.class.getName());

    private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION = conversionSet(
            java.sql.Time.class.getName(),
            java.time.LocalTime.class.getName(),
            Integer.class.getName(),
            int.class.getName(),
            Long.class.getName(),
            long.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = java.time.LocalTime.class;

    private final int precision;

    public TimeType(boolean isNullable, int precision) {
        super(isNullable, LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Time precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION,
                            MAX_PRECISION));
        }
        this.precision = precision;
    }

    public TimeType(int precision) {
        this(true, precision);
    }

    public TimeType() {
        this(DEFAULT_PRECISION);
    }

    public int getPrecision() {
        return precision;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new TimeType(isNullable, precision);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT, precision);
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return NOT_NULL_INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        if (isNullable()) {
            return NULL_OUTPUT_CONVERSION.contains(clazz.getName());
        }
        return NOT_NULL_INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public Class<?> getDefaultConversion() {
        return DEFAULT_CONVERSION;
    }

    @Override
    public List<LogicalType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        TimeType timeType = (TimeType) o;
        return precision == timeType.precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision);
    }
}
