package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.annotation.Internal;
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
public final class LocalZonedTimestampType extends LogicalType {

    public static final int MIN_PRECISION = TimestampType.MIN_PRECISION;

    public static final int MAX_PRECISION = TimestampType.MAX_PRECISION;

    public static final int DEFAULT_PRECISION = TimestampType.DEFAULT_PRECISION;

    private static final String FORMAT = "TIMESTAMP(%d) WITH LOCAL TIME ZONE";

    private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(
            java.time.Instant.class.getName(),
            Integer.class.getName(),
            Long.class.getName());

    private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION = conversionSet(
            java.time.Instant.class.getName(),
            Integer.class.getName(),
            int.class.getName(),
            Long.class.getName(),
            long.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = java.time.Instant.class;

    private final TimestampKind kind;

    private final int precision;

    /**
     * Internal constructor that allows attaching additional metadata about time attribute
     * properties. The additional metadata does not affect equality or serializability.
     *
     * <p>Use {@link #getKind()} for comparing this metadata.
     */
    @Internal
    public LocalZonedTimestampType(boolean isNullable, TimestampKind kind, int precision) {
        super(isNullable, LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Timestamp with local time zone precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION,
                            MAX_PRECISION));
        }
        this.kind = kind;
        this.precision = precision;
    }

    public LocalZonedTimestampType(boolean isNullable, int precision) {
        this(isNullable, TimestampKind.REGULAR, precision);
    }

    public LocalZonedTimestampType(int precision) {
        this(true, precision);
    }

    public LocalZonedTimestampType() {
        this(DEFAULT_PRECISION);
    }

    @Internal
    public TimestampKind getKind() {
        return kind;
    }

    public int getPrecision() {
        return precision;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new LocalZonedTimestampType(isNullable, kind, precision);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT, precision);
    }

    @Override
    public String asSummaryString() {
        if (kind != TimestampKind.REGULAR) {
            return String.format("%s *%s*", asSerializableString(), kind);
        }
        return asSerializableString();
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
        LocalZonedTimestampType that = (LocalZonedTimestampType) o;
        return precision == that.precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision);
    }
}
