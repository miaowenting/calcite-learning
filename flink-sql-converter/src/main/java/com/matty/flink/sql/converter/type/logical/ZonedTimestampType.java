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
public final class ZonedTimestampType extends LogicalType {

    public static final int MIN_PRECISION = TimestampType.MIN_PRECISION;

    public static final int MAX_PRECISION = TimestampType.MAX_PRECISION;

    public static final int DEFAULT_PRECISION = TimestampType.DEFAULT_PRECISION;

    private static final String FORMAT = "TIMESTAMP(%d) WITH TIME ZONE";

    private static final Set<String> INPUT_CONVERSION = conversionSet(
            java.time.ZonedDateTime.class.getName(),
            java.time.OffsetDateTime.class.getName());

    private static final Set<String> OUTPUT_CONVERSION = conversionSet(
            java.time.OffsetDateTime.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = java.time.OffsetDateTime.class;

    private final TimestampKind kind;

    private final int precision;

    /**
     * Internal constructor that allows attaching additional metadata about time attribute
     * properties. The additional metadata does not affect equality or serializability.
     *
     * <p>Use {@link #getKind()} for comparing this metadata.
     */
    @Internal
    public ZonedTimestampType(boolean isNullable, TimestampKind kind, int precision) {
        super(isNullable, LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Timestamp with time zone precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION,
                            MAX_PRECISION));
        }
        this.kind = kind;
        this.precision = precision;
    }

    public ZonedTimestampType(boolean isNullable, int precision) {
        this(isNullable, TimestampKind.REGULAR, precision);
    }

    public ZonedTimestampType(int precision) {
        this(true, precision);
    }

    public ZonedTimestampType() {
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
        return new ZonedTimestampType(isNullable, kind, precision);
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
        return INPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return OUTPUT_CONVERSION.contains(clazz.getName());
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
        ZonedTimestampType that = (ZonedTimestampType) o;
        return precision == that.precision;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision);
    }
}