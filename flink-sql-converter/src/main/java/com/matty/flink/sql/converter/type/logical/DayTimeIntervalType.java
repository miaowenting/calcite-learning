package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Preconditions;

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
public final class DayTimeIntervalType extends LogicalType {

    public static final int MIN_DAY_PRECISION = 1;

    public static final int MAX_DAY_PRECISION = 6;

    public static final int DEFAULT_DAY_PRECISION = 2;

    public static final int MIN_FRACTIONAL_PRECISION = 0;

    public static final int MAX_FRACTIONAL_PRECISION = 9;

    public static final int DEFAULT_FRACTIONAL_PRECISION = 6;

    private static final String DAY_FORMAT = "INTERVAL DAY(%1$d)";

    private static final String DAY_TO_HOUR_FORMAT = "INTERVAL DAY(%1$d) TO HOUR";

    private static final String DAY_TO_MINUTE_FORMAT = "INTERVAL DAY(%1$d) TO MINUTE";

    private static final String DAY_TO_SECOND_FORMAT = "INTERVAL DAY(%1$d) TO SECOND(%2$d)";

    private static final String HOUR_FORMAT = "INTERVAL HOUR";

    private static final String HOUR_TO_MINUTE_FORMAT = "INTERVAL HOUR TO MINUTE";

    private static final String HOUR_TO_SECOND_FORMAT = "INTERVAL HOUR TO SECOND(%2$d)";

    private static final String MINUTE_FORMAT = "INTERVAL MINUTE";

    private static final String MINUTE_TO_SECOND_FORMAT = "INTERVAL MINUTE TO SECOND(%2$d)";

    private static final String SECOND_FORMAT = "INTERVAL SECOND(%2$d)";

    private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(
            java.time.Duration.class.getName(),
            Long.class.getName());

    private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION = conversionSet(
            java.time.Duration.class.getName(),
            Long.class.getName(),
            long.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = java.time.Duration.class;

    /**
     * Supported resolutions of this type.
     *
     * <p>Note: The order of this enum reflects the granularity from coarse to fine.
     */
    public enum DayTimeResolution {
        DAY,
        DAY_TO_HOUR,
        DAY_TO_MINUTE,
        DAY_TO_SECOND,
        HOUR,
        HOUR_TO_MINUTE,
        HOUR_TO_SECOND,
        MINUTE,
        MINUTE_TO_SECOND,
        SECOND
    }

    private final DayTimeResolution resolution;

    private final int dayPrecision;

    private final int fractionalPrecision;

    public DayTimeIntervalType(
            boolean isNullable,
            DayTimeResolution resolution,
            int dayPrecision,
            int fractionalPrecision) {
        super(isNullable, LogicalTypeRoot.INTERVAL_DAY_TIME);
        Preconditions.checkNotNull(resolution);
        if (needsDefaultDayPrecision(resolution) && dayPrecision != DEFAULT_DAY_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Day precision of sub-day intervals must be equal to the default precision %d.",
                            DEFAULT_DAY_PRECISION));
        }
        if (needsDefaultFractionalPrecision(resolution) && fractionalPrecision != DEFAULT_FRACTIONAL_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Fractional precision of super-second intervals must be equal to the default precision %d.",
                            DEFAULT_FRACTIONAL_PRECISION));
        }
        if (dayPrecision < MIN_DAY_PRECISION || dayPrecision > MAX_DAY_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Day precision of day-time intervals must be between %d and %d (both inclusive).",
                            MIN_DAY_PRECISION,
                            MAX_DAY_PRECISION));
        }
        if (fractionalPrecision < MIN_FRACTIONAL_PRECISION || fractionalPrecision > MAX_FRACTIONAL_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Fractional precision of day-time intervals must be between %d and %d (both inclusive).",
                            MIN_FRACTIONAL_PRECISION,
                            MAX_FRACTIONAL_PRECISION));
        }
        this.resolution = resolution;
        this.dayPrecision = dayPrecision;
        this.fractionalPrecision = fractionalPrecision;
    }

    public DayTimeIntervalType(DayTimeResolution resolution, int dayPrecision, int fractionalPrecision) {
        this(true, resolution, dayPrecision, fractionalPrecision);
    }

    public DayTimeIntervalType(DayTimeResolution resolution) {
        this(resolution, DEFAULT_DAY_PRECISION, DEFAULT_FRACTIONAL_PRECISION);
    }

    public DayTimeResolution getResolution() {
        return resolution;
    }

    public int getDayPrecision() {
        return dayPrecision;
    }

    public int getFractionalPrecision() {
        return fractionalPrecision;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new DayTimeIntervalType(isNullable, resolution, dayPrecision, fractionalPrecision);
    }

    @Override
    public String asSerializableString() {
        return withNullability(getResolutionFormat(), dayPrecision, fractionalPrecision);
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
        DayTimeIntervalType that = (DayTimeIntervalType) o;
        return dayPrecision == that.dayPrecision &&
                fractionalPrecision == that.fractionalPrecision &&
                resolution == that.resolution;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resolution, dayPrecision, fractionalPrecision);
    }

    // --------------------------------------------------------------------------------------------

    private boolean needsDefaultDayPrecision(DayTimeResolution resolution) {
        switch (resolution) {
            case HOUR:
            case HOUR_TO_MINUTE:
            case HOUR_TO_SECOND:
            case MINUTE:
            case MINUTE_TO_SECOND:
            case SECOND:
                return true;
            default:
                return false;
        }
    }

    private boolean needsDefaultFractionalPrecision(DayTimeResolution resolution) {
        switch (resolution) {
            case DAY:
            case DAY_TO_HOUR:
            case DAY_TO_MINUTE:
            case HOUR:
            case HOUR_TO_MINUTE:
            case MINUTE:
                return true;
            default:
                return false;
        }
    }

    private String getResolutionFormat() {
        switch (resolution) {
            case DAY:
                return DAY_FORMAT;
            case DAY_TO_HOUR:
                return DAY_TO_HOUR_FORMAT;
            case DAY_TO_MINUTE:
                return DAY_TO_MINUTE_FORMAT;
            case DAY_TO_SECOND:
                return DAY_TO_SECOND_FORMAT;
            case HOUR:
                return HOUR_FORMAT;
            case HOUR_TO_MINUTE:
                return HOUR_TO_MINUTE_FORMAT;
            case HOUR_TO_SECOND:
                return HOUR_TO_SECOND_FORMAT;
            case MINUTE:
                return MINUTE_FORMAT;
            case MINUTE_TO_SECOND:
                return MINUTE_TO_SECOND_FORMAT;
            case SECOND:
                return SECOND_FORMAT;
            default:
                throw new UnsupportedOperationException();
        }
    }
}

