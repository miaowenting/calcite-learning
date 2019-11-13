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
public final class YearMonthIntervalType extends LogicalType {

    public static final int MIN_PRECISION = 1;

    public static final int MAX_PRECISION = 4;

    public static final int DEFAULT_PRECISION = 2;

    private static final String YEAR_FORMAT = "INTERVAL YEAR(%d)";

    private static final String YEAR_TO_MONTH_FORMAT = "INTERVAL YEAR(%d) TO MONTH";

    private static final String MONTH_FORMAT = "INTERVAL MONTH";

    private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(
            java.time.Period.class.getName(),
            Integer.class.getName());

    private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION = conversionSet(
            java.time.Period.class.getName(),
            Integer.class.getName(),
            int.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = java.time.Period.class;

    /**
     * Supported resolutions of this type.
     *
     * <p>Note: The order of this enum reflects the granularity from coarse to fine.
     */
    public enum YearMonthResolution {
        YEAR,
        YEAR_TO_MONTH,
        MONTH
    }

    private final YearMonthResolution resolution;

    private final int yearPrecision;

    public YearMonthIntervalType(boolean isNullable, YearMonthResolution resolution, int yearPrecision) {
        super(isNullable, LogicalTypeRoot.INTERVAL_YEAR_MONTH);
        Preconditions.checkNotNull(resolution);
        if (resolution == YearMonthResolution.MONTH && yearPrecision != DEFAULT_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Year precision of sub-year intervals must be equal to the default precision %d.",
                            DEFAULT_PRECISION));
        }
        if (yearPrecision < MIN_PRECISION || yearPrecision > MAX_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Year precision of year-month intervals must be between %d and %d (both inclusive).",
                            MIN_PRECISION,
                            MAX_PRECISION));
        }
        this.resolution = resolution;
        this.yearPrecision = yearPrecision;
    }

    public YearMonthIntervalType(YearMonthResolution resolution, int yearPrecision) {
        this(true, resolution, yearPrecision);
    }

    public YearMonthIntervalType(YearMonthResolution resolution) {
        this(resolution, DEFAULT_PRECISION);
    }

    public YearMonthResolution getResolution() {
        return resolution;
    }

    public int getYearPrecision() {
        return yearPrecision;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new YearMonthIntervalType(isNullable, resolution, yearPrecision);
    }

    @Override
    public String asSerializableString() {
        return withNullability(getResolutionFormat(), yearPrecision);
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
        YearMonthIntervalType that = (YearMonthIntervalType) o;
        return yearPrecision == that.yearPrecision && resolution == that.resolution;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), resolution, yearPrecision);
    }

    // --------------------------------------------------------------------------------------------

    private String getResolutionFormat() {
        switch (resolution) {
            case YEAR:
                return YEAR_FORMAT;
            case YEAR_TO_MONTH:
                return YEAR_TO_MONTH_FORMAT;
            case MONTH:
                return MONTH_FORMAT;
            default:
                throw new UnsupportedOperationException();
        }
    }
}

