package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.table.api.ValidationException;

import java.math.BigDecimal;
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
public final class DecimalType extends LogicalType {

    public static final int MIN_PRECISION = 1;

    public static final int MAX_PRECISION = 38;

    public static final int DEFAULT_PRECISION = 10;

    public static final int MIN_SCALE = 0;

    public static final int DEFAULT_SCALE = 0;

    private static final String FORMAT = "DECIMAL(%d, %d)";

    private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
            BigDecimal.class.getName(),
            "org.apache.flink.table.dataformat.Decimal");

    private static final Class<?> DEFAULT_CONVERSION = BigDecimal.class;

    private final int precision;

    private final int scale;

    public DecimalType(boolean isNullable, int precision, int scale) {
        super(isNullable, LogicalTypeRoot.DECIMAL);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Decimal precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION,
                            MAX_PRECISION));
        }
        if (scale < MIN_SCALE || scale > precision) {
            throw new ValidationException(
                    String.format(
                            "Decimal scale must be between %d and the precision %d (both inclusive).",
                            MIN_SCALE,
                            precision));
        }
        this.precision = precision;
        this.scale = scale;
    }

    public DecimalType(int precision, int scale) {
        this(true, precision, scale);
    }

    public DecimalType(int precision) {
        this(precision, DEFAULT_SCALE);
    }

    public DecimalType() {
        this(DEFAULT_PRECISION);
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new DecimalType(isNullable, precision, scale);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT, precision, scale);
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
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
        DecimalType that = (DecimalType) o;
        return precision == that.precision && scale == that.scale;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision, scale);
    }
}