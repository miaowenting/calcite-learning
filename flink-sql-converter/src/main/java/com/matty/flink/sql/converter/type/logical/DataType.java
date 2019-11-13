package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Preconditions;
import org.codehaus.commons.nullanalysis.Nullable;

import java.io.Serializable;
import java.util.Objects;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-09
 */
public abstract class DataType implements Serializable {

    protected final LogicalType logicalType;

    protected final Class<?> conversionClass;

    DataType(LogicalType logicalType, @Nullable Class<?> conversionClass) {
        this.logicalType = Preconditions.checkNotNull(logicalType, "Logical type must not be null.");
        this.conversionClass = performEarlyClassValidation(
                logicalType,
                ensureConversionClass(logicalType, conversionClass));
    }

    /**
     * Returns the corresponding logical type.
     *
     * @return a parameterized instance of {@link LogicalType}
     */
    public LogicalType getLogicalType() {
        return logicalType;
    }

    /**
     * Returns the corresponding conversion class for representing values. If no conversion class was
     * defined manually, the default conversion defined by the logical type is used.
     *
     * @see LogicalType#getDefaultConversion()
     *
     * @return the expected conversion class
     */
    public Class<?> getConversionClass() {
        return conversionClass;
    }

    /**
     * Adds a hint that null values are not expected in the data for this type.
     *
     * @return a new, reconfigured data type instance
     */
    public abstract DataType notNull();

    /**
     * Adds a hint that null values are expected in the data for this type (default behavior).
     *
     * <p>This method exists for explicit declaration of the default behavior or for invalidation of
     * a previous call to {@link #notNull()}.
     *
     * @return a new, reconfigured data type instance
     */
    public abstract DataType nullable();

    /**
     * Adds a hint that data should be represented using the given class when entering or leaving
     * the table ecosystem.
     *
     * <p>A supported conversion class depends on the logical type and its nullability property.
     *
     * <p>Please see the implementation of {@link LogicalType#supportsInputConversion(Class)},
     * {@link LogicalType#supportsOutputConversion(Class)}, or the documentation for more information
     * about supported conversions.
     *
     * @return a new, reconfigured data type instance
     */
    public abstract DataType bridgedTo(Class<?> newConversionClass);

    public abstract <R> R accept(DataTypeVisitor<R> visitor);

    @Override
    public String toString() {
        return logicalType.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DataType dataType = (DataType) o;
        return logicalType.equals(dataType.logicalType) &&
                conversionClass.equals(dataType.conversionClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logicalType, conversionClass);
    }

    // --------------------------------------------------------------------------------------------

    /**
     * This method should catch the most common errors. However, another validation is required in
     * deeper layers as we don't know whether the data type is used for input or output declaration.
     */
    private static <C> Class<C> performEarlyClassValidation(
            LogicalType logicalType,
            Class<C> candidate) {

        if (candidate != null &&
                !logicalType.supportsInputConversion(candidate) &&
                !logicalType.supportsOutputConversion(candidate)) {
            throw new ValidationException(
                    String.format(
                            "Logical type '%s' does not support a conversion from or to class '%s'.",
                            logicalType.asSummaryString(),
                            candidate.getName()));
        }
        return candidate;
    }

    private static Class<?> ensureConversionClass(LogicalType logicalType, @Nullable Class<?> clazz) {
        if (clazz == null) {
            return logicalType.getDefaultConversion();
        }
        return clazz;
    }
}

