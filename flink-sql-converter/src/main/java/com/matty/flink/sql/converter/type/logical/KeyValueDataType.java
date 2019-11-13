package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.util.Preconditions;
import org.codehaus.commons.nullanalysis.Nullable;

import java.util.Objects;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-09
 */
public final class KeyValueDataType extends DataType {

    private final DataType keyDataType;

    private final DataType valueDataType;

    public KeyValueDataType(
            LogicalType logicalType,
            @Nullable Class<?> conversionClass,
            DataType keyDataType,
            DataType valueDataType) {
        super(logicalType, conversionClass);
        this.keyDataType = Preconditions.checkNotNull(keyDataType, "Key data type must not be null.");
        this.valueDataType = Preconditions.checkNotNull(valueDataType, "Value data type must not be null.");
    }

    public KeyValueDataType(
            LogicalType logicalType,
            DataType keyDataType,
            DataType valueDataType) {
        this(logicalType, null, keyDataType, valueDataType);
    }

    public DataType getKeyDataType() {
        return keyDataType;
    }

    public DataType getValueDataType() {
        return valueDataType;
    }

    @Override
    public DataType notNull() {
        return new KeyValueDataType(
                logicalType.copy(false),
                conversionClass,
                keyDataType,
                valueDataType);
    }

    @Override
    public DataType nullable() {
        return new KeyValueDataType(
                logicalType.copy(true),
                conversionClass,
                keyDataType,
                valueDataType);
    }

    @Override
    public DataType bridgedTo(Class<?> newConversionClass) {
        return new KeyValueDataType(
                logicalType,
                Preconditions.checkNotNull(newConversionClass, "New conversion class must not be null."),
                keyDataType,
                valueDataType);
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
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
        KeyValueDataType that = (KeyValueDataType) o;
        return keyDataType.equals(that.keyDataType) && valueDataType.equals(that.valueDataType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyDataType, valueDataType);
    }
}
