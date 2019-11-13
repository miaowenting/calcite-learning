package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.util.Preconditions;
import org.codehaus.commons.nullanalysis.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-09
 */
public final class FieldsDataType extends DataType {

    private final Map<String, DataType> fieldDataTypes;

    public FieldsDataType(
            LogicalType logicalType,
            @Nullable Class<?> conversionClass,
            Map<String, DataType> fieldDataTypes) {
        super(logicalType, conversionClass);
        this.fieldDataTypes = Collections.unmodifiableMap(
                new HashMap<>(
                        Preconditions.checkNotNull(fieldDataTypes, "Field data types must not be null.")));
    }

    public FieldsDataType(
            LogicalType logicalType,
            Map<String, DataType> fieldDataTypes) {
        this(logicalType, null, fieldDataTypes);
    }

    public Map<String, DataType> getFieldDataTypes() {
        return fieldDataTypes;
    }

    @Override
    public DataType notNull() {
        return new FieldsDataType(
                logicalType.copy(false),
                conversionClass,
                fieldDataTypes);
    }

    @Override
    public DataType nullable() {
        return new FieldsDataType(
                logicalType.copy(true),
                conversionClass,
                fieldDataTypes);
    }

    @Override
    public DataType bridgedTo(Class<?> newConversionClass) {
        return new FieldsDataType(
                logicalType,
                Preconditions.checkNotNull(newConversionClass, "New conversion class must not be null."),
                fieldDataTypes);
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
        FieldsDataType that = (FieldsDataType) o;
        return fieldDataTypes.equals(that.fieldDataTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fieldDataTypes);
    }
}
