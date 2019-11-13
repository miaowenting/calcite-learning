package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.util.Preconditions;

import java.lang.reflect.Array;
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
public final class ArrayType extends LogicalType {

    private static final String FORMAT = "ARRAY<%s>";

    private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
            "org.apache.flink.table.dataformat.BinaryArray");

    private final LogicalType elementType;

    public ArrayType(boolean isNullable, LogicalType elementType) {
        super(isNullable, LogicalTypeRoot.ARRAY);
        this.elementType = Preconditions.checkNotNull(elementType, "Element type must not be null.");
    }

    public ArrayType(LogicalType elementType) {
        this(true, elementType);
    }

    public LogicalType getElementType() {
        return elementType;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new ArrayType(isNullable, elementType.copy());
    }

    @Override
    public String asSummaryString() {
        return withNullability(FORMAT, elementType.asSummaryString());
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT, elementType.asSerializableString());
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        if (INPUT_OUTPUT_CONVERSION.contains(clazz.getName())) {
            return true;
        }
        if (!clazz.isArray()) {
            return false;
        }
        return elementType.supportsInputConversion(clazz.getComponentType());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        if (INPUT_OUTPUT_CONVERSION.contains(clazz.getName())) {
            return true;
        }
        if (!clazz.isArray()) {
            return false;
        }
        return elementType.supportsOutputConversion(clazz.getComponentType());
    }

    @Override
    public Class<?> getDefaultConversion() {
        return Array.newInstance(elementType.getDefaultConversion(), 0).getClass();
    }

    @Override
    public List<LogicalType> getChildren() {
        return Collections.singletonList(elementType);
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
        ArrayType arrayType = (ArrayType) o;
        return elementType.equals(arrayType.elementType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), elementType);
    }
}

