package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.util.Preconditions;

import java.util.*;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-09
 */
public final class MultisetType extends LogicalType {

    private static final String FORMAT = "MULTISET<%s>";

    private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
            Map.class.getName(),
            "org.apache.flink.table.dataformat.BinaryMap");

    private static final Class<?> DEFAULT_CONVERSION = Map.class;

    private final LogicalType elementType;

    public MultisetType(boolean isNullable, LogicalType elementType) {
        super(isNullable, LogicalTypeRoot.MULTISET);
        this.elementType = Preconditions.checkNotNull(elementType, "Element type must not be null.");
    }

    public MultisetType(LogicalType elementType) {
        this(true, elementType);
    }

    public LogicalType getElementType() {
        return elementType;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new MultisetType(isNullable, elementType.copy());
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
        MultisetType that = (MultisetType) o;
        return elementType.equals(that.elementType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), elementType);
    }
}

