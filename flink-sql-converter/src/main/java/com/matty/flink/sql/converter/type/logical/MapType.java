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
public final class MapType extends LogicalType {

    private static final String FORMAT = "MAP<%s, %s>";

    private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
            Map.class.getName(),
            "org.apache.flink.table.dataformat.BinaryMap");

    private static final Class<?> DEFAULT_CONVERSION = Map.class;

    private final LogicalType keyType;

    private final LogicalType valueType;

    public MapType(boolean isNullable, LogicalType keyType, LogicalType valueType) {
        super(isNullable, LogicalTypeRoot.MAP);
        this.keyType = Preconditions.checkNotNull(keyType, "Key type must not be null.");
        this.valueType = Preconditions.checkNotNull(valueType, "Value type must not be null.");
    }

    public MapType(LogicalType keyType, LogicalType valueType) {
        this(true, keyType, valueType);
    }

    public LogicalType getKeyType() {
        return keyType;
    }

    public LogicalType getValueType() {
        return valueType;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new MapType(isNullable, keyType.copy(), valueType.copy());
    }

    @Override
    public String asSummaryString() {
        return withNullability(FORMAT,
                keyType.asSummaryString(),
                valueType.asSummaryString());
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT,
                keyType.asSerializableString(),
                valueType.asSerializableString());
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
        return Collections.unmodifiableList(Arrays.asList(keyType, valueType));
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
        MapType mapType = (MapType) o;
        return keyType.equals(mapType.keyType) && valueType.equals(mapType.valueType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), keyType, valueType);
    }
}

