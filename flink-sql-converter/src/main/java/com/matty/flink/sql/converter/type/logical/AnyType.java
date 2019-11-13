package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.utils.EncodingUtils;
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
public final class AnyType<T> extends LogicalType {

    private static final String FORMAT = "ANY('%s', '%s')";

    private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
            byte[].class.getName(),
            "org.apache.flink.table.dataformat.BinaryGeneric");

    private final Class<T> clazz;

    private final TypeSerializer<T> serializer;

    private transient String serializerString;

    public AnyType(boolean isNullable, Class<T> clazz, TypeSerializer<T> serializer) {
        super(isNullable, LogicalTypeRoot.ANY);
        this.clazz = Preconditions.checkNotNull(clazz, "Class must not be null.");
        this.serializer = Preconditions.checkNotNull(serializer, "Serializer must not be null.");
    }

    public AnyType(Class<T> clazz, TypeSerializer<T> serializer) {
        this(true, clazz, serializer);
    }

    public Class<T> getOriginatingClass() {
        return clazz;
    }

    public TypeSerializer<T> getTypeSerializer() {
        return serializer;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new AnyType<>(isNullable, clazz, serializer.duplicate());
    }

    @Override
    public String asSummaryString() {
        return withNullability(FORMAT, clazz.getName(), "...");
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT, clazz.getName(), getOrCreateSerializerString());
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return this.clazz.isAssignableFrom(clazz) ||
                INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return clazz.isAssignableFrom(this.clazz) ||
                INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public Class<?> getDefaultConversion() {
        return clazz;
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
        AnyType<?> anyType = (AnyType<?>) o;
        return clazz.equals(anyType.clazz) && serializer.equals(anyType.serializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), clazz, serializer);
    }

    // --------------------------------------------------------------------------------------------

    private String getOrCreateSerializerString() {
        if (serializerString == null) {
            final DataOutputSerializer outputSerializer = new DataOutputSerializer(128);
            try {
                TypeSerializerSnapshot.writeVersionedSnapshot(outputSerializer, serializer.snapshotConfiguration());
                serializerString = EncodingUtils.encodeBytesToBase64(outputSerializer.getCopyOfBuffer());
                return serializerString;
            } catch (Exception e) {
                throw new TableException(String.format(
                        "Unable to generate a string representation of the serializer snapshot of '%s' " +
                                "describing the class '%s' for the ANY type.",
                        serializer.getClass().getName(),
                        clazz.toString()), e);
            }
        }
        return serializerString;
    }
}

