package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableException;
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
public final class TypeInformationAnyType<T> extends LogicalType {

    private static final String FORMAT = "ANY('%s', ?)";

    private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
            byte[].class.getName(),
            "org.apache.flink.table.dataformat.BinaryGeneric");

    private static final TypeInformation<?> DEFAULT_TYPE_INFO = Types.GENERIC(Object.class);

    private final TypeInformation<T> typeInfo;

    public TypeInformationAnyType(boolean isNullable, TypeInformation<T> typeInfo) {
        super(isNullable, LogicalTypeRoot.ANY);
        this.typeInfo = Preconditions.checkNotNull(typeInfo, "Type information must not be null.");
    }

    public TypeInformationAnyType(TypeInformation<T> typeInfo) {
        this(true, typeInfo);
    }

    @SuppressWarnings("unchecked")
    public TypeInformationAnyType() {
        this(true, (TypeInformation<T>) DEFAULT_TYPE_INFO);
    }

    public TypeInformation<T> getTypeInformation() {
        return typeInfo;
    }

    @Internal
    public AnyType<T> resolve(ExecutionConfig config) {
        return new AnyType<>(isNullable(), typeInfo.getTypeClass(), typeInfo.createSerializer(config));
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new TypeInformationAnyType<>(isNullable, typeInfo); // we must assume immutability here
    }

    @Override
    public String asSummaryString() {
        return withNullability(FORMAT, typeInfo.getTypeClass().getName());
    }

    @Override
    public String asSerializableString() {
        throw new TableException(
                "An any type backed by type information has no serializable string representation. It " +
                        "needs to be resolved into a proper any type.");
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return typeInfo.getTypeClass().isAssignableFrom(clazz) ||
                INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return clazz.isAssignableFrom(typeInfo.getTypeClass()) ||
                INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public Class<?> getDefaultConversion() {
        return typeInfo.getTypeClass();
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
        TypeInformationAnyType<?> that = (TypeInformationAnyType<?>) o;
        return typeInfo.equals(that.typeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), typeInfo);
    }
}

