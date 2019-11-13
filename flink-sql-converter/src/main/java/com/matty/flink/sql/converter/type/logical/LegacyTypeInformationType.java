package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-09
 */
public final class LegacyTypeInformationType<T> extends LogicalType {

    private static final String FORMAT = "LEGACY(%s)";

    private final TypeInformation<T> typeInfo;

    public LegacyTypeInformationType(LogicalTypeRoot logicalTypeRoot, TypeInformation<T> typeInfo) {
        super(true, logicalTypeRoot);
        this.typeInfo = Preconditions.checkNotNull(typeInfo, "Type information must not be null.");
    }

    public TypeInformation<T> getTypeInformation() {
        return typeInfo;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new LegacyTypeInformationType<>(getTypeRoot(), typeInfo);
    }

    @Override
    public String asSerializableString() {
        throw new TableException("Legacy type information has no serializable string representation.");
    }

    @Override
    public String asSummaryString() {
        return withNullability(FORMAT, typeInfo);
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return typeInfo.getTypeClass().isAssignableFrom(clazz);
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return clazz.isAssignableFrom(typeInfo.getTypeClass());
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
        LegacyTypeInformationType<?> that = (LegacyTypeInformationType<?>) o;
        return typeInfo.equals(that.typeInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), typeInfo);
    }
}
