package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.util.Preconditions;
import org.codehaus.commons.nullanalysis.Nullable;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-09
 */
public final class AtomicDataType extends DataType {

    public AtomicDataType(LogicalType logicalType, @Nullable Class<?> conversionClass) {
        super(logicalType, conversionClass);
    }

    public AtomicDataType(LogicalType logicalType) {
        super(logicalType, null);
    }

    @Override
    public DataType notNull() {
        return new AtomicDataType(
                logicalType.copy(false),
                conversionClass);
    }

    @Override
    public DataType nullable() {
        return new AtomicDataType(
                logicalType.copy(true),
                conversionClass);
    }

    @Override
    public DataType bridgedTo(Class<?> newConversionClass) {
        return new AtomicDataType(
                logicalType,
                Preconditions.checkNotNull(newConversionClass, "New conversion class must not be null."));
    }

    @Override
    public <R> R accept(DataTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
