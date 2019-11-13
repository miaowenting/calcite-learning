package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.table.api.TableException;

import java.util.Collections;
import java.util.List;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-09
 */
public final class NullType extends LogicalType {

    private static final String FORMAT = "NULL";

    private static final Class<?> INPUT_CONVERSION = Object.class;

    private static final Class<?> DEFAULT_CONVERSION = Object.class;

    public NullType() {
        super(true, LogicalTypeRoot.NULL);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        if (!isNullable) {
            throw new TableException(
                    "The nullability of a NULL type cannot be disabled because the type must always " +
                            "be able to contain a null value.");
        }
        return new NullType();
    }

    @Override
    public String asSerializableString() {
        return FORMAT;
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return INPUT_CONVERSION.equals(clazz);
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        // any nullable class is supported
        return !clazz.isPrimitive();
    }

    @Override
    public Class<?> getDefaultConversion() {
        return DEFAULT_CONVERSION;
    }

    @Override
    public List<LogicalType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}

