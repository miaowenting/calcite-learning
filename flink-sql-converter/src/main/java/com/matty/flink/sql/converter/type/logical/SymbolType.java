package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.TableSymbol;
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
public final class SymbolType<T extends TableSymbol> extends LogicalType {

    private static final String FORMAT = "SYMBOL('%s')";

    private final Class<T> symbolClass;

    public SymbolType(boolean isNullable, Class<T> symbolClass) {
        super(isNullable, LogicalTypeRoot.SYMBOL);
        this.symbolClass = Preconditions.checkNotNull(symbolClass, "Symbol class must not be null.");
    }

    public SymbolType(Class<T> symbolClass) {
        this(true, symbolClass);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new SymbolType<>(isNullable, symbolClass);
    }

    @Override
    public String asSummaryString() {
        return withNullability(FORMAT, symbolClass.getName());
    }

    @Override
    public String asSerializableString() {
        throw new TableException("A symbol type has no serializable string representation.");
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return symbolClass.equals(clazz);
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return symbolClass.equals(clazz);
    }

    @Override
    public Class<?> getDefaultConversion() {
        return symbolClass;
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
        SymbolType<?> that = (SymbolType<?>) o;
        return symbolClass.equals(that.symbolClass);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), symbolClass);
    }
}
