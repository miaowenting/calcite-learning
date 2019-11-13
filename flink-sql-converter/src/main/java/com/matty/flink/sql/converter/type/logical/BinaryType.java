package com.matty.flink.sql.converter.type.logical;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

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
public final class BinaryType extends LogicalType {

    public static final int EMPTY_LITERAL_LENGTH = 0;

    public static final int MIN_LENGTH = 1;

    public static final int MAX_LENGTH = Integer.MAX_VALUE;

    public static final int DEFAULT_LENGTH = 1;

    private static final String FORMAT = "BINARY(%d)";

    private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(
            byte[].class.getName(),
            "org.apache.flink.table.dataformat.BinaryArray");

    private static final Class<?> DEFAULT_CONVERSION = byte[].class;

    private final int length;

    public BinaryType(boolean isNullable, int length) {
        super(isNullable, LogicalTypeRoot.BINARY);
        if (length < MIN_LENGTH) {
            throw new ValidationException(
                    String.format(
                            "Binary string length must be between %d and %d (both inclusive).",
                            MIN_LENGTH,
                            MAX_LENGTH));
        }
        this.length = length;
    }

    public BinaryType(int length) {
        this(true, length);
    }

    public BinaryType() {
        this(DEFAULT_LENGTH);
    }

    /**
     * Helper constructor for {@link #ofEmptyLiteral()} and {@link #copy(boolean)}.
     */
    private BinaryType(int length, boolean isNullable) {
        super(isNullable, LogicalTypeRoot.BINARY);
        this.length = length;
    }

    /**
     * The SQL standard defines that character string literals are allowed to be zero-length strings
     * (i.e., to contain no characters) even though it is not permitted to declare a type that is zero.
     * For consistent behavior, the same logic applies to binary strings.
     *
     * <p>This method enables this special kind of binary string.
     *
     * <p>Zero-length binary strings have no serializable string representation.
     */
    public static BinaryType ofEmptyLiteral() {
        return new BinaryType(EMPTY_LITERAL_LENGTH, false);
    }

    public int getLength() {
        return length;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new BinaryType(length, isNullable);
    }

    @Override
    public String asSerializableString() {
        if (length == EMPTY_LITERAL_LENGTH) {
            throw new TableException(
                    "Zero-length binary strings have no serializable string representation.");
        }
        return withNullability(FORMAT, length);
    }

    @Override
    public String asSummaryString() {
        return withNullability(FORMAT, length);
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
        BinaryType that = (BinaryType) o;
        return length == that.length;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), length);
    }
}

