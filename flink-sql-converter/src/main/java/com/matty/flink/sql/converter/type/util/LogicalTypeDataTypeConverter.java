package com.matty.flink.sql.converter.type.util;

import com.matty.flink.sql.converter.type.logical.*;
import com.matty.flink.sql.converter.type.logical.DataType;
import com.matty.flink.sql.converter.type.logical.LogicalType;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-09
 */
public final class LogicalTypeDataTypeConverter {

    private static final DefaultDataTypeCreator dataTypeCreator = new DefaultDataTypeCreator();

    /**
     * Returns the data type of a logical type without explicit conversions.
     */
    public static DataType toDataType(LogicalType logicalType) {
        return logicalType.accept(dataTypeCreator);
    }

    /**
     * Returns the logical type of a data type.
     */
    public static LogicalType toLogicalType(DataType dataType) {
        return dataType.getLogicalType();
    }

    // --------------------------------------------------------------------------------------------

    private static class DefaultDataTypeCreator implements LogicalTypeVisitor<DataType> {

        @Override
        public DataType visit(CharType charType) {
            return new AtomicDataType(charType);
        }

        @Override
        public DataType visit(VarCharType varCharType) {
            return new AtomicDataType(varCharType);
        }

        @Override
        public DataType visit(BooleanType booleanType) {
            return new AtomicDataType(booleanType);
        }

        @Override
        public DataType visit(BinaryType binaryType) {
            return new AtomicDataType(binaryType);
        }

        @Override
        public DataType visit(VarBinaryType varBinaryType) {
            return new AtomicDataType(varBinaryType);
        }

        @Override
        public DataType visit(DecimalType decimalType) {
            return new AtomicDataType(decimalType);
        }

        @Override
        public DataType visit(TinyIntType tinyIntType) {
            return new AtomicDataType(tinyIntType);
        }

        @Override
        public DataType visit(SmallIntType smallIntType) {
            return new AtomicDataType(smallIntType);
        }

        @Override
        public DataType visit(IntType intType) {
            return new AtomicDataType(intType);
        }

        @Override
        public DataType visit(BigIntType bigIntType) {
            return new AtomicDataType(bigIntType);
        }

        @Override
        public DataType visit(FloatType floatType) {
            return new AtomicDataType(floatType);
        }

        @Override
        public DataType visit(DoubleType doubleType) {
            return new AtomicDataType(doubleType);
        }

        @Override
        public DataType visit(DateType dateType) {
            return new AtomicDataType(dateType);
        }

        @Override
        public DataType visit(TimeType timeType) {
            return new AtomicDataType(timeType);
        }

        @Override
        public DataType visit(TimestampType timestampType) {
            return new AtomicDataType(timestampType);
        }

        @Override
        public DataType visit(ZonedTimestampType zonedTimestampType) {
            return new AtomicDataType(zonedTimestampType);
        }

        @Override
        public DataType visit(LocalZonedTimestampType localZonedTimestampType) {
            return new AtomicDataType(localZonedTimestampType);
        }

        @Override
        public DataType visit(YearMonthIntervalType yearMonthIntervalType) {
            return new AtomicDataType(yearMonthIntervalType);
        }

        @Override
        public DataType visit(DayTimeIntervalType dayTimeIntervalType) {
            return new AtomicDataType(dayTimeIntervalType);
        }

        @Override
        public DataType visit(ArrayType arrayType) {
            return new CollectionDataType(
                    arrayType,
                    arrayType.getElementType().accept(this));
        }

        @Override
        public DataType visit(MultisetType multisetType) {
            return new CollectionDataType(
                    multisetType,
                    multisetType.getElementType().accept(this));
        }

        @Override
        public DataType visit(MapType mapType) {
            return new KeyValueDataType(
                    mapType,
                    mapType.getKeyType().accept(this),
                    mapType.getValueType().accept(this));
        }

        @Override
        public DataType visit(RowType rowType) {
            final Map<String, DataType> fieldDataTypes = rowType.getFields()
                    .stream()
                    .collect(Collectors.toMap(RowType.RowField::getName, f -> f.getType().accept(this)));
            return new FieldsDataType(
                    rowType,
                    fieldDataTypes);
        }

        @Override
        public DataType visit(NullType nullType) {
            return new AtomicDataType(nullType);
        }

        @Override
        public DataType visit(AnyType<?> anyType) {
            return new AtomicDataType(anyType);
        }

        @Override
        public DataType visit(SymbolType<?> symbolType) {
            return new AtomicDataType(symbolType);
        }

        @Override
        public DataType visit(LogicalType other) {
            return new AtomicDataType(other);
        }
    }

    // --------------------------------------------------------------------------------------------

    private LogicalTypeDataTypeConverter() {
        // do not instantiate
    }
}

