package com.matty.flink.sql.converter.type.logical;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-09
 */
public interface LogicalTypeVisitor<R> {

    R visit(CharType charType);

    R visit(VarCharType varCharType);

    R visit(BooleanType booleanType);

    R visit(BinaryType binaryType);

    R visit(VarBinaryType varBinaryType);

    R visit(DecimalType decimalType);

    R visit(TinyIntType tinyIntType);

    R visit(SmallIntType smallIntType);

    R visit(IntType intType);

    R visit(BigIntType bigIntType);

    R visit(FloatType floatType);

    R visit(DoubleType doubleType);

    R visit(DateType dateType);

    R visit(TimeType timeType);

    R visit(TimestampType timestampType);

    R visit(ZonedTimestampType zonedTimestampType);

    R visit(LocalZonedTimestampType localZonedTimestampType);

    R visit(YearMonthIntervalType yearMonthIntervalType);

    R visit(DayTimeIntervalType dayTimeIntervalType);

    R visit(ArrayType arrayType);

    R visit(MultisetType multisetType);

    R visit(MapType mapType);

    R visit(RowType rowType);

    R visit(NullType nullType);

    R visit(AnyType<?> anyType);

    R visit(SymbolType<?> symbolType);

    R visit(LogicalType other);
}
