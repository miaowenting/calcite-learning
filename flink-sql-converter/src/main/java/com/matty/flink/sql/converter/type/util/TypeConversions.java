package com.matty.flink.sql.converter.type.util;

import com.matty.flink.sql.converter.type.logical.DataType;
import com.matty.flink.sql.converter.type.logical.LogicalType;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Optional;
import java.util.stream.Stream;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-09
 */
public final class TypeConversions {

    public static DataType fromLegacyInfoToDataType(TypeInformation<?> typeInfo) {
        return LegacyTypeInfoDataTypeConverter.toDataType(typeInfo);
    }

    public static DataType[] fromLegacyInfoToDataType(TypeInformation<?>[] typeInfo) {
        return Stream.of(typeInfo)
                .map(TypeConversions::fromLegacyInfoToDataType)
                .toArray(DataType[]::new);
    }

    public static TypeInformation<?> fromDataTypeToLegacyInfo(DataType dataType) {
        return LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(dataType);
    }

    public static TypeInformation<?>[] fromDataTypeToLegacyInfo(DataType[] dataType) {
        return Stream.of(dataType)
                .map(TypeConversions::fromDataTypeToLegacyInfo)
                .toArray(TypeInformation[]::new);
    }

    public static Optional<DataType> fromClassToDataType(Class<?> clazz) {
        return ClassDataTypeConverter.extractDataType(clazz);
    }

    public static DataType fromLogicalToDataType(LogicalType logicalType) {
        return LogicalTypeDataTypeConverter.toDataType(logicalType);
    }

    public static DataType[] fromLogicalToDataType(LogicalType[] logicalTypes) {
        return Stream.of(logicalTypes)
                .map(LogicalTypeDataTypeConverter::toDataType)
                .toArray(DataType[]::new);
    }

    public static LogicalType fromDataToLogicalType(DataType dataType) {
        return dataType.getLogicalType();
    }

    public static LogicalType[] fromDataToLogicalType(DataType[] dataTypes) {
        return Stream.of(dataTypes)
                .map(TypeConversions::fromDataToLogicalType)
                .toArray(LogicalType[]::new);
    }

    private TypeConversions() {
        // no instance
    }
}

