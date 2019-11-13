package com.matty.flink.sql.converter.type.util;

import com.matty.flink.sql.converter.type.DataTypes;
import com.matty.flink.sql.converter.type.logical.AtomicDataType;
import com.matty.flink.sql.converter.type.logical.DataType;
import com.matty.flink.sql.converter.type.logical.SymbolType;
import org.apache.flink.table.expressions.TableSymbol;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-09
 */
public final class ClassDataTypeConverter {

    private static final Map<String, DataType> defaultDataTypes = new HashMap<>();

    static {
        // NOTE: this list explicitly excludes data types that need further parameters
        // exclusions: DECIMAL, INTERVAL YEAR TO MONTH, MAP, MULTISET, ROW, NULL, ANY
        addDefaultDataType(String.class, DataTypes.STRING());
        addDefaultDataType(Boolean.class, DataTypes.BOOLEAN());
        addDefaultDataType(boolean.class, DataTypes.BOOLEAN());
        addDefaultDataType(Byte.class, DataTypes.TINYINT());
        addDefaultDataType(byte.class, DataTypes.TINYINT());
        addDefaultDataType(Short.class, DataTypes.SMALLINT());
        addDefaultDataType(short.class, DataTypes.SMALLINT());
        addDefaultDataType(Integer.class, DataTypes.INT());
        addDefaultDataType(int.class, DataTypes.INT());
        addDefaultDataType(Long.class, DataTypes.BIGINT());
        addDefaultDataType(long.class, DataTypes.BIGINT());
        addDefaultDataType(Float.class, DataTypes.FLOAT());
        addDefaultDataType(float.class, DataTypes.FLOAT());
        addDefaultDataType(Double.class, DataTypes.DOUBLE());
        addDefaultDataType(double.class, DataTypes.DOUBLE());
        addDefaultDataType(java.sql.Date.class, DataTypes.DATE());
        addDefaultDataType(java.time.LocalDate.class, DataTypes.DATE());
        addDefaultDataType(java.sql.Time.class, DataTypes.TIME(0));
        addDefaultDataType(java.time.LocalTime.class, DataTypes.TIME(9));
        addDefaultDataType(java.sql.Timestamp.class, DataTypes.TIMESTAMP(9));
        addDefaultDataType(java.time.LocalDateTime.class, DataTypes.TIMESTAMP(9));
        addDefaultDataType(java.time.OffsetDateTime.class, DataTypes.TIMESTAMP_WITH_TIME_ZONE(9));
        addDefaultDataType(java.time.Instant.class, DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9));
        addDefaultDataType(java.time.Duration.class, DataTypes.INTERVAL(DataTypes.SECOND(9)));
    }

    private static void addDefaultDataType(Class<?> clazz, DataType rootType) {
        final DataType dataType;
        if (clazz.isPrimitive()) {
            dataType = rootType.notNull();
        } else {
            dataType = rootType.nullable();
        }
        defaultDataTypes.put(clazz.getName(), dataType.bridgedTo(clazz));
    }

    /**
     * Returns the clearly identifiable data type if possible. For example, {@link Long} can be
     * expressed as {@link DataTypes#BIGINT()}. However, for example, {@link Row} cannot be extracted
     * as information about the fields is missing. Or {@link BigDecimal} needs to be mapped from a
     * variable precision/scale to constant ones.
     */
    @SuppressWarnings("unchecked")
    public static Optional<DataType> extractDataType(Class<?> clazz) {
        // byte arrays have higher priority than regular arrays
        if (clazz.equals(byte[].class)) {
            return Optional.of(DataTypes.BYTES());
        }

        if (clazz.isArray()) {
            return extractDataType(clazz.getComponentType())
                    .map(DataTypes::ARRAY);
        }

        if (TableSymbol.class.isAssignableFrom(clazz)) {
            return Optional.of(new AtomicDataType(new SymbolType(clazz)));
        }

        return Optional.ofNullable(defaultDataTypes.get(clazz.getName()));
    }

    private ClassDataTypeConverter() {
        // no instantiation
    }
}
