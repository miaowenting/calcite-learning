package com.matty.flink.sql.parser;

import com.matty.flink.sql.parser.type.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Util;

import java.nio.charset.Charset;
import java.util.Objects;
import java.util.TimeZone;
import java.util.stream.Collectors;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-08
 */
public class FlinkSqlDataTypeSpec extends SqlDataTypeSpec {
    private Boolean elementNullable;

    public FlinkSqlDataTypeSpec(
            SqlIdentifier collectionsTypeName,
            SqlIdentifier typeName,
            int precision,
            int scale,
            String charSetName,
            Boolean nullable,
            Boolean elementNullable,
            SqlParserPos pos) {
        super(collectionsTypeName, typeName, precision, scale,
                charSetName, null, nullable, pos);
        this.elementNullable = elementNullable;
    }

    public FlinkSqlDataTypeSpec(
            SqlIdentifier collectionsTypeName,
            SqlIdentifier typeName,
            int precision,
            int scale,
            String charSetName,
            TimeZone timeZone,
            Boolean nullable,
            Boolean elementNullable,
            SqlParserPos pos) {
        super(collectionsTypeName, typeName, precision, scale,
                charSetName, timeZone, nullable, pos);
        this.elementNullable = elementNullable;
    }

    public FlinkSqlDataTypeSpec(
            SqlIdentifier typeName,
            int precision,
            int scale,
            String charSetName,
            TimeZone timeZone,
            Boolean nullable,
            Boolean elementNullable,
            SqlParserPos pos) {
        super(null, typeName, precision, scale,
                charSetName, timeZone, nullable, pos);
        this.elementNullable = elementNullable;
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return (getCollectionsTypeName() != null)
                ? new FlinkSqlDataTypeSpec(getCollectionsTypeName(), getTypeName(), getPrecision(),
                getScale(), getCharSetName(), getNullable(), this.elementNullable, pos)
                : new FlinkSqlDataTypeSpec(getTypeName(), getPrecision(), getScale(),
                getCharSetName(), getTimeZone(), getNullable(), this.elementNullable, pos);
    }

    /** Returns a copy of this data type specification with a given
     * nullability. */
    @Override
    public SqlDataTypeSpec withNullable(Boolean nullable) {
        if (Objects.equals(nullable, this.getNullable())) {
            return this;
        }
        return new FlinkSqlDataTypeSpec(getCollectionsTypeName(), getTypeName(),
                getPrecision(), getScale(), getCharSetName(), getTimeZone(), nullable,
                this.elementNullable, getParserPosition());
    }

    @Override
    public RelDataType deriveType(RelDataTypeFactory typeFactory) {
        // Default to be nullable.
        return this.deriveType(typeFactory, true);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlIdentifier typeName = getTypeName();
        String name = typeName.getSimple();
        if (typeName instanceof ExtendedSqlType) {
            typeName.unparse(writer, leftPrec, rightPrec);
        } else if (SqlTypeName.get(name) != null) {
            SqlTypeName sqlTypeName = SqlTypeName.get(name);
            writer.keyword(name);
            if (sqlTypeName.allowsPrec() && this.getPrecision() >= 0) {
                SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
                writer.print(this.getPrecision());
                if (sqlTypeName.allowsScale() && this.getScale() >= 0) {
                    writer.sep(",", true);
                    writer.print(this.getScale());
                }

                writer.endList(frame);
            }

            if (this.getCharSetName() != null) {
                writer.keyword("CHARACTER SET");
                writer.identifier(this.getCharSetName(), false);
            }

            if (this.getCollectionsTypeName() != null) {
                // Fix up nullable attribute if this is a collection type.
                if (elementNullable != null && !elementNullable) {
                    writer.keyword("NOT NULL");
                }
                writer.keyword(this.getCollectionsTypeName().getSimple());
            }
        } else if (name.startsWith("_")) {
            writer.keyword(name.substring(1));
        } else {
            this.getTypeName().unparse(writer, leftPrec, rightPrec);
        }
        if (getNullable() != null && !getNullable()) {
            writer.keyword("NOT NULL");
        }
    }

    @Override
    public RelDataType deriveType(RelDataTypeFactory typeFactory, boolean nullable) {
        final SqlIdentifier typeName = getTypeName();
        if (!typeName.isSimple()) {
            return null;
        }
        final String name = typeName.getSimple();
        final SqlTypeName sqlTypeName = SqlTypeName.get(name);
        // Try to get Flink custom data type first.
        RelDataType type = getExtendedType(typeFactory, typeName);
        if (type == null) {
            if (sqlTypeName == null) {
                return null;
            } else {
                // NOTE jvs 15-Jan-2009:  earlier validation is supposed to
                // have caught these, which is why it's OK for them
                // to be assertions rather than user-level exceptions.
                final int precision = getPrecision();
                final int scale = getScale();
                if ((precision >= 0) && (scale >= 0)) {
                    assert sqlTypeName.allowsPrecScale(true, true);
                    type = typeFactory.createSqlType(sqlTypeName, precision, scale);
                } else if (precision >= 0) {
                    assert sqlTypeName.allowsPrecNoScale();
                    type = typeFactory.createSqlType(sqlTypeName, precision);
                } else {
                    assert sqlTypeName.allowsNoPrecNoScale();
                    type = typeFactory.createSqlType(sqlTypeName);
                }
            }
        }

        if (SqlTypeUtil.inCharFamily(type)) {
            // Applying Syntax rule 10 from SQL:99 spec section 6.22 "If TD is a
            // fixed-length, variable-length or large object character string,
            // then the collating sequence of the result of the <cast
            // specification> is the default collating sequence for the
            // character repertoire of TD and the result of the <cast
            // specification> has the Coercible coercibility characteristic."
            SqlCollation collation = SqlCollation.COERCIBLE;

            Charset charset;
            final String charSetName = getCharSetName();
            if (null == charSetName) {
                charset = typeFactory.getDefaultCharset();
            } else {
                String javaCharSetName =
                        Objects.requireNonNull(
                                SqlUtil.translateCharacterSetName(charSetName), charSetName);
                charset = Charset.forName(javaCharSetName);
            }
            type =
                    typeFactory.createTypeWithCharsetAndCollation(
                            type,
                            charset,
                            collation);
        }

        final SqlIdentifier collectionsTypeName = getCollectionsTypeName();
        if (null != collectionsTypeName) {
            // Fix the nullability of the element type first.
            boolean elementNullable = true;
            if (this.elementNullable != null) {
                elementNullable = this.elementNullable;
            }
            type = typeFactory.createTypeWithNullability(type, elementNullable);

            final String collectionName = collectionsTypeName.getSimple();
            final SqlTypeName collectionsSqlTypeName =
                    Objects.requireNonNull(SqlTypeName.get(collectionName),
                            collectionName);

            switch (collectionsSqlTypeName) {
                case MULTISET:
                    type = typeFactory.createMultisetType(type, -1);
                    break;
                case ARRAY:
                    type = typeFactory.createArrayType(type, -1);
                    break;
                default:
                    throw Util.unexpected(collectionsSqlTypeName);
            }
        }

        // Fix the nullability of this type.
        if (this.getNullable() != null) {
            nullable = this.getNullable();
        }
        type = typeFactory.createTypeWithNullability(type, nullable);

        return type;
    }

    private RelDataType getExtendedType(RelDataTypeFactory typeFactory, SqlIdentifier typeName) {
        // quick check.
        if (!(typeName instanceof ExtendedSqlType)) {
            return null;
        }
        if (typeName instanceof SqlBytesType) {
            return typeFactory.createSqlType(SqlTypeName.VARBINARY, Integer.MAX_VALUE);
        } else if (typeName instanceof SqlStringType) {
            return typeFactory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
        } else if (typeName instanceof SqlArrayType) {
            final SqlArrayType arrayType = (SqlArrayType) typeName;
            return typeFactory.createArrayType(arrayType.getElementType()
                    .deriveType(typeFactory), -1);
        } else if (typeName instanceof SqlMultisetType) {
            final SqlMultisetType multiSetType = (SqlMultisetType) typeName;
            return typeFactory.createMultisetType(multiSetType.getElementType()
                    .deriveType(typeFactory), -1);
        } else if (typeName instanceof SqlMapType) {
            final SqlMapType mapType = (SqlMapType) typeName;
            return typeFactory.createMapType(
                    mapType.getKeyType().deriveType(typeFactory),
                    mapType.getValType().deriveType(typeFactory));
        } else if (typeName instanceof SqlRowType) {
            final SqlRowType rowType = (SqlRowType) typeName;
            return typeFactory.createStructType(
                    rowType.getFieldTypes().stream().map(ft -> ft.deriveType(typeFactory))
                            .collect(Collectors.toList()),
                    rowType.getFieldNames().stream().map(SqlIdentifier::getSimple)
                            .collect(Collectors.toList()));
        } else if (typeName instanceof SqlTimeType) {
            final SqlTimeType zonedTimeType = (SqlTimeType) typeName;
            if (zonedTimeType.getPrecision() >= 0) {
                return typeFactory.createSqlType(zonedTimeType.getSqlTypeName(),
                        zonedTimeType.getPrecision());
            } else {
                // Use default precision.
                return typeFactory.createSqlType(zonedTimeType.getSqlTypeName());
            }
        } else if (typeName instanceof SqlTimestampType) {
            final SqlTimestampType zonedTimestampType = (SqlTimestampType) typeName;
            if (zonedTimestampType.getPrecision() >= 0) {
                return typeFactory.createSqlType(zonedTimestampType.getSqlTypeName(),
                        zonedTimestampType.getPrecision());
            } else {
                // Use default precision.
                return typeFactory.createSqlType(zonedTimestampType.getSqlTypeName());
            }
        }
        return null;
    }
}
