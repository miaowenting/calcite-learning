package com.matty.flink.sql.parser.type;

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-08
 */
public class SqlMapType extends SqlIdentifier implements ExtendedSqlType{
    private final SqlDataTypeSpec keyType;
    private final SqlDataTypeSpec valType;

    public SqlMapType(SqlParserPos pos, SqlDataTypeSpec keyType, SqlDataTypeSpec valType) {
        super(SqlTypeName.MAP.getName(), pos);
        this.keyType = keyType;
        this.valType = valType;
    }

    public SqlDataTypeSpec getKeyType() {
        return keyType;
    }

    public SqlDataTypeSpec getValType() {
        return valType;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("MAP");
        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "<", ">");
        writer.sep(",");
        ExtendedSqlType.unparseType(keyType, writer, leftPrec, rightPrec);
        writer.sep(",");
        ExtendedSqlType.unparseType(valType, writer, leftPrec, rightPrec);
        writer.endList(frame);
    }
}
