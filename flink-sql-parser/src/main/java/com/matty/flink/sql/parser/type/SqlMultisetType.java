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
public class SqlMultisetType extends SqlIdentifier implements ExtendedSqlType{
    private final SqlDataTypeSpec elementType;

    public SqlMultisetType(SqlParserPos pos, SqlDataTypeSpec elementType) {
        super(SqlTypeName.MULTISET.getName(), pos);
        this.elementType = elementType;
    }

    public SqlDataTypeSpec getElementType() {
        return elementType;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("MULTISET<");
        ExtendedSqlType.unparseType(this.elementType, writer, leftPrec, rightPrec);
        writer.keyword(">");
    }
}
