package com.matty.flink.sql.parser.type;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-08
 */
public class SqlBytesType extends SqlIdentifier implements ExtendedSqlType{
    public SqlBytesType(SqlParserPos pos) {
        super("BYTES", pos);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("BYTES");
    }
}
