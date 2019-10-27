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
public class SqlStringType extends SqlIdentifier implements ExtendedSqlType {
    public SqlStringType(SqlParserPos pos) {
        super("STRING", pos);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("STRING");
    }
}
