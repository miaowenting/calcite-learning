package com.matty.flink.sql.parser.dml;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-08
 */
public enum RichSqlInsertKeyword {
    OVERWRITE;

    /**
     * Creates a parse-tree node representing an occurrence of this keyword
     * at a particular position in the parsed text.
     */
    public SqlLiteral symbol(SqlParserPos pos) {
        return SqlLiteral.createSymbol(this, pos);
    }
}
