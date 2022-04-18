package com.matty.flink.sql.parser;

import com.matty.flink.sql.parser.impl.SqlParserImpl;
import com.matty.flink.sql.parser.validate.FlinkSqlConformance;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserTest;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-14
 */
public class BaseParser extends SqlParserTest  {

    private FlinkSqlConformance conformance = FlinkSqlConformance.DEFAULT;

    protected SqlNode parseStmtAndHandleEx(String sql) {
        final SqlNode sqlNode;
        try {
            sqlNode = getSqlParser(sql).parseStmt();
        } catch (SqlParseException e) {
            throw new RuntimeException("Error while parsing SQL: " + sql, e);
        }
        return sqlNode;
    }

    public SqlParser getSqlParser(String sql) {
        return SqlParser.create(sql,
                SqlParser.configBuilder()
                        .setParserFactory(SqlParserImpl.FACTORY)
                        .setQuoting(Quoting.BACK_TICK)
                        .setUnquotedCasing(Casing.UNCHANGED)
                        .setQuotedCasing(Casing.UNCHANGED)
                        .setConformance(conformance)
                        .build());
    }
}
