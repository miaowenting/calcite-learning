package com.matty.flink.sql.parser.dml;

import com.matty.flink.sql.parser.BaseParser;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-14
 */
public class RichSqlInsertTest extends BaseParser {
    private final static Logger LOG = LoggerFactory.getLogger(RichSqlInsertTest.class);

    @Test
    public void richSqlInsert() {
        String sql = "INSERT INTO table_sink\n" +
                "    SELECT\n" +
                "        d.id,\n" +
                "        d.name,\n" +
                "        d.channel,\n" +
                "        d.pv,\n" +
                "        d.xctime,\n" +
                "        d.info\n" +
                "    FROM\n" +
                "    (\n" +
                "        SELECT a.*,b.info\n" +
                "        FROM table_source a\n" +
                "        JOIN table_side b ON a.name = b.name and a.id = b.id\n" +
                "        JOIN table_side1 c ON a.channel = c.channel\n" +
                "        WHERE a.channel='channel1' AND a.pv > 0\n" +
                "    )as d";

        try {
            final SqlNode sqlNode = parseStmtAndHandleEx(sql);
            assert sqlNode instanceof RichSqlInsert;
            final RichSqlInsert richSqlInsert = (RichSqlInsert) sqlNode;

        } catch (Throwable t) {
            LOG.error("The rich insert sql is invalid.");
        }
    }


}
