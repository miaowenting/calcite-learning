package com.matty.flink.sql.parser.dql;

import com.matty.flink.sql.parser.BaseParser;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Description:
 * test SHOW statements
 *
 * @author mwt
 * @version 1.0
 * @date 2022-04-18
 */
public class SqlShowTablesTest extends BaseParser {
    private final static Logger LOG = LoggerFactory.getLogger(SqlShowTablesTest.class);

    @Test
    public void testShowTables() {
        sql("show tables").ok("SHOW TABLES");
        sql("show tables not like '%'").ok("SHOW TABLES NOT LIKE '%'");

        sql("show tables from db1").ok("SHOW TABLES FROM `db1`");
        sql("show tables in db1").ok("SHOW TABLES IN `db1`");
    }

    @Test
    public void showTables() {
        String sql = "show tables";

        final SqlNode sqlNode = parseStmtAndHandleEx(sql);
        assert sqlNode instanceof SqlShowTables;
        final SqlShowTables sqlShowTables = (SqlShowTables) sqlNode;

        // SqlShowTables -> ShowTablesOperation -> catalogManager.listTables
        List<SqlNode> operandList = sqlShowTables.getOperandList();

        LOG.debug("operandList: {}", operandList);
    }


}
