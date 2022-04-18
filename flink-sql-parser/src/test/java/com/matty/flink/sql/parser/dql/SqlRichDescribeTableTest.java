package com.matty.flink.sql.parser.dql;

import com.matty.flink.sql.parser.BaseParser;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description:
 * test DESCRIBE statement
 *
 * @author mwt
 * @version 1.0
 * @date 2022-04-18
 */
public class SqlRichDescribeTableTest extends BaseParser {
    private final static Logger LOG = LoggerFactory.getLogger(SqlRichDescribeTableTest.class);

    @Test
    public void describeTable() {
        String sql = "DESC Orders";

        final SqlNode sqlNode = parseStmtAndHandleEx(sql);
        assert sqlNode instanceof SqlRichDescribeTable;
        final SqlRichDescribeTable sqlRichDescribeTable = (SqlRichDescribeTable) sqlNode;

        // SqlRichDescribeTable -> DescribeTableOperation
        // catalogManager.getTable(ObjectIdentifier)
        String[] fullTableName = sqlRichDescribeTable.fullTableName();
        LOG.debug("fullTableName: {}", fullTableName);
    }
}
