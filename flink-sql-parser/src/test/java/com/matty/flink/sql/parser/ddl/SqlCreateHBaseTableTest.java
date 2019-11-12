package com.matty.flink.sql.parser.ddl;

import com.matty.flink.sql.parser.BaseParser;
import com.matty.flink.sql.parser.exception.SqlParseException;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-11-11
 */
public class SqlCreateHBaseTableTest extends BaseParser {
    private final static Logger LOG = LoggerFactory.getLogger(SqlCreateHBaseTableTest.class);

    @Test
    public void createHBaseSide() {
        String sql = "CREATE TABLE table_side(\n" +
                "    `cf:name` varchar as name,\n" +
                "    `cf:info` varchar as info,\n" +
                "    PRIMARY KEY(name),\n" +
                "    PERIOD FOR SYSTEM_TIME\n" +
                " )WITH(\n" +
                "    type ='hbase',\n" +
                "    zookeeperQuorum ='localhost:2181',\n" +
                "    zookeeperParent ='/hbase',\n" +
                "    tableName ='table_side',\n" +
                "    cache ='ALL'\n" +
                " )";
        final SqlNode sqlNode = parseStmtAndHandleEx(sql);
        assert sqlNode instanceof SqlCreateTable;
        final SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNode;

        boolean valid = false;
        try {
            sqlCreateTable.validate();
            valid = true;
        } catch (SqlParseException e) {
            e.printStackTrace();
        }

        LOG.debug("The sql is valid: {}", valid);

        SqlNodeList columns = sqlCreateTable.getColumnList();
        LOG.debug("columns: {}", columns);

        SqlNodeList properties = sqlCreateTable.getPropertyList();
        LOG.debug("properties: {}", properties);

        SqlNodeList primaryKey = sqlCreateTable.getPrimaryKeyList();
        LOG.debug("primaryKey: {}", primaryKey);

        boolean sideFlag = sqlCreateTable.isSideFlag();
        LOG.debug("sideFlag: {}", sideFlag);

        String nodeInfo = sqlNode.toString();
        LOG.debug("test allows secondary parsing: {}", nodeInfo);
    }
}
