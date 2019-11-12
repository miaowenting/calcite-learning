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
public class SqlCreateMysqlTableTest extends BaseParser {
    private final static Logger LOG = LoggerFactory.getLogger(SqlCreateMysqlTableTest.class);

    @Test
    public void createMysqlSide() {
        String sql = "CREATE TABLE table_side(\n" +
                // allow comment
                "    name varchar comment '名称',\n" +
                "    info varchar comment '详细信息',\n" +
                "    PRIMARY KEY(name),\n" +
                "    PERIOD FOR SYSTEM_TIME\n" +
                " )WITH(\n" +
                "    type ='mysql',\n" +
                "    url ='jdbc:mysql://192.168.1.8:3306/demo?charset=utf8',\n" +
                "    userName ='dipper',\n" +
                "    password ='ide@123',\n" +
                "    tableName ='table_side',\n" +
                "    cache ='NONE',\n" +
                "    parallelism ='1'\n" +
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
        LOG.debug("columns: {}", columns.toString());
        for (SqlNode column : columns) {
            SqlTableColumn tableColumn = (SqlTableColumn) column;
            String columnName = tableColumn.getName().getSimple();
            String typeName = tableColumn.getType().getTypeName().getSimple();
            String comment = tableColumn.getComment().toString();
            LOG.debug("columnName: {}, typeName: {}, comment: {}", columnName, typeName, comment);
        }

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
