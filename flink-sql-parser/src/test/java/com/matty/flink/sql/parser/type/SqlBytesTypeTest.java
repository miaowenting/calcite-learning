package com.matty.flink.sql.parser.type;

import com.matty.flink.sql.parser.BaseParser;
import com.matty.flink.sql.parser.ddl.SqlCreateTable;
import com.matty.flink.sql.parser.ddl.SqlTableOption;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-11-11
 */
public class SqlBytesTypeTest extends BaseParser {
    private final static Logger LOG = LoggerFactory.getLogger(SqlBytesTypeTest.class);

    @Test
    public void createKafkaSourceWithBytesType() {
        String sql =
                " CREATE TABLE table_source(\n" +
                        "    name varchar,\n" +
                        "    channel string,\n" +
                        "    b1 varbinary,\n" +
                        "    b2 bytes,\n" +
                        "    pv int,\n" +
                        "    xctime bigint\n" +
                        " )WITH(\n" +
                        "    type='kafka11',\n" +
                        "    kafka.bootstrap.servers='mwt:9092',\n" +
                        "    kafka.zookeeper.quorum='localhost:2181',\n" +
                        "    kafka.auto.offset.reset='latest',\n" +
                        "    kafka.kerberos.enabled='false',\n" +
                        "    kafka.data.type='json',\n" +
                        "    kafka.topic='table_source',\n" +
                        "    parallelism='1'\n" +
                        " )\n";
        final SqlNode sqlNode = parseStmtAndHandleEx(sql);
        assert sqlNode instanceof SqlCreateTable;
        final SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNode;

        SqlIdentifier tableName = sqlCreateTable.getTableName();
        LOG.debug("tableName: {}", tableName);

        SqlNodeList columns = sqlCreateTable.getColumnList();
        LOG.debug("columns: {}", columns);

        // set with properties
        SqlNodeList propertyList = sqlCreateTable.getPropertyList();
        Map<String, String> properties = new HashMap<>();
        if (propertyList != null) {
            propertyList.getList().forEach(p ->
                    properties.put(((SqlTableOption) p).getKeyString().toLowerCase(),
                            ((SqlTableOption) p).getValueString()));
        }
        LOG.debug("properties: {}", properties);

    }
}