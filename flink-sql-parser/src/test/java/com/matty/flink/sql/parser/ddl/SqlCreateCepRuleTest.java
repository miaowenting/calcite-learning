package com.matty.flink.sql.parser.ddl;

import com.matty.flink.sql.parser.BaseParser;
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
 * @date 2019-10-08
 */
public class SqlCreateCepRuleTest extends BaseParser {
    private final static Logger LOG = LoggerFactory.getLogger(SqlCreateCepRuleTest.class);

    @Test
    public void createKafkaSource() {
        String sql =
                " CREATE TABLE table_source(\n" +
                "    name varchar,\n" +
                "    channel varchar,\n" +
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

    @Test
    public void createKafkaSourceWithComputedColumn() {
        String sql =
                " CREATE TABLE table_source(\n" +
                "    name varchar,\n" +
                "    channel varchar,\n" +
                "    pv int,\n" +
                "    xctime bigint,\n" +
                "    timeLeng AS CHARACTER_LENGTH(channel) " +
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

    @Test
    public void createKafkaSourceWithComputedColumnForWatermark() {
        String sql = "CREATE TABLE table_source(\n" +
                "    channel varchar, -- 频道\n" +
                "    pv int,          -- 点击次数\n" +
                "    xctime varchar,  -- yyyyMMddHHmmss格式时间戳，字符串类型\n" +
                "    ts AS TO_TIMESTAMP(xctime,'yyyyMMddHHmmss'), -- rowtime,必须为TIMESTAMP类型\n" +
                "    WATERMARK FOR ts AS withOffset( ts , '120000' ) --watermark计算方法,允许2分钟的乱序时间,即允许数据迟到2分钟\n" +
                " )WITH(\n" +
                "    type='kafka11',\n" +
                "    kafka.bootstrap.servers='mwt:9092',\n" +
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

        LOG.debug("eventTimeField:{}", sqlCreateTable.getEventTimeField());

        LOG.debug("maxOutOrderless:{}", sqlCreateTable.getMaxOutOrderless());

        String nodeInfo = sqlNode.toString();
        LOG.debug("test allows secondary parsing: {}", nodeInfo);
    }

}

