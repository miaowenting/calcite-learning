package com.matty.flink.sql.parser.ddl;

import com.matty.flink.sql.parser.BaseParser;
import com.matty.flink.sql.parser.exception.SqlParseException;
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
public class SqlCreateTableTest extends BaseParser {
    private final static Logger LOG = LoggerFactory.getLogger(SqlCreateTableTest.class);

    @Test
    public void createKafkaSource() {
        String sql = "-- 测试console sink\n" +
                "\n" +
                "\n" +
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
        String sql = "-- 测试console sink\n" +
                "\n" +
                "\n" +
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
    public void createMysqlSide() {
        String sql = "CREATE TABLE table_side(\n" +
                "    name varchar,\n" +
                "    info varchar,\n" +
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
        LOG.debug("columns: {}", columns);

        SqlNodeList properties = sqlCreateTable.getPropertyList();
        LOG.debug("properties: {}", properties);

        SqlNodeList primaryKey = sqlCreateTable.getPrimaryKeyList();
        LOG.debug("primaryKey: {}", primaryKey);

    }


    @Test
    public void createKafkaSourceWithWatermark() {

        String sql = "-- 测试console sink\n" +
                "\n" +
                "\n" +
                " CREATE TABLE table_source(\n" +
                "    name varchar,\n" +
                "    channel varchar,\n" +
                "    pv int,\n" +
                "    xctime timestamp,\n" +
                "    WATERMARK FOR xctime AS withOffset( xctime , '12000') \n" +
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

        LOG.debug("eventTimeField:{}", sqlCreateTable.getEventTimeField());

        LOG.debug("maxOutOrderless:{}", sqlCreateTable.getMaxOutOrderless());
    }


}
