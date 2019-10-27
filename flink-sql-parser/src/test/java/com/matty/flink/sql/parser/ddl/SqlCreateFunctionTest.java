package com.matty.flink.sql.parser.ddl;

import com.matty.flink.sql.parser.BaseParser;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description:
 *  UDX自定义函数：
 *      UDF 自定义标量函数
 *      UDAF 自定义聚合函数
 *      UDTF 自定义表值函数
 * @author mwt
 * @version 1.0
 * @date 2019-10-14
 */
public class SqlCreateFunctionTest extends BaseParser {
    private final static Logger LOG = LoggerFactory.getLogger(SqlCreateFunctionTest.class);

    @Test
    public void createUdf() {
        String sql = "CREATE FUNCTION kafkapaser AS 'com.XXXX.kafkaUDF'";

        final SqlNode sqlNode = parseStmtAndHandleEx(sql);
        assert sqlNode instanceof SqlCreateFunction;
        final SqlCreateFunction sqlCreateFunction = (SqlCreateFunction) sqlNode;

        SqlIdentifier functionName = sqlCreateFunction.getFunctionName();
        LOG.debug("functionName: {}", functionName);

        String className = sqlCreateFunction.getClassNameString();
        LOG.debug("className: {}", className);

    }

    @Test
    public void createUdaf() {
        String sql = "CREATE FUNCTION kafkapaser AS 'com.XXXX.kafkaUDAF'";

        final SqlNode sqlNode = parseStmtAndHandleEx(sql);
        assert sqlNode instanceof SqlCreateFunction;
        final SqlCreateFunction sqlCreateFunction = (SqlCreateFunction) sqlNode;

        SqlIdentifier functionName = sqlCreateFunction.getFunctionName();
        LOG.debug("functionName: {}", functionName);

        String className = sqlCreateFunction.getClassNameString();
        LOG.debug("className: {}", className);

    }

    @Test
    public void createUdtf(){
        String sql = "CREATE FUNCTION splitUdtf AS 'com.hjc.test.blink.sql.udx.SplitUdtf'";

        final SqlNode sqlNode = parseStmtAndHandleEx(sql);
        assert sqlNode instanceof SqlCreateFunction;
        final SqlCreateFunction sqlCreateFunction = (SqlCreateFunction) sqlNode;

        SqlIdentifier functionName = sqlCreateFunction.getFunctionName();
        LOG.debug("functionName: {}", functionName);

        String className = sqlCreateFunction.getClassNameString();
        LOG.debug("className: {}", className);
    }
}
