package com.matty.flink.sql.parser.ddl;

import com.matty.flink.sql.parser.BaseParser;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public void createCepRule() {
        String sql = "-- 这是行注释信息\n" +
                "/** 这是段注释信息 */" +
                "CREATE CEPRULE cep_rule(\n" +
                "  EVENT `e1->e2`,\n" +
                "  WHERE 'e1.event_code = S_PUBLISH_ITEM,e2.event_code = S_CREATE_ORDER AND e1.extra_info" +
                ".item_id = e2.extra_info.item_id AND e2.event_time - e1.event_time < 5',\n" +
                "  REPEAT `3` SAME `e2.user_id`,-- 行尾注释信息 \n" +
                "  WITHIN `1` `HOUR`,\n" +
                "  RETURN 'user_id=e2.user_id,item_id=e2.extra_info.item_id'\n" +
                ")\n";

        final SqlNode sqlNode = parseStmtAndHandleEx(sql);
        assert sqlNode instanceof SqlCreateCepRule;
        final SqlCreateCepRule sqlCreateCepRule = (SqlCreateCepRule) sqlNode;

        SqlIdentifier cepRuleName = sqlCreateCepRule.getCepRuleName();
        LOG.debug("cepRuleName: {}", cepRuleName);

        String event = sqlCreateCepRule.getEvent().getSimple();
        LOG.debug("event: {}", event);

        String where = sqlCreateCepRule.getWhere().toString();
        LOG.debug("where: {}", where);

        String repeat = sqlCreateCepRule.getRepeat().getSimple();
        LOG.debug("repeat: {}", repeat);

        String same = sqlCreateCepRule.getSame().getSimple();
        LOG.debug("same: {}", same);

        String windowSize = sqlCreateCepRule.getWindowSize().getSimple();
        LOG.debug("windowSize: {}", windowSize);

        String windowCount = sqlCreateCepRule.getWindowUnit().getSimple();
        LOG.debug("windowCount: {}", windowCount);

        String returnContent = sqlCreateCepRule.getReturnContent().toString();
        LOG.debug("returnContent: {}", returnContent);

    }


}

