package com.matty.flink.sql.parser.ddl;

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
public class SqlCreateViewTest extends BaseParser {
    private final static Logger LOG = LoggerFactory.getLogger(SqlCreateViewTest.class);

    @Test
    public void createViewAsSelect() {
        String sql = "CREATE VIEW table_sink_view AS\n" +
                "SELECT\n" +
                "   xctime,\n" +
                "   1 as pv\n" +
                "FROM table_source\n" +
                "GROUP BY name,xctime";

        final SqlNode sqlNode = parseStmtAndHandleEx(sql);
        assert sqlNode instanceof SqlCreateView;
        final SqlCreateView sqlCreateView = (SqlCreateView) sqlNode;

        LOG.debug("query: {}", sqlCreateView.getQuery());
        LOG.debug("viewName: {}", sqlCreateView.getViewName());
    }

    @Test
    public void createViewAsSelectWithLateralTable() {
        String sql = "CREATE VIEW table_sink_view AS\n" +
                " SELECT\n" +
                "    name,\n" +
                "    channel,\n" +
                "    pv,\n" +
                "    xctime,\n" +
                "    splitChannel\n" +
                " FROM table_source,\n" +
                " LATERAL TABLE(SplitUdtf(channel)) AS T(splitChannel)";

        final SqlNode sqlNode = parseStmtAndHandleEx(sql);
        assert sqlNode instanceof SqlCreateView;
        final SqlCreateView sqlCreateView = (SqlCreateView) sqlNode;

        LOG.debug("query: {}", sqlCreateView.getQuery());
        LOG.debug("viewName: {}", sqlCreateView.getViewName());
    }

    @Test
    public void invalidCreateViewSql() {
        String sql = "CREATE VIEW table_sink_view (\n" +
                "    name varchar,\n" +
                "    channel varchar,\n" +
                "    pv int,\n" +
                "    xctime bigint)";

        try {
            final SqlNode sqlNode = parseStmtAndHandleEx(sql);
            assert sqlNode instanceof SqlCreateView;
            final SqlCreateView sqlCreateView = (SqlCreateView) sqlNode;
        } catch (Throwable t) {
            LOG.error("The create view sql is invalid.");
        }
    }

}
