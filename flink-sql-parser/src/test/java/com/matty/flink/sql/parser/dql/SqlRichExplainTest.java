package com.matty.flink.sql.parser.dql;

import com.matty.flink.sql.parser.BaseParser;
import com.matty.flink.sql.parser.ddl.SqlCreateCepRule;
import com.matty.flink.sql.parser.ddl.SqlCreateCepRuleTest;
import com.matty.flink.sql.parser.dml.SqlStatementSet;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * Description:
 * test EXPLAIN statement
 *
 * @author mwt
 * @version 1.0
 * @date 2022-04-18
 */
public class SqlRichExplainTest extends BaseParser {
    private final static Logger LOG = LoggerFactory.getLogger(SqlRichExplainTest.class);

    @Test
    public void explain() {
        String sql = "EXPLAIN PLAN FOR\n" +
                "  SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' \n" +
                "  UNION ALL \n" +
                "  SELECT `count`, word FROM MyTable2";

        final SqlNode sqlNode = parseStmtAndHandleEx(sql);

        // TableEnvironmentImpl.executeInternal(Operation)
        // 1. 获取 explainDetails
        // 2. 获取 operations
        // 3. explainInternal(operations, explainDetails) -> planner.explain(operations, explainDetails) -> PlannerBase#getExplainGraph(operations)
        assert sqlNode instanceof SqlRichExplain;
        final SqlRichExplain sqlRichExplain = (SqlRichExplain) sqlNode;

        // ExplainOperation(PlannerQueryOperation)
        SqlNode sqlQueryNode = sqlRichExplain.getStatement();
        assert sqlQueryNode.getKind().belongsTo(SqlKind.QUERY);

        Set<String> explainDetails = sqlRichExplain.getExplainDetails();
        LOG.debug("explainDetails: {}", explainDetails);

        List<SqlNode> operandList = sqlRichExplain.getOperandList();
        LOG.debug("operandList: {}", operandList);

    }

    @Test
    public void explainForCBO() {
        String sql = "EXPLAIN ESTIMATED_COST, CHANGELOG_MODE, JSON_EXECUTION_PLAN \n" +
                "  SELECT `count`, word FROM MyTable1 WHERE word LIKE 'F%' \n" +
                "  UNION ALL \n" +
                "  SELECT `count`, word FROM MyTable2";

        final SqlNode sqlNode = parseStmtAndHandleEx(sql);
        assert sqlNode instanceof SqlRichExplain;
        final SqlRichExplain sqlRichExplain = (SqlRichExplain) sqlNode;

        // ExplainOperation(PlannerQueryOperation)
        SqlNode sqlQueryNode = sqlRichExplain.getStatement();
        assert sqlQueryNode.getKind().belongsTo(SqlKind.QUERY);

        Set<String> explainDetails = sqlRichExplain.getExplainDetails();
        LOG.debug("explainDetails: {}", explainDetails);

        List<SqlNode> operandList = sqlRichExplain.getOperandList();
        LOG.debug("operandList: {}", operandList);
    }


}
