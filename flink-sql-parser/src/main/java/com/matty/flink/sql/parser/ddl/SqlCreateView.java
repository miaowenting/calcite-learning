package com.matty.flink.sql.parser.ddl;

import com.matty.flink.sql.parser.ExtendedSqlNode;
import com.matty.flink.sql.parser.exception.SqlParseException;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-08
 */
public class SqlCreateView extends SqlCreate implements ExtendedSqlNode {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE_VIEW", SqlKind.CREATE_VIEW);

    private final SqlIdentifier viewName;
    private final SqlNodeList fieldList;
    private final SqlNode query;
    private final SqlCharStringLiteral comment;

    public SqlCreateView(
            SqlParserPos pos,
            SqlIdentifier viewName,
            SqlNodeList fieldList,
            SqlNode query,
            boolean replace,
            SqlCharStringLiteral comment) {
        super(OPERATOR, pos, replace, false);
        this.viewName = viewName;
        this.fieldList = fieldList;
        this.query = query;
        this.comment = comment;
    }

    @Override
    public List<SqlNode> getOperandList() {
        List<SqlNode> ops = new ArrayList<>();
        ops.add(viewName);
        ops.add(fieldList);
        ops.add(query);
        ops.add(SqlLiteral.createBoolean(getReplace(), SqlParserPos.ZERO));
        return ops;
    }

    public SqlIdentifier getViewName() {
        return viewName;
    }

    public SqlNodeList getFieldList() {
        return fieldList;
    }

    public SqlNode getQuery() {
        return query;
    }

    public SqlCharStringLiteral getComment() {
        return comment;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (getReplace()) {
            writer.keyword("OR REPLACE");
        }
        writer.keyword("VIEW");
        viewName.unparse(writer, leftPrec, rightPrec);
        if (fieldList.size() > 0) {
            fieldList.unparse(writer, 1, rightPrec);
        }
        if (comment != null) {
            writer.newlineAndIndent();
            writer.keyword("COMMENT");
            comment.unparse(writer, leftPrec, rightPrec);
        }
        writer.newlineAndIndent();
        writer.keyword("AS");
        writer.newlineAndIndent();
        query.unparse(writer, leftPrec, rightPrec);
    }

    private void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }

    @Override
    public void validate() throws SqlParseException {
        // no-op
    }
}

