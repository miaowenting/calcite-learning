package com.matty.flink.sql.parser.ddl;

import com.matty.flink.sql.parser.ExtendedSqlNode;
import com.matty.flink.sql.parser.exception.SqlParseException;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-14
 */
public class SqlCreateFunction extends SqlCreate implements ExtendedSqlNode {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE FUNCTION", SqlKind.CREATE_FUNCTION);

    private final SqlIdentifier functionName;
    private final SqlNode className;

    public SqlCreateFunction(SqlParserPos pos,
                             SqlIdentifier functionName,
                             SqlNode className) {
        super(OPERATOR, pos, false, false);
        this.functionName = requireNonNull(functionName, "Function name is missing");
        this.className = requireNonNull(className, "Class name is missing");
    }

    public SqlIdentifier getFunctionName() {
        return functionName;
    }

    public SqlNode getClassName() {
        return className;
    }

    public String getClassNameString() {
        return ((NlsString) SqlLiteral.value(className)).getValue();
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(functionName, className);
    }

    @Override
    public void validate() throws SqlParseException {
        // no-op
    }
}
