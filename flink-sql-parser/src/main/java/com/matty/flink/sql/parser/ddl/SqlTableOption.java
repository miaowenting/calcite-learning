package com.matty.flink.sql.parser.ddl;

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
 * @date 2019-10-08
 */
public class SqlTableOption extends SqlCall {

    protected static final SqlOperator OPERATOR =
            new SqlSpecialOperator("TableOption", SqlKind.OTHER);

    private final SqlIdentifier key;
    private final SqlNode value;

    public SqlTableOption(SqlIdentifier key, SqlNode value, SqlParserPos pos) {
        super(pos);
        this.key = requireNonNull(key, "Option key is missing");
        this.value = requireNonNull(value, "Option value is missing");
    }

    public SqlIdentifier getKey() {
        return key;
    }

    public SqlNode getValue() {
        return value;
    }

    public String getKeyString() {
        return key.toString();
    }

    public String getValueString() {
        return ((NlsString) SqlLiteral.value(value)).getValue();
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(key, value);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        key.unparse(writer, leftPrec, rightPrec);
        writer.keyword("=");
        value.unparse(writer, leftPrec, rightPrec);
    }
}