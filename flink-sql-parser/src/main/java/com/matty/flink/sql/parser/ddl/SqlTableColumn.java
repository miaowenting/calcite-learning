package com.matty.flink.sql.parser.ddl;

import com.matty.flink.sql.parser.type.ExtendedSqlType;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-08
 */
public class SqlTableColumn extends SqlCall {
    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL);

    private SqlIdentifier name;
    private SqlDataTypeSpec type;
    private SqlCharStringLiteral comment;

    public SqlTableColumn(SqlIdentifier name,
                          SqlDataTypeSpec type,
                          SqlCharStringLiteral comment,
                          SqlParserPos pos) {
        super(pos);
        this.name = requireNonNull(name, "Column name should not be null");
        this.type = requireNonNull(type, "Column type should not be null");
        this.comment = comment;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, type, comment);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        this.name.unparse(writer, leftPrec, rightPrec);
        writer.print(" ");
        ExtendedSqlType.unparseType(type, writer, leftPrec, rightPrec);
        if (this.comment != null) {
            writer.print(" COMMENT ");
            this.comment.unparse(writer, leftPrec, rightPrec);
        }
    }

    public SqlIdentifier getName() {
        return name;
    }

    public void setName(SqlIdentifier name) {
        this.name = name;
    }

    public SqlDataTypeSpec getType() {
        return type;
    }

    public void setType(SqlDataTypeSpec type) {
        this.type = type;
    }

    public SqlCharStringLiteral getComment() {
        return comment;
    }

    public void setComment(SqlCharStringLiteral comment) {
        this.comment = comment;
    }
}
