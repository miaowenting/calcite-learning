package com.matty.flink.sql.parser.ddl;

import com.matty.flink.sql.parser.type.ExtendedSqlType;
import lombok.Data;
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
@Data
public class SqlTableColumn extends SqlCall {
    private static final SqlSpecialOperator OPERATOR =
            new SqlSpecialOperator("COLUMN_DECL", SqlKind.COLUMN_DECL);

    private SqlIdentifier name;
    private SqlDataTypeSpec type;
    private SqlIdentifier alias;
    private SqlCharStringLiteral comment;

    public SqlTableColumn(SqlIdentifier name,
                          SqlDataTypeSpec type,
                          SqlIdentifier alias,
                          SqlCharStringLiteral comment,
                          SqlParserPos pos) {
        super(pos);
        this.name = requireNonNull(name, "Column name should not be null");
        this.type = requireNonNull(type, "Column type should not be null");
        this.alias = alias;
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
        if (this.alias != null) {
            writer.print(" AS ");
            this.alias.unparse(writer, leftPrec, rightPrec);
        }
        if (this.comment != null) {
            writer.print(" COMMENT ");
            this.comment.unparse(writer, leftPrec, rightPrec);
        }
    }

}