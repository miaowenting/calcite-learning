package com.matty.flink.sql.parser.ddl;

import com.matty.flink.sql.parser.ExtendedSqlNode;
import com.matty.flink.sql.parser.exception.SqlParseException;
import lombok.Data;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.NlsString;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-08
 */
@Data
public class SqlCreateTable extends SqlCreate implements ExtendedSqlNode {

    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    private final SqlIdentifier tableName;

    private final SqlNodeList columnList;

    private final SqlNodeList propertyList;

    private final SqlNodeList primaryKeyList;

    private final SqlCharStringLiteral comment;

    private final boolean sideFlag;

    private SqlIdentifier eventTimeField;

    private SqlNode maxOutOrderless;

    public SqlCreateTable(
            SqlParserPos pos,
            SqlIdentifier tableName,
            SqlNodeList columnList,
            SqlNodeList primaryKeyList,
            SqlNodeList propertyList,
            SqlCharStringLiteral comment,
            boolean sideFlag,
            SqlIdentifier eventTimeField,
            SqlNode maxOutOrderless) {
        super(OPERATOR, pos, false, false);
        this.tableName = requireNonNull(tableName, "Table name is missing");
        this.columnList = requireNonNull(columnList, "Column list should not be null");
        this.primaryKeyList = primaryKeyList;
        this.propertyList = propertyList;
        this.comment = comment;
        this.sideFlag = sideFlag;
        this.eventTimeField = eventTimeField;
        this.maxOutOrderless = maxOutOrderless;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tableName, columnList, primaryKeyList,
                propertyList, comment);
    }

    public Long getMaxOutOrderless() {
        return Long.parseLong(((NlsString) SqlLiteral.value(maxOutOrderless)).getValue());
    }


    @Override
    public void validate() throws SqlParseException {
        Set<String> columnNames = new HashSet<>();
        if (columnList != null) {
            for (SqlNode column : columnList) {
                String columnName = null;
                if (column instanceof SqlTableColumn) {
                    SqlTableColumn tableColumn = (SqlTableColumn) column;
                    columnName = tableColumn.getName().getSimple();
                    if (tableColumn.getAlias() != null) {
                        columnName = tableColumn.getAlias().getSimple();
                    }
                    String typeName = tableColumn.getType().getTypeName().getSimple();
                    if (SqlColumnType.getType(typeName).isUnsupported()) {
                        throw new SqlParseException(
                                column.getParserPosition(),
                                "Not support type [" + typeName + "], at " + column.getParserPosition());
                    }
                } else if (column instanceof SqlBasicCall) {
                    SqlBasicCall tableColumn = (SqlBasicCall) column;
                    columnName = tableColumn.getOperands()[1].toString();
                }

                if (!columnNames.add(columnName)) {
                    throw new SqlParseException(
                            column.getParserPosition(),
                            "Duplicate column name [" + columnName + "], at " +
                                    column.getParserPosition());
                }
            }
        }

        if (this.primaryKeyList != null) {
            for (SqlNode primaryKeyNode : this.primaryKeyList) {
                String primaryKey = ((SqlIdentifier) primaryKeyNode).getSimple();
                if (!columnNames.contains(primaryKey)) {
                    throw new SqlParseException(
                            primaryKeyNode.getParserPosition(),
                            "Primary key [" + primaryKey + "] not defined in columns, at " +
                                    primaryKeyNode.getParserPosition());
                }
            }
        }

    }

    public boolean containsComputedColumn() {
        for (SqlNode column : columnList) {
            if (column instanceof SqlBasicCall) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the projection format of the DDL columns(including computed columns).
     * e.g. If we got a DDL:
     * <pre>
     *   create table tbl1(
     *     col1 int,
     *     col2 varchar,
     *     col3 as to_timestamp(col2)
     *   ) with (
     *     'connector' = 'csv'
     *   )
     * </pre>
     * we would return a query like:
     *
     * <p>"col1, col2, to_timestamp(col2) as col3", caution that the "computed column" operands
     * have been reversed.
     */
    public String getColumnSqlString() {
        SqlPrettyWriter writer = new SqlPrettyWriter(AnsiSqlDialect.DEFAULT);
        writer.setAlwaysUseParentheses(true);
        writer.setSelectListItemsOnSeparateLines(false);
        writer.setIndentation(0);
        writer.startList("", "");
        for (SqlNode column : columnList) {
            writer.sep(",");
            if (column instanceof SqlTableColumn) {
                SqlTableColumn tableColumn = (SqlTableColumn) column;
                tableColumn.getName().unparse(writer, 0, 0);
            } else {
                column.unparse(writer, 0, 0);
            }
        }

        return writer.toString();
    }

    @Override
    public void unparse(
            SqlWriter writer,
            int leftPrec,
            int rightPrec) {
        writer.keyword("CREATE TABLE");
        tableName.unparse(writer, leftPrec, rightPrec);
        SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.create("sds"), "(", ")");
        for (SqlNode column : columnList) {
            printIndent(writer);
            if (column instanceof SqlBasicCall) {
                SqlCall call = (SqlCall) column;
                SqlCall newCall = call.getOperator().createCall(
                        SqlParserPos.ZERO,
                        call.operand(1),
                        call.operand(0));
                newCall.unparse(writer, leftPrec, rightPrec);
            } else {
                column.unparse(writer, leftPrec, rightPrec);
            }
        }

        if (primaryKeyList != null && primaryKeyList.size() > 0) {
            printIndent(writer);
            writer.keyword("PRIMARY KEY");
            SqlWriter.Frame keyFrame = writer.startList("(", ")");
            primaryKeyList.unparse(writer, leftPrec, rightPrec);
            writer.endList(keyFrame);
        }

        if (sideFlag) {
            printIndent(writer);
            writer.keyword("PERIOD FOR SYSTEM_TIME");
        }

        if (eventTimeField != null) {
            printIndent(writer);
            writer.keyword("WATERMARK FOR ");
            eventTimeField.unparse(writer, leftPrec, rightPrec);
            writer.keyword("AS withOffset");
            SqlWriter.Frame offsetFrame = writer.startList("(", ")");
            eventTimeField.unparse(writer, leftPrec, rightPrec);
            writer.keyword(",");
            maxOutOrderless.unparse(writer, leftPrec, rightPrec);
            writer.endList(offsetFrame);
        }

        writer.newlineAndIndent();
        writer.endList(frame);

        if (comment != null) {
            writer.newlineAndIndent();
            writer.keyword("COMMENT");
            comment.unparse(writer, leftPrec, rightPrec);
        }

        if (propertyList != null) {
            writer.keyword("WITH");
            SqlWriter.Frame withFrame = writer.startList("(", ")");
            for (SqlNode property : propertyList) {
                printIndent(writer);
                property.unparse(writer, leftPrec, rightPrec);
            }
            writer.newlineAndIndent();
            writer.endList(withFrame);
        }
    }

    private void printIndent(SqlWriter writer) {
        writer.sep(",", false);
        writer.newlineAndIndent();
        writer.print("  ");
    }

    /**
     * Table creation context.
     */
    public static class TableCreationContext {
        public List<SqlNode> columnList = new ArrayList<>();
        public SqlNodeList primaryKeyList;
        public boolean sideFlag;
        public SqlIdentifier eventTimeField;
        public SqlNode maxOutOrderless;
    }

    public String[] fullTableName() {
        return tableName.names.toArray(new String[0]);
    }
}