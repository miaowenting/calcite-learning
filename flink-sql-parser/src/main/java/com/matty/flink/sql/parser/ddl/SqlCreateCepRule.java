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
public class SqlCreateCepRule extends SqlCreate implements ExtendedSqlNode {

    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE CEPRULE", SqlKind.OTHER_DDL);

    private final SqlIdentifier cepRuleName;
    private final SqlIdentifier event;
    private final SqlCharStringLiteral where;
    private final SqlIdentifier repeat;
    private final SqlIdentifier same;
    private final SqlIdentifier windowSize;
    private final SqlIdentifier windowUnit;
    private final SqlCharStringLiteral returnContent;

    public SqlCreateCepRule(
            SqlParserPos pos,
            SqlIdentifier cepRuleName,
            SqlIdentifier event,
            SqlCharStringLiteral where,
            SqlIdentifier repeat,
            SqlIdentifier same,
            SqlIdentifier windowSize,
            SqlIdentifier windowUnit,
            SqlCharStringLiteral returnContent) {
        super(OPERATOR, pos, false, false);
        this.cepRuleName = requireNonNull(cepRuleName, "cepRuleName is missing");
        this.event = requireNonNull(event, "event should not be null");
        this.where = requireNonNull(where, "where should not be null");
        this.repeat = requireNonNull(repeat, "repeat should not be null");
        this.same = requireNonNull(same, "same should not be null");
        this.windowSize = requireNonNull(windowSize, "windowSize should not be null");
        this.windowUnit = requireNonNull(windowUnit, "windowUnit should not be null");
        this.returnContent = requireNonNull(returnContent, "Column list should not be null");
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(cepRuleName, event, where, repeat, same, windowSize, windowUnit, returnContent);
    }

    @Override
    public void validate() throws SqlParseException {


    }



    @Override
    public void unparse(
            SqlWriter writer,
            int leftPrec,
            int rightPrec) {

    }

}