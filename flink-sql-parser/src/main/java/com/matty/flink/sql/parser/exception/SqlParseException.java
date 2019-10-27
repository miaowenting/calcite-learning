package com.matty.flink.sql.parser.exception;

import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-11
 */
public class SqlParseException extends Exception {
    private SqlParserPos errorPos;
    private String message;

    public SqlParseException(SqlParserPos errorPos, String message) {
        this.errorPos = errorPos;
        this.message = message;
    }

    public SqlParseException(SqlParserPos errorPos, String message, Exception e) {
        super(e);
        this.errorPos = errorPos;
        this.message = message;
    }
}
