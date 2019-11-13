package com.matty.flink.sql.converter.operation;

/**
 * Description:
 * Exception thrown during the execution of SQL statements.
 *
 * @author mwt
 * @version 1.0
 * @date 2019-11-13
 */
public class SqlConversionException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public SqlConversionException(String message) {
        super(message);
    }

    public SqlConversionException(String message, Throwable e) {
        super(message, e);
    }
}