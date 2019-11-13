package com.matty.flink.sql.converter.type.util;

public class SqlConversionException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public SqlConversionException(String message) {
		super(message);
	}

	public SqlConversionException(String message, Throwable e) {
		super(message, e);
	}
}