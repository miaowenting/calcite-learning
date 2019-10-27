package com.matty.flink.sql.parser;

import com.matty.flink.sql.parser.exception.SqlParseException;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-08
 */
public interface ExtendedSqlNode {

    void validate() throws SqlParseException;

}
