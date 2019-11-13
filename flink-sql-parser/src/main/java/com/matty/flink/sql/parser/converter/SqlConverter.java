package com.matty.flink.sql.parser.converter;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-10
 */
public interface SqlConverter<T> {

    T convert(String sql);

}
