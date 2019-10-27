package com.matty.flink.sql.parser.convertor;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-10
 */
public interface SqlConvertor<T> {

    T convert(String sql);

}
