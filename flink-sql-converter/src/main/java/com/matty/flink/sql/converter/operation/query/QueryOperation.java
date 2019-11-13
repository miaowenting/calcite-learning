package com.matty.flink.sql.converter.operation.query;

import com.matty.flink.sql.converter.operation.Operation;
import com.matty.flink.sql.converter.table.TableSchema;

import java.util.List;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-11-13
 */
public interface QueryOperation extends Operation {

    TableSchema getTableSchema();

    List<QueryOperation> getChildren();


}
