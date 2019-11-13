package com.matty.flink.sql.converter.operation.modify;

import com.matty.flink.sql.converter.operation.Operation;
import com.matty.flink.sql.converter.operation.query.QueryOperation;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-11-13
 */
public interface ModifyOperation extends Operation {
    QueryOperation getChild();

}
