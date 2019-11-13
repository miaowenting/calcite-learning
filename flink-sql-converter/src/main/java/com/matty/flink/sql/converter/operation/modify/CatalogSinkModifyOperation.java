package com.matty.flink.sql.converter.operation.modify;

import com.matty.flink.sql.converter.operation.query.QueryOperation;
import lombok.Data;

import java.util.List;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-11-13
 */
@Data
public class CatalogSinkModifyOperation implements ModifyOperation {

    private final List<String> tablePath;
    private final QueryOperation child;

    public CatalogSinkModifyOperation(List<String> tablePath, QueryOperation child){
        this.tablePath = tablePath;
        this.child = child;

    }
    @Override
    public QueryOperation getChild() {
        return child;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
