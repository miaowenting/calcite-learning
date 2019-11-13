package com.matty.flink.sql.converter.operation.ddl;

import lombok.Data;

/**
 * Description:
 * Operation to describe a CREATE TABLE statement.
 *
 * @author mwt
 * @version 1.0
 * @date 2019-11-13
 */
@Data
public class CreateTableOperation implements CreateOperation {

    private final String[] tablePath;
    private CatalogTable catalogTable;

    public CreateTableOperation(String[] tablePath,
                                CatalogTable catalogTable) {
        this.tablePath = tablePath;
        this.catalogTable = catalogTable;
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
