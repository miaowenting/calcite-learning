package com.matty.flink.sql.converter.operation.ddl;

import java.util.List;
import java.util.Map;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-11-13
 */
public interface CatalogTable extends CatalogBaseTable {

    List<String> getPrimaryKeys();

    /**
     * Return a property map for table factory discovery purpose. The properties will be used to match a [[TableFactory]].
     * Please refer to {@link org.apache.flink.table.factories.TableFactory}
     *
     * @return a map of properties
     */
    Map<String, String> toProperties();
}
