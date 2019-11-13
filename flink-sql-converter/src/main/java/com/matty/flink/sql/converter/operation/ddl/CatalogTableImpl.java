package com.matty.flink.sql.converter.operation.ddl;

import com.matty.flink.sql.converter.table.TableSchema;

import java.util.*;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-11-13
 */
public class CatalogTableImpl extends AbstractCatalogTable {

    public CatalogTableImpl(
            TableSchema tableSchema,
            Map<String, String> properties,
            String comment) {
        super(tableSchema, properties, comment);
    }

    public CatalogTableImpl(
            TableSchema tableSchema,
            List<String> primaryKeys,
            Map<String, String> properties,
            String comment) {
        super(tableSchema, primaryKeys, properties, comment);
    }

    public CatalogTableImpl(TableSchema tableSchema,
                            List<String> primaryKeys,
                            Map<String, String> properties,
                            String comment,
                            String eventTimeField,
                            Long maxOutOrderless) {
        super(tableSchema, primaryKeys, properties, comment, eventTimeField, maxOutOrderless);
    }

    @Override
    public Map<String, String> toProperties() {
        return null;
    }

    @Override
    public CatalogBaseTable copy() {
        return new CatalogTableImpl(
                getSchema().copy(),
                new ArrayList<>(getPrimaryKeys()),
                new HashMap<>(getProperties()),
                getComment()
        );
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.of(getComment());
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.of("This is a catalog table");
    }
}
