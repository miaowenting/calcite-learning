package com.matty.flink.sql.converter.operation.ddl;

import com.matty.flink.sql.converter.table.TableSchema;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import scala.math.Ordering;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-11-13
 */
public abstract class AbstractCatalogTable implements CatalogTable {
    /**
     * Schema of the table (column names and types)
     */
    private final TableSchema tableSchema;
    /**
     * Partition keys if this is a partitioned table. It's an empty set if the table is not partitioned
     */
    private final List<String> primaryKeys;
    /**
     * Properties of the table
     */
    private final Map<String, String> properties;
    /**
     * Comment of the table
     */
    private final String comment;

    private String eventTimeField;

    private Long maxOutOrderless;

    AbstractCatalogTable(TableSchema tableSchema,
                         Map<String, String> properties,
                         String comment) {
        this(tableSchema, Lists.newArrayList(), properties, comment);
    }

    AbstractCatalogTable(TableSchema tableSchema,
                         List<String> primaryKeys,
                         Map<String, String> properties,
                         String comment) {
        this(tableSchema, primaryKeys, properties, comment, StringUtils.EMPTY, 0L);
    }

    AbstractCatalogTable(TableSchema tableSchema,
                         List<String> primaryKeys,
                         Map<String, String> properties,
                         String comment,
                         String eventTimeField,
                         Long maxOutOrderless) {
        this.tableSchema = requireNonNull(tableSchema, "tableSchema cannot be null");
        this.primaryKeys = requireNonNull(primaryKeys, "primaryKeys cannot be null");
        this.properties = requireNonNull(properties, "properties cannot be null");
        this.comment = comment;
        this.eventTimeField = eventTimeField;
        this.maxOutOrderless = maxOutOrderless;
    }

    @Override
    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public TableSchema getSchema() {
        return tableSchema;
    }

    @Override
    public String getComment() {
        return comment;
    }
}
