package com.matty.flink.sql.converter.operation.ddl;

import com.matty.flink.sql.converter.table.TableSchema;

import java.util.Map;
import java.util.Optional;

public interface CatalogBaseTable {
    Map<String, String> getProperties();

    TableSchema getSchema();

    String getComment();

    CatalogBaseTable copy();

    Optional<String> getDescription();

    Optional<String> getDetailedDescription();
}