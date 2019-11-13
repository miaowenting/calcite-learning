package com.matty.flink.sql.converter.type.logical;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-09
 */
public interface DataTypeVisitor<R> {

    R visit(AtomicDataType atomicDataType);

    R visit(CollectionDataType collectionDataType);

    R visit(FieldsDataType fieldsDataType);

    R visit(KeyValueDataType keyValueDataType);
}
