package com.matty.flink.sql.converter.operation.query;

import com.matty.flink.sql.converter.table.TableSchema;
import com.matty.flink.sql.converter.type.logical.DataType;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter;

import java.util.Collections;
import java.util.List;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-11-13
 */
public class PlannerQueryOperation implements QueryOperation {

    private final RelNode calciteTree;
    private final TableSchema tableSchema;

    public PlannerQueryOperation(RelNode calciteTree) {
        this.calciteTree = calciteTree;

        RelDataType rowType = calciteTree.getRowType();
        String[] fieldNames = rowType.getFieldNames().toArray(new String[0]);
        DataType[] fieldTypes = rowType.getFieldList()
                .stream()
                .map(field ->
                        LogicalTypeDataTypeConverter.fromLogicalTypeToDataType(
                                FlinkTypeFactory.toLogicalType(field.getType())))
                .toArray(DataType[]::new);

        this.tableSchema = TableSchema.builder().fields(fieldNames, fieldTypes).build();
    }

    public RelNode getCalciteTree() {
        return calciteTree;
    }

    @Override
    public TableSchema getTableSchema() {
        return tableSchema;
    }

    @Override
    public List<QueryOperation> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
