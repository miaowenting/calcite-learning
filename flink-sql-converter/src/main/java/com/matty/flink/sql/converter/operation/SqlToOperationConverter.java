package com.matty.flink.sql.converter.operation;

import com.matty.flink.sql.converter.operation.ddl.CatalogTable;
import com.matty.flink.sql.converter.operation.ddl.CatalogTableImpl;
import com.matty.flink.sql.converter.operation.ddl.CreateTableOperation;
import com.matty.flink.sql.converter.operation.modify.CatalogSinkModifyOperation;
import com.matty.flink.sql.converter.operation.query.PlannerQueryOperation;
import com.matty.flink.sql.converter.table.TableSchema;
import com.matty.flink.sql.converter.type.util.TypeConversions;
import com.matty.flink.sql.parser.ddl.*;
import com.matty.flink.sql.parser.dml.RichSqlInsert;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-11-13
 */
public class SqlToOperationConverter {

    private FlinkPlannerImpl flinkPlanner;

    private SqlToOperationConverter(FlinkPlannerImpl flinkPlanner) {
        this.flinkPlanner = flinkPlanner;
    }

    public static Operation convert(FlinkPlannerImpl flinkPlanner, SqlNode sqlNode) {
        final SqlNode validated = flinkPlanner.validate(sqlNode);
        SqlToOperationConverter converter = new SqlToOperationConverter(flinkPlanner);
        if (validated instanceof SqlCreateTable) {
            return converter.convertCreateTable((SqlCreateTable) validated);
        } else if (validated instanceof SqlCreateView) {
            // TODO
            return null;
        } else if (validated instanceof SqlCreateFunction) {
            // TODO
            return null;
        } else if (validated instanceof RichSqlInsert) {
            return converter.convertSqlInsert((RichSqlInsert) validated);
        } else if (validated.getKind().belongsTo(SqlKind.QUERY)) {
            return converter.convertSqlQuery(validated);
        } else {
            throw new RuntimeException("Unsupported node type " + sqlNode.getClass().getSimpleName());
        }
    }

    private Operation convertCreateTable(SqlCreateTable sqlCreateTable) {
        String tableName = sqlCreateTable.getTableName().toString();

        // table schema
        SqlNodeList columns = sqlCreateTable.getColumnList();
        TableSchema tableSchema = createTableSchema(columns,
                new FlinkTypeFactory(new FlinkTypeSystem()));

        String tableComment = "";
        if (sqlCreateTable.getComment() != null) {
            tableComment = sqlCreateTable.getComment().getNlsString().getValue();
        }

        // set with properties
        SqlNodeList propertyList = sqlCreateTable.getPropertyList();
        Map<String, String> propMap = new HashMap<>(50);
        if (propertyList != null) {
            propertyList.getList().forEach(p ->
                    propMap.put(((SqlTableOption) p).getKeyString().toLowerCase(),
                            ((SqlTableOption) p).getValueString()));
        }

        // primary keys
        List<String> primaryKeyList = null;
        if (sqlCreateTable.getPrimaryKeyList() != null) {
            String primaryKeys = sqlCreateTable.getPrimaryKeyList().toString().replace("`", "");
            primaryKeyList = Arrays.asList(primaryKeys.split(","));
        }

        // eventTimeField and maxOutOrderless for watermark
        SqlIdentifier eventTimeFieldIdentifier = sqlCreateTable.getEventTimeField();
        String eventTimeField = null;
        long maxOutOrderless = 0L;
        if (eventTimeFieldIdentifier != null) {
            eventTimeField = eventTimeFieldIdentifier.toString();
            maxOutOrderless = sqlCreateTable.getMaxOutOrderless();
        }

        CatalogTable catalogTable = new CatalogTableImpl(tableSchema, primaryKeyList, propMap, tableComment,
                eventTimeField, maxOutOrderless);

        return new CreateTableOperation(sqlCreateTable.fullTableName(), catalogTable);
    }

    /**
     * Create table schema
     */
    private static TableSchema createTableSchema(SqlNodeList columns,
                                                 FlinkTypeFactory factory) {
        // setup table columns
        TableSchema physicalSchema = null;
        TableSchema.Builder builder = new TableSchema.Builder();
        // collect the physical table schema first.
        final List<SqlNode> physicalColumns = columns.getList().stream()
                .filter(n -> n instanceof SqlTableColumn).collect(Collectors.toList());
        for (SqlNode node : physicalColumns) {
            SqlTableColumn column = (SqlTableColumn) node;
            final RelDataType relType = column.getType().deriveType(factory,
                    column.getType().getNullable());
            builder.field(column.getAlias() != null ? column.getAlias().getSimple() : column.getName().getSimple(),
                    TypeConversions.fromLegacyInfoToDataType(org.apache.flink.table.calcite.FlinkTypeFactory.toTypeInfo(relType)));
            physicalSchema = builder.build();
        }

        assert physicalSchema != null;
        return physicalSchema;
    }

    private Operation convertSqlInsert(RichSqlInsert richSqlInsert) {
        List<String> targetTablePath = ((SqlIdentifier) richSqlInsert.getTargetTable()).names;
        return new CatalogSinkModifyOperation(
                targetTablePath,
                (PlannerQueryOperation) convert(flinkPlanner, richSqlInsert.getSource()));
    }

    private Operation convertSqlQuery(SqlNode validated) {
        return toQueryOperation(validated);
    }

    private PlannerQueryOperation toQueryOperation(SqlNode validated) {
        RelRoot relational = flinkPlanner.rel(validated);
        return new PlannerQueryOperation(relational.project());
    }


}
