package com.matty.flink.sql.parser.converter;

import com.matty.flink.sql.parser.ddl.SqlCreateTable;
import com.matty.flink.sql.parser.ddl.SqlTableOption;
import lombok.Data;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-10
 */
public class CreateTableSqlConverter extends BaseSqlConverter implements SqlConverter<CreateTableSqlConverter.CreateTableSqlConvertResult> {

    @Override
    public CreateTableSqlConvertResult convert(String sql) {
        final SqlNode sqlNode = parseStmtAndHandleEx(sql);
        assert sqlNode instanceof SqlCreateTable;
        final SqlCreateTable sqlCreateTable = (SqlCreateTable) sqlNode;

        String tableName = sqlCreateTable.getTableName().toString();

        SqlNodeList columns = sqlCreateTable.getColumnList();

        // set with properties
        SqlNodeList propertyList = sqlCreateTable.getPropertyList();
        Map<String, String> propMap = new HashMap<>(50);
        if (propertyList != null) {
            propertyList.getList().forEach(p ->
                    propMap.put(((SqlTableOption) p).getKeyString().toLowerCase(),
                            ((SqlTableOption) p).getValueString()));
        }

        List<String> primaryKeyList = null;
        if (sqlCreateTable.getPrimaryKeyList() != null) {
            String primaryKeys = sqlCreateTable.getPrimaryKeyList().toString().replace("`", "");
            primaryKeyList = Arrays.asList(primaryKeys.split(","));
        }

        SqlIdentifier eventTimeFieldIdentifier = sqlCreateTable.getEventTimeField();
        String eventTimeField = null;
        long maxOutOrderless = 0L;
        if (eventTimeFieldIdentifier != null) {
            eventTimeField = eventTimeFieldIdentifier.toString();
            maxOutOrderless = sqlCreateTable.getMaxOutOrderless();
        }

        return new CreateTableSqlConvertResult()
                .tableName(tableName)
                .fieldsInfoStr(columns.toString())
                .columns(columns)
                .primaryKeys(primaryKeyList)
                .propMap(propMap)
                .sideFlag(sqlCreateTable.isSideFlag())
                .eventTimeField(eventTimeField)
                .maxOutOrderless(maxOutOrderless)
                .containsComputedColumns(sqlCreateTable.containsComputedColumn());
    }

    @Data
    public static class CreateTableSqlConvertResult {
        /**
         * the table name
         */
        private String tableName;
        /**
         * the columns
         */
        private SqlNodeList columns;
        /**
         * fields string
         */
        private String fieldsInfoStr;

        private List<String> primaryKeys;
        /**
         * WITH properties map
         */
        private Map<String, String> propMap;
        /**
         * the flag of side table
         */
        private boolean sideFlag;
        /**
         * the event time field
         */
        private String eventTimeField;
        /**
         * the max milliseconds for orderless
         */
        private long maxOutOrderless;

        /**
         * contains computed columns or not
         */
        private boolean containsComputedColumns;

        public CreateTableSqlConvertResult tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        private CreateTableSqlConvertResult columns(SqlNodeList columns) {
            this.columns = columns;
            return this;
        }

        private CreateTableSqlConvertResult fieldsInfoStr(String fieldsInfoStr) {
            this.fieldsInfoStr = fieldsInfoStr;
            return this;
        }

        private CreateTableSqlConvertResult primaryKeys(List<String> primaryKeys) {
            this.primaryKeys = primaryKeys;
            return this;
        }


        private CreateTableSqlConvertResult propMap(Map<String, String> propMap) {
            this.propMap = propMap;
            return this;
        }

        private CreateTableSqlConvertResult sideFlag(boolean sideFlag) {
            this.sideFlag = sideFlag;
            return this;
        }

        private CreateTableSqlConvertResult eventTimeField(String eventTimeField) {
            this.eventTimeField = eventTimeField;
            return this;
        }

        private CreateTableSqlConvertResult maxOutOrderless(long maxOutOrderless) {
            this.maxOutOrderless = maxOutOrderless;
            return this;
        }

        private CreateTableSqlConvertResult containsComputedColumns(boolean containsComputedColumns) {
            this.containsComputedColumns = containsComputedColumns;
            return this;
        }

    }

}
