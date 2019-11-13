package com.matty.flink.sql.converter.operation;

import com.matty.flink.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.operations.CatalogSinkModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.catalog.CatalogManagerCalciteSchema;
import org.apache.flink.table.planner.delegation.PlannerContext;
import org.apache.flink.table.planner.operations.SqlToOperationConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.apache.calcite.jdbc.CalciteSchemaBuilder.asRootSchema;
import static org.apache.calcite.sql.parser.SqlParser.DEFAULT_IDENTIFIER_MAX_LENGTH;
import static org.junit.Assert.assertEquals;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-11-13
 */
public class SqlToOperationConverterTest {
    private final TableConfig tableConfig = new TableConfig();

    private final Catalog catalog = new GenericInMemoryCatalog("MockCatalog",
            "default");
    private final CatalogManager catalogManager =
            new CatalogManager("builtin", catalog);

    private final FunctionCatalog functionCatalog = new FunctionCatalog(catalogManager);

    private final PlannerContext plannerContext =
            new PlannerContext(tableConfig,
                    functionCatalog,
                    asRootSchema(new CatalogManagerCalciteSchema(catalogManager, false)),
                    new ArrayList<>());

    @Before
    public void before() throws TableAlreadyExistException, DatabaseNotExistException {

        final ObjectPath path1 = new ObjectPath(catalogManager.getCurrentDatabase(), "table_source");
        final TableSchema tableSchema1 = TableSchema.builder()
                .field("name", DataTypes.VARCHAR(Integer.MAX_VALUE))
                .field("channel", DataTypes.VARCHAR(Integer.MAX_VALUE))
                .field("pv", DataTypes.INT())
                .build();
        Map<String, String> properties1 = new HashMap<>();
        properties1.put("type", "kafka11");
        properties1.put("kafka.bootstrap.servers", "mwt:9092");
        properties1.put("kafka.auto.offset.reset", "latest");
        properties1.put("kafka.kerberos.enabled", "false");
        properties1.put("kafka.data.type", "json");
        properties1.put("kafka.topic", "table_source");
        properties1.put("parallelism", "1");
        final CatalogTable catalogTable1 = new CatalogTableImpl(tableSchema1, properties1, "");
        catalog.createTable(path1, catalogTable1, true);

        final ObjectPath path2 = new ObjectPath(catalogManager.getCurrentDatabase(), "table_side");
        final TableSchema tableSchema2 = TableSchema.builder()
                .field("name", DataTypes.VARCHAR(Integer.MAX_VALUE))
                .field("info", DataTypes.VARCHAR(Integer.MAX_VALUE))
                .build();
        Map<String, String> properties2 = new HashMap<>();
        properties2.put("type", "mysql");
        properties2.put("url", "jdbc:mysql://192.168.1.8:3306/demo?charset=utf8");
        properties2.put("userName", "dipper");
        properties2.put("password", "ide@123");
        properties2.put("tableName", "table_side");
        properties2.put("cache", "NONE");
        properties2.put("parallelism", "1");
        final CatalogTable catalogTable2 = new CatalogTableImpl(tableSchema2, properties2, "");
        catalog.createTable(path2, catalogTable2, true);

        final ObjectPath path3 = new ObjectPath(catalogManager.getCurrentDatabase(), "table_sink");
        final TableSchema tableSchema3 = TableSchema.builder()
                .field("name", DataTypes.VARCHAR(Integer.MAX_VALUE))
                .field("channel", DataTypes.VARCHAR(Integer.MAX_VALUE))
                .field("pv", DataTypes.INT())
                .field("info", DataTypes.VARCHAR(Integer.MAX_VALUE))
                .build();
        Map<String, String> properties3 = new HashMap<>();
        properties3.put("type", "console");
        properties3.put("parallelism", "1");
        final CatalogTable catalogTable3 = new CatalogTableImpl(tableSchema3, properties3, "");
        catalog.createTable(path3, catalogTable3, true);
    }

    @After
    public void after() throws TableNotExistException {
        final ObjectPath path1 = new ObjectPath(catalogManager.getCurrentDatabase(), "table_source");
        final ObjectPath path2 = new ObjectPath(catalogManager.getCurrentDatabase(), "table_side");
        final ObjectPath path3 = new ObjectPath(catalogManager.getCurrentDatabase(), "table_sink");
        catalog.dropTable(path1, true);
        catalog.dropTable(path2, true);
        catalog.dropTable(path3, true);
    }


    @Test
    public void testSqlInsert() {
        final String sql = "INSERT INTO table_sink\n" +
                "    SELECT\n" +
                "        a.name,\n" +
                "        a.channel,\n" +
                "        a.pv,\n" +
                "        b.info\n" +
                "    FROM table_source a\n" +
                "    JOIN table_side b ON a.name = b.name\n" +
                "    WHERE a.channel='channel1' AND a.pv>0";
        FlinkPlannerImpl planner = getPlannerBySqlDialect(SqlDialect.HIVE);
        Operation operation = parse(sql, planner);
        assert operation instanceof CatalogSinkModifyOperation;
        CatalogSinkModifyOperation sinkModifyOperation = (CatalogSinkModifyOperation) operation;
        final Map<String, String> expectedStaticPartitions = new HashMap<>();
        expectedStaticPartitions.put("a", "1");
        assertEquals(expectedStaticPartitions, sinkModifyOperation.getStaticPartitions());
    }

    private static SqlNodeList getSqlNodeList(String sql) throws Exception {

        try (StringReader sqlIn = new StringReader(sql)) {
            SqlParserImpl impl = new SqlParserImpl(sqlIn);

            // back tick as the quote
            impl.switchTo("BTID");
            impl.setTabSize(1);
            impl.setQuotedCasing(Lex.JAVA.quotedCasing);
            impl.setUnquotedCasing(Lex.JAVA.unquotedCasing);
            impl.setIdentifierMaxLength(DEFAULT_IDENTIFIER_MAX_LENGTH);
            return impl.parseSqlStmtList();
        }
    }

    private Operation parse(String sql, FlinkPlannerImpl planner) {
        SqlNode node = planner.parse(sql);
        return SqlToOperationConverter.convert(planner, node);
    }

    private FlinkPlannerImpl getPlannerBySqlDialect(SqlDialect sqlDialect) {
        tableConfig.setSqlDialect(sqlDialect);
        return plannerContext.createFlinkPlanner(catalogManager.getCurrentCatalog(),
                catalogManager.getCurrentDatabase());
    }
}
