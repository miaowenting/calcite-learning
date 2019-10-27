package com.matty.flink.sql.parser.convertor;

import com.matty.flink.sql.parser.ddl.FunctionType;
import com.matty.flink.sql.parser.ddl.SqlCreateFunction;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-15
 */
public class CreateFunctionSqlConvertor extends BaseSqlConvertor implements SqlConvertor<CreateFunctionSqlConvertor.CreateFunctionSqlConvertResult> {

    @Override
    public CreateFunctionSqlConvertResult convert(String sql) {
        final SqlNode sqlNode = parseStmtAndHandleEx(sql);
        assert sqlNode instanceof SqlCreateFunction;
        final SqlCreateFunction sqlCreateFunction = (SqlCreateFunction) sqlNode;

        return new CreateFunctionSqlConvertResult()
                .functionName(sqlCreateFunction.getFunctionName().toString())
                .className(sqlCreateFunction.getClassNameString());
    }

    @Data
    public static class CreateFunctionSqlConvertResult {
        private FunctionType functionType;
        private String functionName;
        private String className;

        private CreateFunctionSqlConvertResult functionName(String functionName) {
            this.functionName = functionName;
            return this;
        }

        private CreateFunctionSqlConvertResult className(String className) {
            this.className = className;
            return this;
        }

    }
}
