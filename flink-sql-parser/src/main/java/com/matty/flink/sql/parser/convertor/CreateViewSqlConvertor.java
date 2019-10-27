package com.matty.flink.sql.parser.convertor;

import com.matty.flink.sql.parser.ddl.SqlCreateView;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-14
 */
public class CreateViewSqlConvertor extends BaseSqlConvertor implements SqlConvertor<CreateViewSqlConvertor.CreateViewSqlConvertResult> {

    @Override
    public CreateViewSqlConvertResult convert(String sql) {
        final SqlNode sqlNode = parseStmtAndHandleEx(sql);
        assert sqlNode instanceof SqlCreateView;
        final SqlCreateView sqlCreateView = (SqlCreateView) sqlNode;

        String query = null;
        if (sqlCreateView.getQuery() != null) {
            query = sqlCreateView.getQuery().toString();
        }

        String fieldsInfoStr = null;
        if (sqlCreateView.getFieldList() != null) {
            fieldsInfoStr = sqlCreateView.getFieldList().toString();
        }
        return new CreateViewSqlConvertResult()
                .viewName(sqlCreateView.getViewName().toString())
                .query(query)
                .fieldsInfoStr(fieldsInfoStr);
    }

    @Data
    public static class CreateViewSqlConvertResult {
        private String viewName;
        private String query;
        private String fieldsInfoStr;

        private CreateViewSqlConvertResult viewName(String viewName) {
            this.viewName = viewName;
            return this;
        }

        private CreateViewSqlConvertResult query(String query) {
            this.query = query;
            return this;
        }

        private CreateViewSqlConvertResult fieldsInfoStr(String fieldsInfoStr) {
            this.fieldsInfoStr = fieldsInfoStr;
            return this;
        }

    }
}
