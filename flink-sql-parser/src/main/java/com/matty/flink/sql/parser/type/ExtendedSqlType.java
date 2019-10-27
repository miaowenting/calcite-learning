package com.matty.flink.sql.parser.type;

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlWriter;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-08
 */
public interface ExtendedSqlType {

    static void unparseType(SqlDataTypeSpec type,
                            SqlWriter writer,
                            int leftPrec,
                            int rightPrec) {
        if (type.getTypeName() instanceof ExtendedSqlType) {
            type.getTypeName().unparse(writer, leftPrec, rightPrec);
        } else {
            type.unparse(writer, leftPrec, rightPrec);
        }
    }
}
