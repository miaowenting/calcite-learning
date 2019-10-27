package com.matty.flink.sql.parser.type;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-08
 */
public class SqlTimestampType extends SqlIdentifier implements ExtendedSqlType {
    private final int precision;
    private final boolean withLocalTimeZone;

    public SqlTimestampType(SqlParserPos pos, int precision, boolean withLocalTimeZone) {
        super(getTypeName(withLocalTimeZone), pos);
        this.precision = precision;
        this.withLocalTimeZone = withLocalTimeZone;
    }

    private static String getTypeName(boolean withLocalTimeZone) {
        if (withLocalTimeZone) {
            return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE.name();
        } else {
            return SqlTypeName.TIMESTAMP.name();
        }
    }

    public SqlTypeName getSqlTypeName() {
        if (withLocalTimeZone) {
            return SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
        } else {
            return SqlTypeName.TIMESTAMP;
        }
    }

    public int getPrecision() {
        return this.precision;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(SqlTypeName.TIMESTAMP.name());
        if (this.precision >= 0) {
            final SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
            writer.print(precision);
            writer.endList(frame);
        }
        if (this.withLocalTimeZone) {
            writer.keyword("WITH LOCAL TIME ZONE");
        }
    }
}
