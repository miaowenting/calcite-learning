package com.matty.flink.sql.parser.validate;

import org.apache.calcite.sql.validate.SqlConformance;

/**
 * Description:
 * Flink sql适应类
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-08
 */
public enum FlinkSqlConformance implements SqlConformance {
    /**
     * Calcite's default SQL behavior.
     */
    DEFAULT,

    /**
     * Conformance value that instructs Calcite to use SQL semantics
     * consistent with the Apache HIVE, but ignoring its more
     * inconvenient or controversial dicta.
     */
    HIVE;

    @Override
    public boolean isLiberal() {
        return false;
    }

    @Override
    public boolean isGroupByAlias() {
        return false;
    }

    @Override
    public boolean isGroupByOrdinal() {
        return false;
    }

    @Override
    public boolean isHavingAlias() {
        return false;
    }

    @Override
    public boolean isSortByOrdinal() {
        switch (this) {
            case DEFAULT:
            case HIVE:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean isSortByAlias() {
        switch (this) {
            case DEFAULT:
            case HIVE:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean isSortByAliasObscures() {
        return false;
    }

    @Override
    public boolean isFromRequired() {
        return false;
    }

    @Override
    public boolean isBangEqualAllowed() {
        return false;
    }

    @Override
    public boolean isPercentRemainderAllowed() {
        return false;
    }

    @Override
    public boolean isMinusAllowed() {
        return false;
    }

    @Override
    public boolean isApplyAllowed() {
        return false;
    }

    @Override
    public boolean isInsertSubsetColumnsAllowed() {
        return false;
    }

    @Override
    public boolean allowNiladicParentheses() {
        return false;
    }

    @Override
    public boolean allowExplicitRowValueConstructor() {
        switch (this) {
            case DEFAULT:
                return true;
        }
        return false;
    }

    @Override
    public boolean allowExtend() {
        return false;
    }

    @Override
    public boolean isLimitStartCountAllowed() {
        return false;
    }

    @Override
    public boolean allowGeometry() {
        return false;
    }

    @Override
    public boolean shouldConvertRaggedUnionTypesToVarying() {
        return false;
    }

    @Override
    public boolean allowExtendedTrim() {
        return false;
    }

    /**
     * Whether to allow "insert into tbl1 partition(col1=val1)" grammar.
     */
    public boolean allowInsertIntoPartition() {
        switch (this) {
            case HIVE:
                return true;
        }
        return false;
    }

    /**
     * Whether to allow "insert overwrite tbl1 partition(col1=val1)" grammar.
     */
    public boolean allowInsertOverwrite() {
        switch (this) {
            case HIVE:
                return true;
        }
        return false;
    }

}
