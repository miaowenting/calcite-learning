package com.matty.flink.sql.parser.ddl;

/**
 * Description:
 *
 * @author mwt
 * @version 1.0
 * @date 2019-10-15
 */
public enum FunctionType {

    /**
     * 自定义标量函数
     */
    UDF("ScalarFunction"),
    /**
     * 自定义表值函数
     */
    UDTF("TableFunction"),
    UNSUPPORTED("");

    private String superClassName;

    FunctionType(String superClassName) {
        this.superClassName = superClassName;
    }

    private String getSuperClassName() {
        return superClassName;
    }

    /**
     * Returns the column type with the string representation.
     **/
    public static FunctionType getType(String type) {
        if (type == null) {
            return UNSUPPORTED;
        }
        try {
            return FunctionType.valueOf(type.toUpperCase());
        } catch (IllegalArgumentException var1) {
            return UNSUPPORTED;
        }
    }

    public static FunctionType getTypeBySuperClass(String superClassName) {
        if (superClassName == null) {
            return UNSUPPORTED;
        }
        for (FunctionType functionType : FunctionType.values()) {
            if (superClassName.equalsIgnoreCase(functionType.getSuperClassName())) {
                return functionType;
            }
        }
        return UNSUPPORTED;
    }
}
