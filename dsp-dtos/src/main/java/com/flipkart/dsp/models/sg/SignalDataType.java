package com.flipkart.dsp.models.sg;

/**
 */
public enum SignalDataType {
    TEXT,
    DOUBLE,
    FLOAT,
    INTEGER,
    BIG_INTEGER,
    TIME_DAY,
    TIME_WEEK,
    TIME_MONTH,
    TIME_YEAR,
    BOOLEAN,
    DATETIME,
    DATE;

    public static String fromType(String type) {
        type = type.startsWith("varchar") ? "varchar" : type;
        type = type.startsWith("decimal") ? "decimal" : type;
        switch (type) {
            case "tinyint":
            case "smallint":
            case "integer":
            case "int":
                return INTEGER.name();
            case "bigint":
                return BIG_INTEGER.name();
            case "float":
                return FLOAT.name();
            case "double":
            case "double_precision":
            case "decimal":
            case "numeric":
                return DOUBLE.name();
            case "string":
            case "varchar":
            case "char":
                return TEXT.name();
            case "boolean":
            case "binary":
                return BOOLEAN.name();
            case "timestamp":
            case "datetime":
                return DATETIME.name();
            case "date":
                return DATE.name();
            default:
                return type;
        }
    }
}
