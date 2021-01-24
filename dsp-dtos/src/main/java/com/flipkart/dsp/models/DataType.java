package com.flipkart.dsp.models;

/**
 */
public enum DataType {

        STRING("character"),
        INT("integer"),
        LONG("long"),
        DOUBLE("double"),
        FACTOR("factor"),
        ARRAY(""),  // List of Objects (Integer/String etc. )
        BYTEARRAY("bytearray"),
        DATAFRAME(""),
        DATAFRAME_PATH(""),
        BOOLEAN("logical"),
        VAR_NAME(""), // for assigning one variable to another. e.g fulldata <- train_df
        DATE_TIME("datetime"),
        DATE("date"),
        MODEL("");

    private final String rType;

    DataType(String rType) {
        this.rType = rType;
    }

    public String getRType() {
        return this.rType;
    }

    public static DataType getDataTypeFromRType(String rType) {
        if (rType.equalsIgnoreCase(STRING.getRType())) {
            return DataType.STRING;
        } else if (rType.equalsIgnoreCase(INT.getRType())) {
            return DataType.INT;
        } else if (rType.equalsIgnoreCase(DOUBLE.getRType())) {
            return DataType.DOUBLE;
        } else if (rType.equalsIgnoreCase(FACTOR.getRType())) {
            return DataType.FACTOR;
        } else if (rType.equalsIgnoreCase(BOOLEAN.getRType())) {
            return DataType.BOOLEAN;
        } else {
            throw new RuntimeException("Invalid Rtype: " + rType);
        }
    }
}
