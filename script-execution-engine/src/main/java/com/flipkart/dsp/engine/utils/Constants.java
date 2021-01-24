package com.flipkart.dsp.engine.utils;

/**
 * +
 */
public class Constants {

    public static final String TEMP = "/temp";
    public static final String SLASH = "/";
    public static final String CREATE_INPUT_SCRIPTS_PATH = "/scripts";
    public static final String CREATE_R_DATA_TABLE_FILE_NAME = "read_input_r.r";
    public static final String CREATE_PYTHON_DATA_FRAME_FILE_NAME = "read_input_python.py";


    // Common Read Script Constants
    public static final String PATH = "PATH";
    public static final String HEADERS = "HEADERS";
    public static final String SEPARATOR = "SEPARATOR";
    public static final String DATAFRAME_NAME = "DATAFRAME_NAME";

    // R Specific Read Script Constants
    public static final String FILL = "FILL";
    public static final String READ_TYPE = "READ_TYPE";
    public static final String NA_STRINGS = "NA_STRINGS";
    public static final String COL_CLASSES = "COL_CLASSES";

    // Python Specific Read Script Constants
    public static final String ENCODING = "ENCODING";
    public static final String DATE_TYPE = "DATE_TYPE";
    public static final String NA_VALUES = "NA_VALUES";
    public static final String QUOTE_CHAR = "QUOTE_CHAR";

}
