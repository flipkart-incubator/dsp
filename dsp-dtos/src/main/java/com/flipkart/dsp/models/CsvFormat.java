package com.flipkart.dsp.models;

public enum CsvFormat {
    // more info https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/CSVFormat.Predefined.html
    Excel,
    Oracle,
    MySQL,
    PostgreSQLCsv,
    PostgreSQLText,
    Default,
    InformixUnload,
    InformixUnloadCsv,
    MongoDBTsv,
    TDF
}
