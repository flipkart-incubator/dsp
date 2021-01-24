package com.flipkart.dsp.engine.commands;

/**
 */
public interface RCommands {
    String IMPORT_FREAD_FWRITE = "require(data.table)";
    String WRITE_TO_LOCAL_FILE = "fwrite(%s,\"%s\",col.names=%s,sep=\",\",quote=FALSE)";
    String SAVE_RDS = "saveRDS(%s, file = '%s')";
    String READ_RDS = "%s <- readRDS('%s')";
    String READ_DATETIME = "%s <- as.POSIXct('%s',format='%%Y-%%m-%%dT%%H:%%M:%%S')";
    String READ_NUMERIC = "%s <- as.numeric('%s')";
    String READ_DATE = "%s <- as.Date('%s','%%Y-%%m-%%d')";
    String ASSIGN_DATATYPES = "c(%s)";
    String ASSIGN_HEADERS = "names(%s) <- c(%s)";
    String DELETE_FOLDER = "unlink('%s', recursive=TRUE)";
}
