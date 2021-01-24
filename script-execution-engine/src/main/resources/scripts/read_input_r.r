require(data.table)

files <- list.files('PATH', full.names = TRUE)

for (file in files){
   if (file.size(file)>0){
     if (!exists('DATAFRAME_NAME')){
        DATAFRAME_NAME <- READ_TYPE(file, stringsAsFactors=FALSE, header=HEADERS, sep='SEPARATOR', fill=FILL, na.strings=c(NA_STRINGS), colClasses=COL_CLASSES)
        unlink('file')
      }

     else {
        temp_dataframe <- READ_TYPE(file, stringsAsFactors=FALSE, header=HEADERS, sep='SEPARATOR', fill=FILL, na.strings=c(NA_STRINGS), colClasses=COL_CLASSES)
        DATAFRAME_NAME<-rbind(DATAFRAME_NAME, temp_dataframe)
        rm(temp_dataframe)
        unlink('file')
     }
   }
}
message(paste("DataFrame Generation completed for dataFrame: ", deparse(substitute(DATAFRAME_NAME))))
