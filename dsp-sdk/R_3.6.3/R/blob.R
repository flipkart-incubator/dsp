getDSPClient = function() {
  return (dsp$Client$new())
}

getValidationClient = function() {
  return (validation$Client$new())
}

getHadoopPort = function() {
  if(Sys.getenv(c("DSP_SDK_HADOOP_PORT")) == "")
    return ("50070")
  return (Sys.getenv("DSP_SDK_HADOOP_PORT"))
}

getHDFSClient = function() {
  active_name_node = getDSPClient()$getActiveNameNode("hadoopcluster2")$active_nn
  web_hdfs_uri = paste0("http://", active_name_node, ":", getHadoopPort(), "/webhdfs/v1")
  return (hdfs$Client$new(web_hdfs_uri))
}

#'
#' Get or create a Blob with request_id
#' @param request_id request_id or name of blob
#' @return Blob Object
#' \itemize{
#'   \item request_id = "#\{request_id\}"
#'   \item location = "hdfs://hadoopcluster2/projects/planning/dsp_stage/blob/R/#\{request_id\}/"
#'   \item type = "R"
#'   \item status = "COMPLETED"
#' }
#' @examples
#' blob_1 <- blob(11);
#' blob_1 <- blob("ab");
#' blob_1 <- blob("Blob_1");
#' @export
blob = function(request_id) {
  getValidationClient()$verifyNotNull("request_id",request_id)
  return (getDSPClient()$getOrCreateBlob(request_id, "R"))
}

#'
#' Dump a blob for a variable_name
#' @param blob blob object
#' @param variable_name variable_name
#' @param data data for dump
#' @return None
#' @examples
#' blob1 <- blob("R_blob_1")
#' data <- "12"
#' blobDump(blob1, "v_1", data)
#' @export
blobDump = function(blob, variable_name, data) {
  getValidationClient()$validateDumpRequest(blob, variable_name, data)
  blob_path = blob$location
  hdfs_path = paste(stringr::str_remove(blob_path, "hdfs://hadoopcluster2"), variable_name, sep ="")
  retry_count <- 3
  while(retry_count>0){
      retry_count <- retry_count - 1
      tryCatch({
        getHDFSClient()$write(hdfs_path, variable_name, data = data, type = "RDS")
        break
      }, error = function (cond) {
        if (retry_count <= 0) {
          message(cond)
          stop("Error in writing blob")
        }
      })
    }
}

#'
#' load a blob with a variable name
#' @param blob blob object
#' @param variable_name variable_name
#' @return object data stored for variable nam for blob object
#' @examples
#' blob1 <- blob("R_blob_1")
#' data = blobLoad(blob1, "v_1")
#' @export
blobLoad = function(blob, variable_name) {
  getValidationClient()$validateLoadRequest(blob, variable_name)
  blob_path = blob$location
  hdfs_path = paste(stringr::str_remove(blob_path, "hdfs://hadoopcluster2"), variable_name, sep ="")
  retry_count <- 3
  while(retry_count>0){
    retry_count <- retry_count - 1
    tryCatch({
      return(getHDFSClient()$read(hdfs_path, variable_name, type = "RDS"))
    }, error = function (cond) {
      if (retry_count <= 0) {
        message(cond)
        stop("Error in loading blob")
      }
    })
  }
}

#'
#' Dump a blob file for a variable_name
#' @param blob blob object
#' @param variable_name variable_name
#' @param data data for dump
#' @param type file type
#' @return None
#' @examples
#' blob1 <- blob("R_blob_1")
#' data <- "12"
#' blobDumpFile(blob1, "v_1", data, "CSV")
#' @export
blobDumpFile = function(blob, variable_name, data, type) {
  getValidationClient()$validateDumpRequest(blob, variable_name, data)
  blob_path = blob$location
  hdfs_path_temp = paste(stringr::str_remove(blob_path, "hdfs://hadoopcluster2"), variable_name, sep ="")
  hdfs_path = stringr::str_replace(hdfs_path_temp, "R", "ALL")
  retry_count <- 3
  while(retry_count>0){
      retry_count <- retry_count - 1
      tryCatch({
        if (type == "CSV") {
          getHDFSClient()$write_chunks(hdfs_path, variable_name, data = data, type = type)
        } else {
          getHDFSClient()$write(hdfs_path, variable_name, data = data, type = type)
        }
        break
      }, error = function (cond) {
        if (retry_count <= 0) {
          message(cond)
          stop("Error in loading blob")
        }
      })
    }
}

#'
#' load a blob file with a variable name
#' @param blob blob object
#' @param variable_name variable_name
#' @param type file type
#' @return object data stored for variable nam for blob object
#' @examples
#' blob1 <- blob("R_blob_1")
#' data = blobLoad(blob1, "v_1", "CSV")
#' @export
blobLoadFile = function(blob, variable_name, type) {
  getValidationClient()$validateLoadFileRequest(blob, variable_name)
  blob_path = blob$location
  hdfs_path_temp = paste(stringr::str_remove(blob_path, "hdfs://hadoopcluster2"), variable_name, sep ="")
  hdfs_path = stringr::str_replace(hdfs_path_temp, "R", "ALL")
  retry_count <- 3
  while(retry_count>0){
    retry_count <- retry_count - 1
    tryCatch({
      if (type == "CSV") {
        return(getHDFSClient()$read_chunks(hdfs_path, variable_name, type = type))
      } else {
        return(getHDFSClient()$read(hdfs_path, variable_name, type = type))
      }
    }, error = function (cond) {
      if (retry_count <= 0) {
        message(cond)
        stop("Error in loading blob")
      }
    })
  }
}

#'
#' list of all the variables for a blob
#' @param blob blob object
#' @return list of all the variables for a blob
#' \itemize{
#'   \item blobs = "["v_1","v_2","v_3"]"
#' }
#' @examples
#' blob1 <- blob("R_blob_1")
#' var_list <- blobListAll(blob1);
#' @export
blobListAll = function(blob) {
  getValidationClient()$verifyInputBlobObject(blob)
  getDSPClient()$getAllBlobs(blob$request_id, blob$type)
}
