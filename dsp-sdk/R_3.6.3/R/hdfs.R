hdfs <- rlang::new_environment(
  data = list(
    Client = R6::R6Class(
      "Client",
      lock_class = FALSE,
      lock_objects = FALSE,
      public = list(
        user = NULL,
        user_param = NULL,
        web_hdfs_uri = NULL,
        initialize = function (uri) {
          self$web_hdfs_uri = uri
          self$user = Sys.getenv(c("DSP_SDK_HADOOP_USER"))
          if (self$user == "") {
            self$user = "fk-ip-data-service"
          }
          self$user_param <- paste0('user.name=', self$user)
        },

        getJsonOutput = function(response) {
          result <- httr::content(response, "text", encoding = "UTF-8")
          jsonlite::fromJSON(result)
        },

        list_files = function (hdfs_path) {
          uri <- paste0(self$web_hdfs_uri, hdfs_path, '?', self$user_param, '&op=LISTSTATUS')
          tryCatch({
            response <- httr::GET(uri)
            response_code <- httr::status_code(response)
            if (response_code != 200)
              stop(paste0("response_code: ", response_code))
            else {
              json_data <- self$getJsonOutput(response)
              fileStatus <- json_data$FileStatuses$FileStatus
              if (class(fileStatus)=='list') return(fileStatus)
              files <- fileStatus[fileStatus['type']=='FILE','pathSuffix']
              return(files)
            }
           }, error = function (cond) {
            message(cond)
            error_message <- paste0("Not able to get file list from hdfs")
            stop(error_message)
          })
        },

        delete_dir = function (hdfs_path) {
          uri <- paste0(self$web_hdfs_uri, hdfs_path, '?', self$user_param, '&op=DELETE&recursive=true')
          tryCatch({
            response <- httr::DELETE(uri)
            response_code <- httr::status_code(response)
            if (response_code != 200)
              stop(paste0("response_code: ", response_code))
           }, error = function (cond) {
            message(cond)
            error_message <- paste0("Not able to delete dir from hdfs")
            stop(error_message)
          })
        },

        read = function (hdfs_path, variable_name, type) {
          local_file_name = paste(variable_name, sample(1:10000, 1), sep ="_")
          local_path = paste(getwd(), local_file_name, sep ="/")
          uri <- paste0(self$web_hdfs_uri, hdfs_path, '?', self$user_param, '&op=OPEN')

          tryCatch({
            httr::GET(uri, httr::write_disk(local_path, overwrite = TRUE))
            if (type == "RDS") {
              return (readRDS(local_path))
            } else if (type == "CSV") {
              return (data.table::fread(local_path, header=TRUE))
            } else if (type == "YAML") {
              return (yaml::read_yaml(local_path))
            } else {
              stop("Valid type allowed: CSV, YAML")
            }
          }, error = function (cond) {
            error_message <- paste("Error while reading R variable:", variable_name, ". Error Message:", cond )
            stop(error_message)
          }, finally = {
            file.remove(local_path)
          })
        },

        read_chunks = function (hdfs_path, variable_name, type) {
            file_list <- self$list_files(hdfs_path)
            datalist = list()
            idx <- 0
            for(file_name in file_list){
              chunk_file <- paste(hdfs_path, file_name, sep ="/")
              idx <- idx + 1
              retry_count <- 3
              while(retry_count>0){
                retry_count <- retry_count - 1
                tryCatch({
                  datalist[[idx]] <- self$read(chunk_file, variable_name, type)
                  break
                }, error = function (cond) {
                  if (retry_count <= 0) {
                    message(cond)
                    stop("Error in loading blob")
                  }
                })
              }
            }
            dataframe <- dplyr::bind_rows(datalist)
            rm(datalist)
            return(dataframe)
        },

        write = function (hdfs_path, variable_name, data, type) {
          local_file_name = paste(variable_name, sample(1:10000, 1), sep ="_")
          local_path = paste(getwd(), local_file_name, sep ="/")
          uri <- paste0(self$web_hdfs_uri, hdfs_path, '?', self$user_param, '&op=CREATE&overwrite=true')

          tryCatch({
            if (type == "RDS") {
              saveRDS(data, file = local_path)
            } else if (type == "CSV") {
              data.table::fwrite(data, file = local_path, col.names=TRUE)
            } else if (type == "YAML"){
              yaml::write_yaml(data, local_path)
            } else {
              stop("Valid type allowed: CSV, YAML")
            }
            response <- httr::PUT(uri)
            if (httr::status_code(response) == 201) {
              response.redirect <- response$url
              response.upload <- httr::PUT(response.redirect, body = httr::upload_file(local_path))
            } else {
              stop("Error creating  HDFS file")
            }
          }, error = function (cond) {
            error_message <- paste("Error while writing R variable:", variable_name, ". Error Message:", cond )
            stop(error_message)
          }, finally = {
            file.remove(local_path)
          })
        },

        write_chunks = function (hdfs_path, variable_name, data, type) {
            file_list <- self$delete_dir(hdfs_path)
            chunk_size <- nrow(data)
            num_chunks <- ceiling(nrow(data)/chunk_size)
            for(i in 1:num_chunks){
              chunk_file_path <- paste(hdfs_path, "/chunk_", i, sep ="")
              start <- ((i-1)*chunk_size)+1
              end <- min(nrow(data) , i * chunk_size)
              chunk <-  data[start:end,]
              retry_count <- 3
              while(retry_count>0){
                retry_count <- retry_count - 1
                tryCatch({
                  self$write(chunk_file_path, variable_name, chunk, type)
                  break
                }, error = function (cond) {
                  if (retry_count <= 0) {
                    message(cond)
                    stop("Error in dump file chunk")
                  }
                })
              }
            }
        }
      )
    )
  )
)
