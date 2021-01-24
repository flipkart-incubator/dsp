dsp <- rlang::new_environment(
  data = list(
    Client = R6::R6Class(
      "Client",
      lock_class = FALSE,
      lock_objects = FALSE,
      public = list(
        base_url = NULL,
        initialize = function () {
          self$base_url = Sys.getenv(c("DSP_SDK_BASE_URL"))
          if (self$base_url == "") {
            self$base_url = "http://0.0.0.0:9090"
          }
        },

        getJsonOutput = function(response) {
          result <- httr::content(response, "text", encoding = "UTF-8")
          jsonlite::fromJSON(result)
        },

        getBlob = function(request_id, blob_type) {
          get_blob_url <- paste0(self$base_url, "/v1/blob")
          tryCatch({
            response <- httr::GET(get_blob_url, query = list(request_id = request_id, type = blob_type))
            response_code <- httr::status_code(response)

            if (response_code == 400) {
              message(paste0("No Blob found with request_id: ", request_id, " and type: ", blob_type, "."))
              return(NULL)
            }
            else if (response_code == 200) {
              message(paste0("Fetching existed Blob with request_id: ", request_id, " and type: ", blob_type, "."))
              return (self$getJsonOutput(response))
            }

            else {
              stop(paste0("Blob get request failed with respone_code: ", response_code, ". Please contact dsp-oncall@flipkart.com"))
            }

          }, error = function (cond) {
            error_message <- paste0("Error while getting Blob for request_id: ", request_id, " and type: ", blob_type,". Error Message: ", cond )
            stop(error_message)
          })

        },

        createBlob = function(request_id, blob_type) {
          create_blob_url <- paste0(self$base_url, "/v1/blob")
          body <- list(request_id = request_id, type = blob_type, status = "STARTED")

          tryCatch({
            response <- httr::POST(create_blob_url, body = body, encode = "json")
            response_code <- httr::status_code(response)
            if (response_code != 200)
              stop(paste0("Blob create request failed with respone_code: ", response_code, ". Please contact dsp-oncall@flipkart.com"))
            else
              return (self$getJsonOutput(response))

          }, error = function (cond) {
            error_message <- paste0("Creation of Blob Variable with request_id: ", request_id, " and blob_type: ",
                                    blob_type, " failed. ", "Error Message: ", cond)
            stop(error_message)
          })
        },

        getOrCreateBlob = function(request_id, blob_type) {
          current_blob = self$getBlob(request_id, blob_type)
          if (is.null(current_blob)) {
            message(paste0("Creating New Blob with request_id: ", request_id, " and type: ", blob_type, "."))
            return (self$createBlob(request_id, blob_type))
          }
          return(current_blob)
        },

        getActiveNameNode = function(cluster_name) {
          active_nn_url = paste0(self$base_url, "/v1/active-nn")

          tryCatch({
            response <- httr::GET(active_nn_url, query = list(cluster = cluster_name))
            response_code <- httr::status_code(response)

            if (response_code != 200)
              stop(paste0("response_code: ", response_code))
            else
              return (self$getJsonOutput(response))

          }, error = function (cond) {
            error_message <- paste0("Not able to get active Name Node from DSP Service for cluster: ", cluster_name,
                                    ". Error Message: ", cond)
            stop(error_message)
          })
        },

        getAllBlobs = function(request_id, blob_type) {
          get_all_blob_list_url <- paste0(self$base_url, "/v1/blob/all")

          tryCatch({
            response <- httr::GET(get_all_blob_list_url, query = list(request_id = request_id, type = blob_type))
            response_code <- httr::status_code(response)

            if (response_code != 200)
              stop(paste0("response_code: ", response_code))
            else
              return (self$getJsonOutput(response))

          }, error = function (cond) {
            error_message <- paste0("Error while getting Blob variables list for blob with request_id: ", request_id,
                                    " and type: ", blob_type,". Error Message: ", cond )
            stop(error_message)
          })
        },

        gethadoopclusterDataNodesHostMapping = function() {
          get_data_node_host_mapping_url = paste0(base_url, "/v1/cluster-hosts")
          tryCatch({
            response <- httr::GET(get_data_node_host_mapping_url)
            response_code <- httr::status_code(response)

            if (response_code != 200)
              stop(paste0("response_code: ", response_code))
            else
              return (self$getJsonOutput(response)$node)

          }, error = function (cond) {
            error_message <- paste0("Error while getting Blob variables list for blob with request_id: ", request_id,
                                    " and type: ", blob_type,". Error Message: ", cond )
            stop(error_message)
          })

        }
      )
    )
  )
)
