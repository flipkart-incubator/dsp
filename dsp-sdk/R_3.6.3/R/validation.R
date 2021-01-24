validation <- rlang::new_environment(
  data = list(
    Client = R6::R6Class(
      "Client",
      lock_class = FALSE,
      lock_objects = FALSE,
      public = list(
        initialize = function () {
        },

        verifyInputBlobObject = function(blob) {
          self$verifyNotNull("blob", blob)
          self$verifyNotNull("type of blob ", blob$type)
          self$verifyNotNull("location of blob", blob$location)
          self$verifyNotNull("request_id of blob", blob$request_id)
          self$verifyExistanceOfInputBlob(blob)

        },

        validateDumpRequest = function(blob, variable_name, data) {
          self$verifyInputBlobObject(blob)
          self$verifyNotNull("variable_name",variable_name)
          self$verifyNotEmpty("variable_name",variable_name)
          self$verifyNotNull("data", data)
        },

        validateLoadRequest = function(blob, variable_name) {
          self$verifyInputBlobObject(blob)
          self$verifyNotNull("variable_name", variable_name)
          self$verifyNotEmpty("variable_name",variable_name)
          self$verifyDumpExistnaceForVariable(blob, variable_name)
        },

        validateLoadFileRequest = function(blob, variable_name) {
          self$verifyInputBlobObject(blob)
          self$verifyNotNull("variable_name", variable_name)
          self$verifyNotEmpty("variable_name",variable_name)
        },

        verifyDumpExistnaceForVariable = function(blob, variable_name) {
          all_variables = dsp$Client$new()$getAllBlobs(blob$request_id, blob$type)
          variable_exist <- grepl(variable_name, all_variables, fixed=TRUE)
          if ("TRUE" %in% variable_exist) {
            return(TRUE);
          }
          stop(paste0("No HDFS dump found for variable ", variable_name, " of blob with request_id: ", blob$request_id, " and type: ", blob$type, "."))
        },

        verifyExistanceOfInputBlob = function(blob) {
          existing_blob = dsp$Client$new()$getBlob(blob$request_id, blob$type)
          if (is.null(existing_blob)) {
            stop(paste0("No blob found with request_id: ", blob$request_id, " and type: ", blob$type, "."))
          }
        },

        verifyNotNull = function(variable_name, value) {
          if (is.null(value)) {
            stop(paste0("value of variable ", variable_name, " can't be null."))
          }
        },

        verifyNotEmpty = function(variable_name, value) {
          if (value == "") {
            stop(paste("value of variable", variable_name, "can't be empty"))
          }
        }
      )
    )
  )
)
