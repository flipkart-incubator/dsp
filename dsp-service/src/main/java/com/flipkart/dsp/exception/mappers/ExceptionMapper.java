package com.flipkart.dsp.exception.mappers;

import com.flipkart.dsp.models.ExceptionResponse;

import javax.ws.rs.core.Response;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;

/**
 * +
 */
public class ExceptionMapper {

    public Response toResponse(Throwable cause, String message) {
        ExceptionResponse errorResponse = new ExceptionResponse();
        String errorMessage = message + (cause != null ? " : " + cause.getMessage() : "");
        ExceptionResponse.Error error = new ExceptionResponse.Error(BAD_REQUEST.getReasonPhrase(), errorMessage);
        errorResponse.add(error);

        return Response.status(BAD_REQUEST).entity(errorResponse).type(APPLICATION_JSON_TYPE).build();
    }
}
