package com.flipkart.dsp.exception.mappers;

import com.flipkart.dsp.exception.ExecuteWorkflowException;
import com.flipkart.dsp.models.ExceptionResponse;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

public class IllegalArgumentExceptionMapper extends com.flipkart.dsp.exception.mappers.ExceptionMapper implements ExceptionMapper<IllegalArgumentException> {

    @Override
    public Response toResponse(IllegalArgumentException e) {
        return super.toResponse(e.getCause(), e.getMessage());
    }
}
