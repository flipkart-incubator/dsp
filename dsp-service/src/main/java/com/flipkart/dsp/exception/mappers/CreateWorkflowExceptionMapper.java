package com.flipkart.dsp.exception.mappers;


import com.flipkart.dsp.exception.CreateWorkflowException;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

@Slf4j
public class CreateWorkflowExceptionMapper extends com.flipkart.dsp.exception.mappers.ExceptionMapper implements ExceptionMapper<CreateWorkflowException> {

    @Override
    public Response toResponse(CreateWorkflowException e) {
        return super.toResponse(e.getCause(), e.getMessage());
    }
}