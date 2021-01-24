package com.flipkart.dsp.exception.mappers;


import com.flipkart.dsp.exception.ExecuteWorkflowException;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

@Slf4j
public class ExecuteWorkflowExceptionMapper extends com.flipkart.dsp.exception.mappers.ExceptionMapper implements ExceptionMapper<ExecuteWorkflowException> {

    @Override
    public Response toResponse(ExecuteWorkflowException e) {
        return super.toResponse(e.getCause(), e.getMessage());
    }
}