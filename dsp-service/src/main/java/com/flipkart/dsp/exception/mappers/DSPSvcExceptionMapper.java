package com.flipkart.dsp.exception.mappers;


import com.flipkart.dsp.exception.DSPSvcException;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;

@Slf4j
public class DSPSvcExceptionMapper extends com.flipkart.dsp.exception.mappers.ExceptionMapper implements ExceptionMapper<DSPSvcException> {

    @Override
    public Response toResponse(DSPSvcException e) {
        return super.toResponse(e.getCause(), e.getMessage());
    }
}