package com.flipkart.dsp.exceptions.mapper;

import com.flipkart.dsp.exceptions.EntityNotFoundException;
import com.flipkart.dsp.models.ExceptionResponse;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import java.util.HashMap;
import java.util.Map;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

public class EntityNotFoundExceptionMapper implements ExceptionMapper<EntityNotFoundException> {

  private final static String ENTITY_NAME = "entity_name";

  @Override
  public Response toResponse(EntityNotFoundException e) {
    ExceptionResponse errorResponse = new ExceptionResponse();
    ExceptionResponse.Error error = new ExceptionResponse.Error(NOT_FOUND.getReasonPhrase(), e.getMessage());

    Map<String, String> params = new HashMap<>();
    params.put(ENTITY_NAME, e.getEntityName());
    error.setParams(params);
    errorResponse.add(error);

    return Response.status(NOT_FOUND).entity(errorResponse).type(APPLICATION_JSON_TYPE).build();
  }
}
