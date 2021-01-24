package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.RequestStepAuditActor;
import com.flipkart.dsp.entities.request.RequestStepAudit;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

/**
 */
@Api("request-step-audit")
@Path("/v1/request_step_audit")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class RequestStepAuditResource {
    private final RequestStepAuditActor requestStepAuditActor;

    @GET
    @Timed
    @Path("/{id}")
    @UnitOfWork(readOnly = true)
    public RequestStepAudit getRequestStepAudit(@PathParam("id") Long requestStepIdAuditId) {
        return requestStepAuditActor.getRequestStepAuditById(requestStepIdAuditId);
    }

    @POST
    @Timed
    @UnitOfWork
    public RequestStepAudit createRequestStepAudit(RequestStepAudit requestStepAudit) {
        return requestStepAuditActor.createRequestStepAudit(requestStepAudit);
    }
}
