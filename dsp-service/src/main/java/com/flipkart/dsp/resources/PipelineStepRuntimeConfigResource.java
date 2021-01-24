package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.PipelineStepRuntimeConfigActor;
import com.flipkart.dsp.dto.Error;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepRuntimeConfig;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 */
@Slf4j
@Produces(MediaType.APPLICATION_JSON)
@Api("pipeline-step-runtime-configs")
@Path("/v1/pipelineSteps/runtimeConfigs")
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class PipelineStepRuntimeConfigResource {
    private final PipelineStepRuntimeConfigActor pipelineStepRuntimeConfigActor;

    @POST
    @Timed
    @UnitOfWork
    @Consumes(MediaType.APPLICATION_JSON)
    public Response createPipelineStepRuntimeConfig(PipelineStepRuntimeConfig pipelineStepRuntimeConfig) {
        try {
            return Response.ok().type(APPLICATION_JSON).entity(pipelineStepRuntimeConfigActor.save(pipelineStepRuntimeConfig)).build();
        } catch (Exception e) {
            Error error = new Error(Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }

    @GET
    @Timed
    @UnitOfWork(readOnly = true)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response getPipelineStepRuntimeConfig(@QueryParam("pipelineExecutionId") String pipelineExecutionId,
                                                 @QueryParam("pipelineStepId") Long pipelineStepId) {
        try {
            return Response.ok().type(APPLICATION_JSON).entity(pipelineStepRuntimeConfigActor.getPipelineStepRuntimeConfig(pipelineExecutionId, pipelineStepId)).build();
        } catch (DSPCoreException e) {
            Error error = new Error(Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }

    @GET
    @Path("/scope")
    @Timed
    @UnitOfWork(readOnly = true)
    @Consumes(MediaType.APPLICATION_JSON)
    public List<PipelineStepRuntimeConfig> getPipelineStepRuntimeConfigsByScope(@QueryParam("workflowExecutionId") String workflowExecutionId,
                                                                                @QueryParam("scope") String scope) {
        return pipelineStepRuntimeConfigActor.getPipelineStepRuntimeConfigsByScope(workflowExecutionId, scope);
    }
}
