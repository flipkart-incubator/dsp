package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.PipelineStepActor;
import com.flipkart.dsp.entities.pipelinestep.PipelineStep;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

/**
 */


@Slf4j
@Api("pipeline_steps")
@Path("/v1/pipeline_step")
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class PipelineStepResource {

    private final PipelineStepActor pipelineStepActor;

    @GET
    @Timed
    @Path("/{pipeline_step_id}")
    @UnitOfWork(readOnly = true)
    public PipelineStep getPipelineStepStatus(@PathParam("pipeline_step_id") Long pipelineStepId) {
        return pipelineStepActor.getPipelineStepById(pipelineStepId);
    }

}
