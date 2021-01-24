package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.PipelineStepAuditActor;
import com.flipkart.dsp.entities.misc.ExecutionDetails;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepAudit;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

/**
 */


@Slf4j
@Api("pipeline_step_audits")
@Path("/v1/pipeline_step_audit")
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class PipelineStepAuditResource {

    private final PipelineStepAuditActor pipelineStepAuditActor;

    @POST
    @Timed
    @UnitOfWork
    @Consumes(MediaType.APPLICATION_JSON)
    public PipelineStepAudit createPipelineStepAudits(PipelineStepAudit pipelineStepAudit) {
        return pipelineStepAuditActor.saveAuditEntry(pipelineStepAudit);
    }

    @GET
    @Path("/executionTime/pipelineStepId")
    @Timed
    @UnitOfWork(readOnly = true)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public ExecutionDetails getPipelineStepStatus(@QueryParam("pipelineId") long pipelineStep, @QueryParam("pipelineExecutionId") String pipelineExecutionId){
        return pipelineStepAuditActor.getExecutionDetailsForPipelineStep(pipelineStep, pipelineExecutionId);
    }

    @GET
    @Path("/pipelineStepDetails")
    @Timed
    @UnitOfWork(readOnly = true)
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public List<PipelineStepAudit> getPipelineStepAuditsByPipelineExecutionId(@QueryParam("attempt") Integer attempt,
                                                                              @QueryParam("refreshId") Long refreshId,
                                                                              @QueryParam("pipelineStepId") Long pipelineStepId,
                                                                              @QueryParam("pipelineExecutionId") String pipelineExecutionId) {
        return pipelineStepAuditActor.getPipelineStepAudits(attempt, refreshId, pipelineStepId, pipelineExecutionId, null);
    }
}
