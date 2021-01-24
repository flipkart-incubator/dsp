package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.PipelineStepSGAuditActor;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepSGAudit;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Slf4j
@Api("pipeline_step_sg_audits")
@Path("/v1/pipeline_step_sg_audit")
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class PipelineStepSGAuditResource {
    private final PipelineStepSGAuditActor pipelineStepSGAuditActor;

    @POST
    @Timed
    @UnitOfWork
    @Consumes(MediaType.APPLICATION_JSON)
    public Long createPipelineStepSgAudits(PipelineStepSGAudit pipelineStepAudit) throws DSPCoreException {
        return pipelineStepSGAuditActor.saveAuditEntry(pipelineStepAudit);
    }
}
