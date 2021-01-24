package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.PipelineStepAuditActor;
import com.flipkart.dsp.actors.PipelineStepSGAuditActor;
import com.flipkart.dsp.api.JobDetailsAPI;
import com.flipkart.dsp.api.RunDetailsAPI;
import com.flipkart.dsp.api.WorkflowAPI;
import com.flipkart.dsp.client.MesosLogsClient;
import com.flipkart.dsp.db.entities.PipelineStepSGAuditEntity;
import com.flipkart.dsp.dto.Error;
import com.flipkart.dsp.entities.pipelinestep.PipelineStepAudit;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.entities.script.Script;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exception.DSPSvcException;
import com.flipkart.dsp.exceptions.MesosLogsClientException;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.ScriptRepoMeta;
import com.flipkart.dsp.models.workflow.WorkflowPromoteRequest;
import com.flipkart.dsp.models.workflow.WorkflowPromoteResponse;
import com.flipkart.dsp.validation.Validator;
import com.google.common.base.Preconditions;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Objects;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Slf4j
@Api("executions")
@Path("/v1/executions")
@Produces(APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ExecutionResource {
    private final Validator validator;
    private final WorkflowAPI workflowAPI;
    private final RunDetailsAPI runDetailsAPI;
    private final JobDetailsAPI jobDetailsAPI;
    private final MesosLogsClient mesosLogsClient;
    private final PipelineStepAuditActor pipelineStepAuditActor;
    private final PipelineStepSGAuditActor pipelineStepSGAuditActor;

    @GET
    @Timed
    @UnitOfWork
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/{pipeline-step-audit-id}/log")
    public String getLogsForPipelineStepAuditId(@PathParam("pipeline-step-audit-id") Long pipelineStepAuditId,
                                                @QueryParam("offset")  @DefaultValue("0") Integer offset,
                                                @QueryParam("log-type") @DefaultValue("stderr") String logType) throws MesosLogsClientException {
        PipelineStepAudit pipelineStepAudit = pipelineStepAuditActor.getPipelineStepAuditsById(pipelineStepAuditId);
        return mesosLogsClient.getLogs(pipelineStepAudit.getLogs(), offset, logType);
    }

    @GET
    @Timed
    @UnitOfWork
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/sg/{audit-id}/log")
    public String getLogsForPipelineStepSgAuditId(@PathParam("audit-id") Long pipelineStepSgAuditId,
                                                  @QueryParam("offset")  @DefaultValue("0") Integer offset,
                                                  @QueryParam("log-type") @DefaultValue("stderr") String logType) throws MesosLogsClientException {
        PipelineStepSGAuditEntity pipelineStepSGAuditEntity = pipelineStepSGAuditActor.getById(pipelineStepSgAuditId);
        return mesosLogsClient.getLogs(pipelineStepSGAuditEntity.getLogs(), offset, logType);
    }

    @POST
    @Path("/{request-id}/promote")
    @Timed
    @UnitOfWork
    public Response promoteWorkflow(@PathParam("request-id") Long requestId, WorkflowPromoteRequest workflowPromoteRequest) throws DSPSvcException {
        try {
            WorkflowDetails workflowDetails = workflowAPI.promoteWorkflow(requestId, workflowPromoteRequest);
            Workflow workflow = workflowDetails.getWorkflow();
            WorkflowPromoteResponse workflowPromoteResponse = WorkflowPromoteResponse.builder().workflowName(workflow.getName())
                    .workflowGroupName(workflow.getWorkflowGroupName()).version(workflow.getVersion()).description(workflow.getDescription()).build();
            return Response.accepted().type(APPLICATION_JSON).entity(workflowPromoteResponse).build();
        } catch (DSPSvcException | ValidationException e) {
            e.printStackTrace();
            throw new DSPSvcException("Promote Workflow request failed for request id: " + requestId + " with following reason: " + e.getMessage());
        }
    }

    @GET
    @Path("/{request-id}")
    @UnitOfWork
    @Timed
    public Response getRunStatus(@PathParam("request-id") Long requestId) {
        try {
            return Response.ok().entity(runDetailsAPI.getRunStatus(requestId)).build();
        } catch (ValidationException e) {
            Error error = new Error(Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }

    @GET
    @Path("/{request-id}/script-meta")
    @Timed
    @UnitOfWork(readOnly = true)
    public Response getScriptByRequestId(@PathParam("request-id") Long requestId) throws DSPSvcException {
        try {
            Preconditions.checkNotNull(requestId, "RequestId can't be null");
            Request request = validator.verifyRequestId(requestId);
            Script script = request.getWorkflowDetails().getPipelineSteps().get(0).getScript();
            if (Objects.isNull(script))
                throw new DSPSvcException(String.format("No Script for given requestId %s found ", requestId));
            return Response.ok().entity(ScriptRepoMeta.builder().gitRepo(script.getGitRepo()).gitCommitId(script.getGitCommitId()).build()).build();
        } catch (ValidationException e) {
            throw new DSPSvcException(e.getMessage());
        }
    }

    @GET
    @Timed
    @Path("/{request-id}/details")
    public Response getRequestDetails(@PathParam("request-id") Long requestId) throws Exception {
        try {
            Request request = validator.verifyRequestId(requestId);
            return Response.ok().entity(jobDetailsAPI.getJobDetails(request)).build();
        } catch (Exception e) {
            throw new DSPSvcException("Error while getting Logs for request-id: " + requestId + " ,Error: " + e.getMessage());
        }
    }
}
