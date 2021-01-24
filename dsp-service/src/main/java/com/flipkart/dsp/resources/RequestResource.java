package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.RequestActor;
import com.flipkart.dsp.actors.ScriptActor;
import com.flipkart.dsp.api.AzkabanExecutionAPI;
import com.flipkart.dsp.dto.Error;
import com.flipkart.dsp.entities.request.Request;
import com.flipkart.dsp.exception.DSPSvcException;
import com.flipkart.dsp.exceptions.EntityNotFoundException;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.ExecutionOutput;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.service.AzkabanProjectHelper;
import com.flipkart.dsp.validation.Validator;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static com.flipkart.dsp.utils.Constants.APPLICATION_JSON;

/**
 */
@Slf4j
@Api("requests")
@Path("/v2/requests")
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class RequestResource {

    private final Validator validator;
    private final ScriptActor scriptActor;
    private final RequestActor requestActor;
    private final AzkabanExecutionAPI azkabanExecutionAPI;
    private final AzkabanProjectHelper azkabanProjectHelper;

    @GET
    @Path("/{requestId}")
    @Timed
    @UnitOfWork(readOnly = true)
    @Consumes(MediaType.APPLICATION_JSON)
    public Request getRequest(@PathParam("requestId") long requestId) {
        return requestActor.getRequest(requestId);
    }

    @GET
    @Path("/status/{request_id}")
    @Timed
    @UnitOfWork(readOnly = true)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response getRequestStatus(@PathParam("request_id") Long requestId) throws EntityNotFoundException {
        try {
            Request request = validator.verifyRequestId(requestId);
            return Response.ok().type(APPLICATION_JSON).entity(request.getRequestStatus()).build();
        } catch (ValidationException e) {
            Error error = new Error(Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }

    @GET
    @Path("/{request_id}/workflow_id")
    @Timed
    @UnitOfWork
    public Response getWorkflowId(@PathParam("request_id") Long requestId) {
        Long workflowId = requestActor.getRequest(requestId).getWorkflowId();
        return Response.accepted().type(MediaType.APPLICATION_JSON_TYPE).entity(workflowId).build();
    }

    @GET
    @Timed
    @Path("{request_id}/kill")
    public Response killRunningWorkflow(@PathParam("request_id") Long requestId) throws DSPSvcException {
        //todo: what about authorisation??
        try {
            Request request = validator.verifyRequestId(requestId);
            if (request.getRequestStatus().equals(RequestStatus.COMPLETED) || request.getRequestStatus().equals(RequestStatus.FAILED))
                throw new DSPSvcException("Request id: " + requestId + " is already completed");
            Long azkabanExecId = request.getAzkabanExecId();
            validator.verifyAzkabanExecId(azkabanExecId, requestId);
            azkabanExecutionAPI.killJob(azkabanExecId);
            return Response.ok("Job with request id: " + requestId + " azkaban job id: " + azkabanExecId + " Killed successfully!").build();
        } catch (ValidationException e) {
            throw new DSPSvcException("Abort Request failed for requestId: " + requestId + ". Errors: " + e.getMessage());
        }
    }

    @GET
    @Timed
    @Path("{request_id}/retry")
    public Response retryJob(@PathParam("request_id") Long requestId, @DefaultValue("") @QueryParam("commit_id") String commitId) throws DSPSvcException {
        try {
            Request request = validator.verifyRequestId(requestId);
            scriptActor.updateScriptCommitId(request, commitId); //todo: add commit Id validation here when you figure out github rate limit.
            Long azkabanExecId = request.getAzkabanExecId();
            validator.verifyAzkabanExecId(azkabanExecId, requestId);
            Long newExecId = azkabanExecutionAPI.retryJob(azkabanExecId, request);
            String azkabanUrl = azkabanProjectHelper.getAzkabanUrl(newExecId);
            ExecutionOutput executionOutput = ExecutionOutput.builder().jobId(requestId).azkabanUrl(azkabanUrl).build();
            return Response.ok().entity(executionOutput).build();
        } catch (ValidationException | DSPSvcException e) {
            throw new DSPSvcException("Retry Request failed for requestId: " + requestId + ". Errors: " + e.getMessage());
        }
    }
}
