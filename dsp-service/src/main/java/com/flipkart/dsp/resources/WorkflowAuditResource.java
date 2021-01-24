package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.WorkflowAuditActor;
import com.flipkart.dsp.dto.Error;
import com.flipkart.dsp.entities.workflow.WorkflowAudit;
import com.flipkart.dsp.exception.DSPSvcException;
import com.flipkart.dsp.models.WorkflowStatus;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.stream.Collectors;

/**
 */
@Slf4j
@Api("workflow-audits")
@Path("/v1/workflow_audits")
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class WorkflowAuditResource {

    private final WorkflowAuditActor workflowAuditActor;

    @POST
    @Timed
    @UnitOfWork
    @Path("/update")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    public Response updateWorkflowAudits(@FormParam("refreshId") Long refreshId, @FormParam("workflowId") Long workflowId,
                                         @FormParam("workflowExecutionId") String workflowExecutionId, @FormParam("status") String status) {
        try {
            workflowAuditActor.update(refreshId, workflowId, workflowExecutionId, WorkflowStatus.valueOf(status));
            return Response.ok().build();
        } catch (Exception e) {
            Error error = new Error(Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }

    @GET
    @Timed
    @Path("/status/{workflowExecutionId}")
    @UnitOfWork(readOnly = true)
    public Response isWorkflowSuccessful(@PathParam("workflowExecutionId") String workflowExecutionId) {
        boolean status = workflowAuditActor.isWorkflowSuccessful(workflowExecutionId);
        return Response.ok().type(MediaType.APPLICATION_JSON).entity(status).build();
    }


    @GET
    @Timed
    @UnitOfWork(readOnly = true)
    @Path("/executionTime/workflow/{workflowExecutionId}")
    public Response getWorkflowExecutionTime(@PathParam("workflowExecutionId") String workflowExecutionId) throws DSPSvcException {
        return Response.ok().entity(workflowAuditActor.getExecutionTimeForWorkflow(workflowExecutionId)).build();
    }

    @GET
    @Path("/executionTime/refreshId/{refreshId}")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Timed
    @UnitOfWork(readOnly = true)
    public Response getWorkflowExecutionTime(@PathParam("refreshId") long refreshId) throws DSPSvcException {
        return Response.ok().entity(workflowAuditActor.getExecutionTimeForRefreshId(refreshId)).build();
    }

    @GET
    @Timed
    @UnitOfWork(readOnly = true)
    @Path("/workflowAuditStatusMap")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response getWorkflowAuditStatusMap(@QueryParam("workflowExecutionId") String workflowExecId) throws Exception {
        Map<WorkflowStatus, Long> statusToCount = workflowAuditActor.getWorkflowAudits(workflowExecId).stream().collect(Collectors.groupingBy
                (WorkflowAudit::getWorkflowStatus, Collectors.counting()));
        if (statusToCount == null || statusToCount.size() == 0) {
            Error error = new Error(Response.Status.BAD_REQUEST.getStatusCode(), "Unable to fetch workflow audits for workflow execution id: " + workflowExecId);
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
        return Response.ok().entity(statusToCount).build();
    }

    @GET
    @Timed
    @UnitOfWork(readOnly = true)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response getWorkflowAuditsByWorkflowExecId(@QueryParam("workflowExecutionId") String workflowExecId) {
        return Response.ok().entity(workflowAuditActor.getWorkflowAudits(workflowExecId)).build();
    }

    @PUT
    @Timed
    @UnitOfWork
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/abortWorkflow/{workflowExecutionId}")
    public Response markWorkflowAuditsAborted(@PathParam("workflowExecutionId") String workflowExecutionId) {
        workflowAuditActor.markWorkflowAuditsAborted(workflowExecutionId);
        return Response.ok().build();
    }
}
