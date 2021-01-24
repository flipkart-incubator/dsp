package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.api.*;
import com.flipkart.dsp.entities.subscription.SubscriptionCallback;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exception.DSPSvcException;
import com.flipkart.dsp.exception.ExecuteWorkflowException;
import com.flipkart.dsp.exceptions.EntityNotFoundException;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.ExecutionOutput;
import com.flipkart.dsp.models.WorkflowGroupExecuteRequest;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.validation.Validator;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.Objects;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;


/**
 *
 */

@Slf4j
@Api("workflow-group")
@Produces(APPLICATION_JSON)
@Consumes(APPLICATION_JSON)
@Path("/v2/workflowGroups")
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class WorkflowGroupResource {

    private final Validator validator;
    private final RequestAPI requestAPI;
    private final WorkflowAPI workflowAPI;
    private final SubscriptionAPI subscriptionAPI;
    private final WorkflowGroupAPI workflowGroupAPI;
    private final WorkflowDetailsAPI workflowDetailsAPI;
    private final WorkflowVersionAPI workflowVersionAPI;

    @POST
    @Timed
    @UnitOfWork
    @Path("/execute_workflow/subscription")
    public Response executeWorkflowBySubscription(@DefaultValue(Constants.PRODUCTION_DEFAULT_USER) @HeaderParam("triggered_by") String triggeredBy,
                                                  SubscriptionCallback subscriptionCallback) throws Exception {
        subscriptionAPI.executeWorkflowBySubscription(subscriptionCallback, triggeredBy);
        return Response.accepted().type(APPLICATION_JSON).build();
    }

    @POST
    @Path("/{workflow_group_name}/run")
    @Timed
    @UnitOfWork
    public Response runWorkflowGroup(@DefaultValue(Constants.PRODUCTION_DEFAULT_USER) @HeaderParam("triggered_by") String triggeredBy,
                                     @QueryParam("version") Double version, @QueryParam("draft") @DefaultValue("false") Boolean draft,
                                     @PathParam("workflow_group_name") String workflowGroupName, @Valid WorkflowGroupExecuteRequest workflowGroupExecuteRequest) throws ExecuteWorkflowException {
        try {
            boolean isProd = !draft;  // Changing isDraft to isProd
            ExecuteWorkflowRequest executeWorkflowRequest = workflowGroupAPI.convertToWorkflowExecuteRequest(isProd, version, workflowGroupExecuteRequest);
            validator.validateRefreshIdWorkflowExecute(executeWorkflowRequest);
            ExecutionOutput executionOutput = workflowAPI.executeWorkflow(triggeredBy, workflowGroupName, executeWorkflowRequest);
            return Response.accepted().type(APPLICATION_JSON).entity(executionOutput).build();
        } catch (ValidationException | ExecuteWorkflowException | DSPSvcException e) {
            throw new ExecuteWorkflowException("Execution Request failed for workflow. Errors: " + e.getMessage());
        }
    }

    @GET
    @Path("/{workflow_group_name}")
    @Timed
    @UnitOfWork(readOnly = true)
    public Response getWorkflowGroupByName(@PathParam("workflow_group_name") String name, @DefaultValue("false") @QueryParam("draft") Boolean draft,
                                           @QueryParam("version") Double version) {
        boolean isProd = !draft; // Changing isDraft to isProd
        String versionInString = workflowVersionAPI.parseVersionToString(version);
        WorkflowDetails workflowDetails = workflowDetailsAPI.getLatestWorkflowDetails(null, name, versionInString, isProd);
        if (Objects.isNull(workflowDetails)) {
            String errorMessage = String.format("No WorkflowGroup found with Id: %s and draft=%s", name, draft);
            throw new EntityNotFoundException("WorkflowGroupEntity", errorMessage);
        }
        return Response.ok().entity(workflowGroupAPI.convertToWorkflowGroupCreateDetails(workflowDetails)).build();
    }

}
