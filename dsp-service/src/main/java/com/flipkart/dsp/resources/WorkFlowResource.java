package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.WorkFlowActor;
import com.flipkart.dsp.api.WorkFlowResponseAPI;
import com.flipkart.dsp.api.WorkflowAPI;
import com.flipkart.dsp.api.WorkflowDetailsAPI;
import com.flipkart.dsp.api.WorkflowGroupAPI;
import com.flipkart.dsp.api.dataFrame.DataframeOverrideAPI;
import com.flipkart.dsp.db.entities.DataFrameEntity;
import com.flipkart.dsp.db.entities.WorkflowEntity;
import com.flipkart.dsp.dto.QueueInfoDTO;
import com.flipkart.dsp.dto.WorkflowResponseDTO;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.entities.workflow.WorkflowDetails;
import com.flipkart.dsp.exception.CreateWorkflowException;
import com.flipkart.dsp.exception.DSPSvcException;
import com.flipkart.dsp.exception.DataFrameCreationException;
import com.flipkart.dsp.exception.ExecuteWorkflowException;
import com.flipkart.dsp.exceptions.EntityNotFoundException;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.ExecutionOutput;
import com.flipkart.dsp.models.WorkflowGroupCreateDetails;
import com.flipkart.dsp.models.workflow.CreateWorkflowRequest;
import com.flipkart.dsp.models.workflow.CreateWorkflowResponse;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import com.flipkart.dsp.utils.Constants;
import com.flipkart.dsp.validation.Validator;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.parquet.Strings;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Objects;

import static com.flipkart.dsp.utils.Constants.APPLICATION_JSON;

/**
 */
@Slf4j
@Api("workflow")
@Path("/v1/workflow")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class WorkFlowResource {
    private final Validator validator;
    private final WorkflowAPI workflowAPI;
    private final WorkFlowActor workFlowActor;
    private final WorkflowGroupAPI workflowGroupAPI;
    private final WorkflowDetailsAPI workflowDetailsAPI;
    private final WorkFlowResponseAPI workFlowResponseAPI;
    private final DataframeOverrideAPI dataframeOverrideAPI;

    @POST
    @Timed
    @UnitOfWork
    @Path("/create")
    public Response createWorkflow(@DefaultValue(Constants.PRODUCTION_DEFAULT_USER) @HeaderParam("triggered_by") String triggeredBy,
                                   CreateWorkflowRequest createWorkflowRequest) throws CreateWorkflowException {
        try {
            List<DataFrameEntity> sgDataFrameEntityList = dataframeOverrideAPI.createMissingDataFrames(createWorkflowRequest);
            CreateWorkflowResponse createWorkflowResponse = workflowAPI.createWorkflow(triggeredBy, createWorkflowRequest, sgDataFrameEntityList);
            return Response.accepted().type(APPLICATION_JSON).entity(createWorkflowResponse).build();
        } catch (CreateWorkflowException | ValidationException | DataFrameCreationException e) {
            throw new CreateWorkflowException("Workflow create request failed for workflow_name: " + createWorkflowRequest.getWorkflow().getName()
                    + " because of following reason: " + e.getMessage());
        }
    }

    @POST
    @Timed
    @UnitOfWork
    @Path("/execute")
    public Response executeWorkflow(@DefaultValue(Constants.PRODUCTION_DEFAULT_USER) @HeaderParam("triggered_by") String triggeredBy,
                                    ExecuteWorkflowRequest executeWorkflowRequest) throws ExecuteWorkflowException {
        try {
            ExecutionOutput executionOutput = workflowAPI.executeWorkflow(triggeredBy, null, executeWorkflowRequest);
            return Response.accepted().type(APPLICATION_JSON).entity(executionOutput).build();
        } catch (ValidationException| ExecuteWorkflowException e) {
            throw new ExecuteWorkflowException("Workflow execute request failed for workflow_name: " + executeWorkflowRequest.getWorkflowName()
                    + " because of following reason: " + e.getMessage());
        }
    }

    @GET
    @Path("/{id}/queue")
    @Timed
    @UnitOfWork(readOnly = true)
    public Response getQueueInfo(@PathParam("id") Long workflowId) throws DSPSvcException {
        try {
            WorkflowDetails workflowDetails = validator.verifyWorkflowId(workflowId);
            Workflow workflow = workflowDetails.getWorkflow();
            QueueInfoDTO queueInfoDTO = QueueInfoDTO.builder().hiveQueue(workflow.getWorkflowMeta().getHiveQueue())
                    .mesosQueue(workflow.getWorkflowMeta().getMesosQueue()).build();
            return Response.ok().type(APPLICATION_JSON).entity(queueInfoDTO).build();
        } catch (ValidationException e) {
            throw new DSPSvcException(e.getMessage());
        }
    }


    @GET
    @Path("/details/{workflow_id}")
    @Timed
    @UnitOfWork(readOnly = true)
    public Response getWorkflowDetailsById(@PathParam("workflow_id") Long workflowId) {
        WorkflowDetails workflowDetails = workFlowActor.getWorkflowDetailsById(workflowId);
        return Response.accepted().type(APPLICATION_JSON).entity(workflowDetails).build();
    }


    @GET
    @Path("/{workflow_name}")
    @Timed
    @UnitOfWork(readOnly = true)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response getLatestWorkflowDetails(@PathParam("workflow_name") String workflowName,
                                             @DefaultValue("true") @QueryParam("is_prod") Boolean isProd,
                                             @QueryParam("version") String version) {
        WorkflowDetails workflowDetails = workflowDetailsAPI.getLatestWorkflowDetails(workflowName, null,  version, isProd);
        if (Objects.isNull(workflowDetails)) {
            String errorMessage = String.format("No Workflow found with name: %s, is_prod=%s%s" ,  workflowName, isProd,
                    Strings.isNullOrEmpty(version)? "." : " and version: " + version + ".");
            throw new EntityNotFoundException(WorkflowEntity.class.getSimpleName(), errorMessage);
        }
        WorkflowResponseDTO workflowResponseDTO = workFlowResponseAPI.getWorkflowResponse(workflowDetails);
        return Response.accepted().type(MediaType.APPLICATION_JSON).entity(workflowResponseDTO).build();
    }

    @GET
    @Path("/download/{workflow_name}")
    @Timed
    @UnitOfWork(readOnly = true)
    public Response getWorkflowByName(@PathParam("workflow_name") String workflowName,
                                      @QueryParam("version") String version,
                                      @DefaultValue("true") @QueryParam("is_prod") Boolean isProd) {
        WorkflowDetails workflowDetails = workflowDetailsAPI.getLatestWorkflowDetails(workflowName, null, version, isProd);
        if (Objects.isNull(workflowDetails)) {
            String errorMessage = String.format("No Workflow found with name: %s and is_prod=%s", workflowName, isProd);
            throw new EntityNotFoundException(WorkflowEntity.class.getSimpleName(), errorMessage);
        }
        WorkflowGroupCreateDetails details = workflowGroupAPI.convertToWorkflowGroupCreateDetails(workflowDetails);
        CreateWorkflowRequest createWorkflowRequest = workflowGroupAPI.convertToCreateWorkflowRequest(workflowDetails.getWorkflow().getVersion(), details);
        return Response.ok().entity(createWorkflowRequest).build();
    }

    @GET
    @Timed
    @UnitOfWork(readOnly = true)
    public Response getAllWorkflowNames() {
        return Response.ok().entity(workFlowActor.getAllDistinctWorkFlowNames()).build();
    }
}
