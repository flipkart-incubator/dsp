package com.flipkart.dsp.resources;


import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.DataFrameAuditActor;
import com.flipkart.dsp.dto.Error;
import com.flipkart.dsp.entities.sg.core.DataFrameAudit;
import com.flipkart.dsp.entities.sg.core.DataFrameAuditStatus;
import com.flipkart.dsp.exception.DSPSvcException;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.Valid;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toList;

/**
 * Definitions in the resources are tentative and are expected to change.
 */


@Slf4j
@Api("dataframe_audits")
@Path("/v1/dataframe_audits")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DataframeAuditResource {

    private final DataFrameAuditActor dataFrameAuditActor;

    @GET
    @Timed
    @UnitOfWork(readOnly = true)
    public Response getDataFrameRunIds(@QueryParam("dataframes") String dataFrameIds) throws DSPCoreException {
        try {
            if (StringUtils.isEmpty(dataFrameIds)) throw new IllegalArgumentException("Dataframes is empty");
            List<Long> dataframeIds = Arrays.stream(dataFrameIds.split(","))
                    .map(dataFrameId -> Long.valueOf(dataFrameId.trim())).collect(toList());
            return Response.ok().entity(dataFrameAuditActor.getDataFrameRunIds(dataframeIds)).build();
        } catch (Exception e) {
            Error error = new Error(Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }

    @POST
    @Timed
    @UnitOfWork
    public Response createDataFrameAudits(DataFrameAudit dataFrameAudit) {
        try {
            return Response.ok().type(MediaType.APPLICATION_JSON).entity(dataFrameAuditActor.persist(dataFrameAudit)).build();
        } catch (Exception e) {
            Error error = new Error(Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }

    @GET
    @Path("{id}")
    @Timed
    @UnitOfWork
    public Response getDataFrameAuditById(@PathParam("id") Long dataFrameAuditId) throws DSPSvcException {
        try {
            DataFrameAudit dataFrameAudit = dataFrameAuditActor.getDataFrameAuditById(dataFrameAuditId);
            if (Objects.isNull(dataFrameAudit))
                throw new DSPSvcException("Dataframe audit for runId: " + dataFrameAuditId + " not found!");
            return Response.ok().entity(dataFrameAudit).build();
        } catch (Exception e) {
            Error error = new Error(Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }


    @GET
    @Path("dataframe_id/{dataframe_id}")
    @Timed
    @UnitOfWork
    public Response getDataFrameAudit(@PathParam("dataframe_id") Long dataFrameId,
                                      @QueryParam("partitions") String partitions,
                                      @QueryParam("override_audit_id") Long dataFrameOverrideAuditId) throws DSPSvcException {

        try {
            DataFrameAudit dataFrameAudit = dataFrameAuditActor.getDataFrameAudit(dataFrameId, dataFrameOverrideAuditId, partitions);
            if (Objects.isNull(dataFrameAudit))
                throw new DSPSvcException("Dataframe audit for DataFrame Id: " + dataFrameId + " not found!");
            return Response.ok().type(MediaType.APPLICATION_JSON).entity(dataFrameAudit).build();
        } catch (IllegalArgumentException e) {
            Error error = new Error(400, e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }


    @POST
    @Path("{request_id}/{workflow_id}/{pipeline_step_id}/create")
    @Timed
    @UnitOfWork
    public Response createRequestDataFrameAudits(@PathParam("request_id") Long requestId,
                                                 @PathParam("workflow_id") Long workflowId,
                                                 @PathParam("pipeline_step_id") Long pipelineStepId,
                                                 @Valid Set<DataFrameAudit> dataFrameAudits) {
        try {
            return Response.ok().type(MediaType.APPLICATION_JSON)
                    .entity(dataFrameAuditActor.saveRequestDataFrameAudits(requestId, workflowId, dataFrameAudits, pipelineStepId))
                    .build();
        } catch (Exception e) {
            Error error = new Error(Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }

    @PATCH
    @Path("{request_id}/{workflow_id}/{current_status}/{new_status}/update")
    @Timed
    @UnitOfWork
    public Response updateDataFrameAuditStatus(@PathParam("request_id") Long requestId,
                                               @PathParam("workflow_id") Long workflowId,
                                               @PathParam("current_status") String currentStatus,
                                               @PathParam("new_status") String newStatus) {
        try {
            dataFrameAuditActor.updateDataFrameAuditStatus(requestId, workflowId, DataFrameAuditStatus.valueOf(currentStatus), DataFrameAuditStatus.valueOf(newStatus));
            return Response.ok().build();
        } catch (Exception e) {
            Error error = new Error(Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }

    }


}
