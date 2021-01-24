package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.DataFrameOverrideAuditActor;
import com.flipkart.dsp.dto.Error;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideAudit;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideType;
import com.flipkart.dsp.exception.DSPSvcException;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Objects;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 * +
 */
@Slf4j
@Api("dataframe_override_audits")
@Path("/v1/dataframe_override_audits")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DataFrameOverrideAuditResource {
    private final DataFrameOverrideAuditActor dataFrameOverrideAuditActor;

    @POST
    @Timed
    @UnitOfWork
    public Response createDataFrameOverrideAudit(DataFrameOverrideAudit dataframeOverrideAudit) {
        try {
            return Response.ok().type(APPLICATION_JSON).entity(dataFrameOverrideAuditActor.save(dataframeOverrideAudit)).build();
        } catch (Exception e) {
            Error error = new Error(400, e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }

    @POST
    @Timed
    @UnitOfWork
    @Path("/update")
    public Response updateDataFrameOverrideAudit(DataFrameOverrideAudit dataFrameOverrideAudit) {
        try {
            return Response.ok().type(APPLICATION_JSON).entity(dataFrameOverrideAuditActor.save(dataFrameOverrideAudit)).build();
        } catch (Exception e) {
            Error error = new Error(400, e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }

    @PATCH
    @Timed
    @UnitOfWork
    @Path("/update_failed")
    public Response updateFailedDataFrameOverrideAudit(@QueryParam("request_id") Long requestId) {
        try {
            if (requestId != null) dataFrameOverrideAuditActor.updateStartedAudits(requestId);
            return Response.ok().build();
        } catch (Exception e) {
            Error error = new Error(400, e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }

    @GET
    @Path("dataframe_id/{dataframe_id}")
    @Timed
    @UnitOfWork
    public Response getDataFrameOverrideAudit(@PathParam("dataframe_id") Long dataFrameId,
                                              @QueryParam("request_id") Long requestId,
                                              @QueryParam("input_data_id") String inputDataId,
                                              @QueryParam("override_type") DataFrameOverrideType dataFrameOverrideType) {
        try {
            DataFrameOverrideAudit dataFrameOverrideAudit = dataFrameOverrideAuditActor.getDataFrameOverrideAudit(requestId, dataFrameId, inputDataId, dataFrameOverrideType);
            return Response.ok().type(MediaType.APPLICATION_JSON).entity(dataFrameOverrideAudit).build();
        } catch (Exception e) {
            Error error = new Error(400, e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }

    @GET
    @Path("{id}")
    @Timed
    @UnitOfWork
    public Response getDataFrameOverrideAuditById(@PathParam("id") Long dataFrameOverrideAuditId) {
        try {
            DataFrameOverrideAudit dataFrameOverrideAudit = dataFrameOverrideAuditActor.getDataFrameOverrideAudit(dataFrameOverrideAuditId);
            if (Objects.isNull(dataFrameOverrideAudit)) {
                String errorMessage = "Dataframe Override Audit for runId: " + dataFrameOverrideAuditId + " not found!";
                throw new DSPSvcException(errorMessage);
            }
            return Response.ok().type(MediaType.APPLICATION_JSON).entity(dataFrameOverrideAudit).build();

        } catch (Exception e) {
            Error error = new Error(400, e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }
}
