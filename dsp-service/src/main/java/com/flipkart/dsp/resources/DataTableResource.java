package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.DataTableActor;
import com.flipkart.dsp.dto.Error;
import com.flipkart.dsp.models.sg.DataTable;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 */
@Slf4j
@Api("data_tables")
@Path("/v1/data_tables")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DataTableResource {

    private final DataTableActor dataTableActor;

    @GET
    @Timed
    @Path("/{table_name}")
    @UnitOfWork(readOnly = true)
    public Response getDataTables(@PathParam("table_name") String tableName) {
        try {
            return Response.ok().type(APPLICATION_JSON).entity(dataTableActor.getTable(tableName)).build();
        } catch (Exception e) {
            Error error = new Error(400, e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }
}
