package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.ExternalCredentialsActor;
import com.flipkart.dsp.dto.Error;
import com.flipkart.dsp.exceptions.DSPCoreException;
import com.flipkart.dsp.models.ExecutionOutput;
import com.flipkart.dsp.models.ExternalCredentials;
import com.flipkart.dsp.models.externalentities.ExternalEntity;
import com.flipkart.dsp.utils.Constants;
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
@Api("external_credentials")
@Path("/v2/external_credentials")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ExternalCredentialsResource {

    private final ExternalCredentialsActor externalCredentialsActor;

    @POST
    @Path("/{entity}")
    @Timed
    @UnitOfWork
    public Response createCredentials(@PathParam("entity") String entity,
                                      ExternalEntity externalEntity) {
        ExternalCredentials externalCredentials = externalCredentialsActor.createCredentials(entity, externalEntity);
        return Response.ok().type(APPLICATION_JSON).entity(externalCredentials).build();
    }

    @GET
    @Path("/{client_alias}")
    @Timed
    @UnitOfWork
    public Response getExternalCredentials(@PathParam("client_alias") String clientAlias) throws DSPCoreException {
        try {
            ExternalCredentials externalCredentials = externalCredentialsActor.getCredentials(clientAlias);
            return Response.accepted().type(Constants.APPLICATION_JSON).entity(externalCredentials).build();
        } catch (Exception e) {
            Error error = new Error(Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }

}
