package com.flipkart.dsp.resources.v1;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.BlobMetaActor;
import com.flipkart.dsp.dto.BlobRequest;
import com.flipkart.dsp.dto.BlobResponse;
import com.flipkart.dsp.dto.Error;
import com.flipkart.dsp.entities.enums.BlobType;
import com.flipkart.dsp.exception.DSPSvcException;
import com.flipkart.dsp.exceptions.HDFSUtilsException;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Objects;

@Slf4j
@Api("blob")
@Path("/v1/blob")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class BlobResource {

    private final BlobMetaActor blobMetaActor;

    @GET
    @Path("/all")
    @Timed
    @UnitOfWork(readOnly = true)
    public Response getAllBlobVariables(@QueryParam("request_id") String requestId,
                                        @QueryParam("type") String type) throws DSPSvcException {
        try {
            BlobResponse blobResponse = blobMetaActor.getCompletedBlobsByRequestIdAndType(requestId, BlobType.valueOf(type));
            if(Objects.isNull(blobResponse))
                throw new DSPSvcException("blob meta for request Id: " + requestId + " and type: " + type + " not found");
            return Response.ok().type(MediaType.APPLICATION_JSON).entity(blobMetaActor.getAllBlobVariables(blobResponse.getLocation())).build();
        } catch (IllegalArgumentException | HDFSUtilsException e) {
            Error error = new Error(400, e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }

    }

    @GET
    @Timed
    @UnitOfWork(readOnly = true)
    public Response getAllBlobs(@QueryParam("request_id") String requestId,
                                @QueryParam("type") String type) throws DSPSvcException {
        try {
            BlobResponse blobResponse = blobMetaActor.getCompletedBlobsByRequestIdAndType(requestId, BlobType.valueOf(type));
            if(Objects.isNull(blobResponse))
                throw new DSPSvcException("blob meta for request Id: " + requestId + " and type: " + type + " not found");
             return Response.ok().type(MediaType.APPLICATION_JSON).entity(blobResponse).build();
        } catch (IllegalArgumentException e) {
            Error error = new Error(400, e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }


    @POST
    @Timed
    @UnitOfWork
    public Response createBlob(BlobRequest blobRequest) {
        try {
            return Response.ok().type(MediaType.APPLICATION_JSON)
                    .entity(blobMetaActor.persist(blobRequest))
                    .build();
        } catch (IllegalArgumentException | HDFSUtilsException e) {
            Error error = new Error(Response.Status.BAD_REQUEST.getStatusCode(), e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }
}
