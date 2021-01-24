package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.flipkart.dsp.config.MiscConfig;
import com.google.common.io.Files;
import io.swagger.annotations.Api;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Date;

/**
 */
@Api("elb-healthcheck")
@Path("/elb-healthcheck")
@Produces(MediaType.APPLICATION_JSON)
public class HealthCheckResource {

    private final File rotationStatusFile;

    @Inject
    public HealthCheckResource(MiscConfig config) {
        this.rotationStatusFile = new File(config.getServiceOorFile());
        this.rotationStatusFile.delete();
    }

    @GET
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    public Response checkHealth() {
        if (rotationStatusFile.exists()) {
            return Response.status(Response.Status.NOT_FOUND)
                    .entity(new HealthCheckResponse(new Date()))
                    .type(MediaType.APPLICATION_JSON)
                    .build() ;
        } else {
            return Response.status(Response.Status.OK)
                    .entity(new HealthCheckResponse(new Date(rotationStatusFile.lastModified())))
                    .type(MediaType.APPLICATION_JSON)
                    .build() ;
        }

    }

    @POST
    @Timed
    @Path("oor")
    public Response oor() throws IOException {
        String message = "" ;
        if (rotationStatusFile.exists()) {
            message = "Already oor!!" ;
        } else {
            Files.write("OOR", rotationStatusFile, Charset.defaultCharset());
            message = "oor done!!" ;
        }
        return Response.status(Response.Status.OK)
                .entity(message)
                .type(MediaType.APPLICATION_JSON)
                .build() ;
    }

    @POST
    @Timed
    @Path("bir")
    public Response bir() {
        String message = "" ;
        if (!rotationStatusFile.exists()) {
            message = "Already in Rotation" ;
        } else {
            this.rotationStatusFile.delete();
            message = "Back in Rotation" ;
        }
        return Response.status(Response.Status.OK)
                .entity(message)
                .type(MediaType.APPLICATION_JSON)
                .build() ;
    }


    public static class HealthCheckResponse {
        @JsonProperty
        long uptime ;
        @JsonProperty
        long requests ;
        @JsonProperty
        long capacity ;

        public HealthCheckResponse(Date date) {
            this.uptime = date.getTime() / 1000;
            this.requests = 1000;
            this.capacity = 100;
        }
    }
}
