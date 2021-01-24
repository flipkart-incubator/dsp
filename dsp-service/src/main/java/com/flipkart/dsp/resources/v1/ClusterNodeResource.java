package com.flipkart.dsp.resources.v1;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.HdfsActor;
import com.flipkart.dsp.api.ClusterNodeAPI;
import com.flipkart.dsp.dto.Error;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Slf4j
@Api("cluster_node")
@Path("/v1")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ClusterNodeResource {

    private final HdfsActor hdfsActor;
    private final ClusterNodeAPI clusterNodeAPI;

    @GET
    @Path("active-nn")
    @Timed
    @UnitOfWork
    public Response findActiveNameNode(@DefaultValue("hadoopcluster2") @QueryParam("cluster") String clusterName) {
        try {
            return Response.ok().type(APPLICATION_JSON).entity(hdfsActor.getActiveNameNode(clusterName)).build();
        } catch (Exception e) {
            Error error = new Error(400, e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }

    @GET
    @Path("cluster-hosts")
    @Timed
    @UnitOfWork
    public Response getActiveClusterNode(@DefaultValue("hadoopcluster2") @QueryParam("cluster") String clusterName) {
        try {
            return Response.ok().type(APPLICATION_JSON).entity(clusterNodeAPI.getClusterNodes(clusterName)).build();
        } catch (Exception e) {
            Error error = new Error(400, e.getMessage());
            return Response.status(Response.Status.BAD_REQUEST).entity(error).build();
        }
    }
}
