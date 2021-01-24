package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.ExecutionEnvironmentActor;
import com.flipkart.dsp.models.ExecutionEnvironmentSummary;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Set;

@Api("execution-environments")
@Path("/v1/execution-environments")
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ExecutionEnvironmentResource {
    private final ExecutionEnvironmentActor executionEnvironmentActor;

    @GET
    @Timed
    @UnitOfWork
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/html/image-names")
    public Response getExecutionEnvironmentNameForDisplay() {
        Set<String> set = executionEnvironmentActor.getAllExecutionEnvironmentNames();
        return Response.ok(set).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Timed
    @UnitOfWork
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Path("/image-names")
    public Set<String> getExecutionEnvironmentName() {
        return executionEnvironmentActor.getAllExecutionEnvironmentNames();
    }

    @GET
    @Timed
    @UnitOfWork
    public List<ExecutionEnvironmentSummary> getExecutionEnvironmentDetails() {
        return getAllExecutionEnvironmentDetails();
    }

    private List<ExecutionEnvironmentSummary> getAllExecutionEnvironmentDetails() {
        return executionEnvironmentActor.getAllExecutionEnvironments();
    }


    @GET
    @Timed
    @UnitOfWork
    @Path("{execution-env}")
    public ExecutionEnvironmentSummary getExecutionEnvironment(@PathParam("execution-env") String executionEnv) {
        return executionEnvironmentActor.getExecutionEnvironmentByName(executionEnv);
    }



    @GET
    @Timed
    @UnitOfWork
    @Path("/summary/{execution-env}")
    public ExecutionEnvironmentSummary getExecutionEnvironmentSummary(@PathParam("execution-env") String executionEnv) {
        return executionEnvironmentActor.getExecutionEnvironmentSummary(executionEnv);
    }

    @GET
    @Timed
    @UnitOfWork
    @Path("/html/summary/{execution-env}")
    public Response getExecutionEnvironmentDetailsForDisplay(@PathParam("execution-env") String executionEnv) {
        return Response.ok(executionEnvironmentActor.getExecutionEnvironmentSummary(executionEnv)).header("Access-Control-Allow-Origin", "*").build();
    }
}
