package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.ExecutionEnvironmentActor;
import com.flipkart.dsp.actors.ExecutionEnvironmentSnapShotActor;
import com.flipkart.dsp.entities.misc.ImageDetail;
import com.flipkart.dsp.exception.DSPSvcException;
import com.flipkart.dsp.models.ExecutionEnvironmentSnapshot;
import com.flipkart.dsp.models.ExecutionEnvironmentSummary;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Objects;

/**
 */
@Slf4j
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Api("execution-environment-snapshots")
@Path("/v1/execution-environment-snapshots")
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class ExecutionEnvironmentSnapshotResource {

    private final ExecutionEnvironmentActor executionEnvironmentActor;
    private final ExecutionEnvironmentSnapShotActor executionEnvironmentSnapshotActor;

    @GET
    @Timed
    @UnitOfWork
    public List<ImageDetail> getExecutionEnvironmentSnapShotsDetails() {
        return executionEnvironmentSnapshotActor.getImageDetails();
    }

    @POST
    @Path("/create")
    @Timed
    @UnitOfWork
    public long createExecutionEnvironmentSnapshot(ExecutionEnvironmentSnapshot executionEnvironmentSnapshot) throws DSPSvcException {
        validateExecutionEnvironment(executionEnvironmentSnapshot.getExecutionEnvironmentId());
        return executionEnvironmentSnapshotActor.save(executionEnvironmentSnapshot);
    }

    private void validateExecutionEnvironment(Long executionEnvironmentId) throws DSPSvcException {
        ExecutionEnvironmentSummary executionEnvironmentSummary = executionEnvironmentActor.getExecutionEnvironmentById(executionEnvironmentId);
        if (Objects.isNull(executionEnvironmentSummary))
            throw new DSPSvcException("Execution Environment not found for id: "  + executionEnvironmentId);
    }
}
