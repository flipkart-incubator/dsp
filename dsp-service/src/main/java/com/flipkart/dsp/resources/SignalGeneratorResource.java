package com.flipkart.dsp.resources;


import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.actors.DataFrameActor;
import com.flipkart.dsp.api.dataFrame.CreateDataFramesAPI;
import com.flipkart.dsp.api.dataFrame.DeleteDataFramesAPI;
import com.flipkart.dsp.api.dataFrame.GetDataFramesAPI;
import com.flipkart.dsp.api.dataFrame.UpdateDataFramesAPI;
import com.flipkart.dsp.db.entities.DataFrameEntity;
import com.flipkart.dsp.models.sg.ConfigurableSGDTO;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.validation.SignalValidator;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Definitions in the resources are tentative and are expected to change.
 */


@Slf4j
@Path("/v1/sg")
@Api("signal-generation")
@Produces(MediaType.APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
@Consumes(MediaType.APPLICATION_JSON)
public class SignalGeneratorResource {
    private final SignalValidator signalValidator;
    private final DataFrameActor dataFrameActor;
    private final GetDataFramesAPI getDataFramesAPI;
    private final CreateDataFramesAPI createDataFramesAPI;
    private final DeleteDataFramesAPI deleteDataFramesAPI;
    private final UpdateDataFramesAPI updateDataFramesAPI;

    @POST
    @Timed
    @Path("/")
    @Consumes(MediaType.APPLICATION_JSON)
    @UnitOfWork
    public Map<String, Long> persistDataFrame(ConfigurableSGDTO configurableSGDTO) {
        signalValidator.checkForMandatoryFields(configurableSGDTO);
        List<DataFrameEntity> sgDataFrameEntities = createDataFramesAPI.createDataFrames(configurableSGDTO);
        return sgDataFrameEntities.stream().collect(Collectors.toMap(DataFrameEntity::getName, DataFrameEntity::getId));
    }

    @Timed
    @DELETE
    @Path("/{data_frame_ids}")
    @UnitOfWork
    public Response deleteDataFrame(@NonNull @PathParam("data_frame_ids") String dataFrameIds) {
        List<Long> dataFrameIdList = Lists.newArrayList(dataFrameIds.split(",")).stream()
                .map(dataFrameId -> Long.valueOf(dataFrameId.trim())).collect(Collectors.toList());
        return Response.ok().entity(deleteDataFramesAPI.deleteDataFrames(dataFrameIdList)).build();
    }

    @GET
    @Timed
    @Path("/data_frame_names")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> getAllDataFrameNames() {
        return dataFrameActor.getAllDataFrames().stream().map(DataFrame::getName).collect(Collectors.toList());
    }

    @PUT
    @Timed
    @Path("/")
    @UnitOfWork
    @Produces(MediaType.APPLICATION_JSON)
    public ConfigurableSGDTO updateDataFrameDetails(ConfigurableSGDTO configurableSGDTO) {
        signalValidator.checkForMandatoryFields(configurableSGDTO);
        updateDataFramesAPI.updateDataFrames(configurableSGDTO);
        return configurableSGDTO;
    }

    @GET
    @Timed
    @Path("/{data_frame_ids}")
    @Produces(MediaType.APPLICATION_JSON)
    @UnitOfWork(readOnly = true)
    public ConfigurableSGDTO getDataFrameDetails(@NonNull @PathParam("data_frame_ids") String dataFrameIds) {
        final List<Long> dataFrameIdList = Lists.newArrayList(dataFrameIds.split(",")).stream()
                .map(dataFrameName -> Long.valueOf(dataFrameName.trim())).collect(Collectors.toList());
        return getDataFramesAPI.getDataFrames(dataFrameIdList);
    }

}
