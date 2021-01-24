package com.flipkart.dsp.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.dsp.api.RunDetailsAPI;
import com.flipkart.dsp.api.VisibilityAPI;
import com.flipkart.dsp.dto.RunDetailsDTO;
import com.flipkart.dsp.dto.VisibilityDTO;
import com.flipkart.dsp.exceptions.ValidationException;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.dropwizard.hibernate.UnitOfWork;
import io.swagger.annotations.Api;
import lombok.RequiredArgsConstructor;

import javax.ws.rs.*;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

@Api("dataframes")
@Path("/v1/dataframes")
@Produces(APPLICATION_JSON)
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class VisibilityResource {
    private static final String COMMA = ",";
    private final VisibilityAPI visibilityAPI;
    private final RunDetailsAPI runDetailsAPI;

    @GET
    @Path("/{dataframe_ids}")
    @UnitOfWork
    @Timed
    public Map<String, VisibilityDTO.VisibilityDataFrame> getVisibilityDetails(@PathParam("dataframe_ids") String dataFrameIds) throws ValidationException {
        checkNotNull(dataFrameIds, "dataFrameIds cannot be null!!");
        final List<Long> dataFrameIdList = Lists.newArrayList(dataFrameIds.split(COMMA)).stream().map(Long::valueOf).collect(Collectors.toList());
        return visibilityAPI.getDataFrameDetails(dataFrameIdList);
    }

    @GET
    @Path("/{dataframe_id}/runs")
    @UnitOfWork
    @Timed
    public RunDetailsDTO getRunIdDetails(@PathParam("dataframe_id") String dataFrameId, @QueryParam("limit") @DefaultValue("5") Integer noOfRuns) {
        checkNotNull(dataFrameId, "dataFrame Name cannot be null!!");
        return runDetailsAPI.getRunIDDetails(dataFrameId, noOfRuns);
    }
}
