package com.flipkart.dsp.api.dataFrame;

import com.flipkart.dsp.db.entities.DataFrameEntity;
import com.flipkart.dsp.exceptions.ConfigurableSGException;
import com.flipkart.dsp.models.sg.ConfigurableSGDTO;
import com.flipkart.dsp.models.sg.DataFrame;
import com.flipkart.dsp.models.workflow.CreateWorkflowRequest;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class UpdateDataFramesAPI {
    private final DeleteDataFramesAPI deleteDataFramesAPI;
    private final CreateDataFramesAPI createDataFramesAPI;

    public void updateDataFrames(ConfigurableSGDTO configurableSGDTO) {
        try {
            List<Long> oldDataFrameIds = configurableSGDTO.getDataFrameList().stream().map(DataFrame::getId).collect(Collectors.toList());
            deleteDataFramesAPI.deleteDataFrames(oldDataFrameIds);
            createDataFramesAPI.createDataFrames(configurableSGDTO);
        } catch (Exception e) {
            throw new ConfigurableSGException(e.getMessage());
        }
    }

    public void updateDataframeIds(List<DataFrameEntity> dataFrameEntityList, CreateWorkflowRequest createWorkflowRequest) {
        final Map<String, DataFrameEntity> dataFrameEntityMap = dataFrameEntityList.stream()
                .collect(Collectors.toMap(DataFrameEntity::getName, Function.identity()));

        createWorkflowRequest.getWorkflow().getDataframes().stream()
                .filter(dataframe -> dataFrameEntityMap.containsKey(dataframe.getName()))
                .forEach(dataframe -> dataframe.setId(dataFrameEntityMap.get(dataframe.getName()).getId()));
    }
}
