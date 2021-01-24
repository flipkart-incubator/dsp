package com.flipkart.dsp.api.dataFrame;

import com.flipkart.dsp.actors.DataSourceActor;
import com.flipkart.dsp.config.HiveConfig;
import com.flipkart.dsp.db.entities.DataFrameEntity;
import com.flipkart.dsp.dto.ConfigurableSG.AutomateSGConfigurationDTO;
import com.flipkart.dsp.exception.DataFrameCreationException;
import com.flipkart.dsp.exceptions.DataframeOverrideException;
import com.flipkart.dsp.models.RequestOverride;
import com.flipkart.dsp.models.overrides.*;
import com.flipkart.dsp.models.sg.SignalDataType;
import com.flipkart.dsp.models.workflow.CreateWorkflowRequest;
import com.flipkart.dsp.qe.clients.MetaStoreClient;
import com.flipkart.dsp.service.AutomateSGConfiguration;
import com.flipkart.dsp.utils.SignalDataTypeMapper;
import lombok.RequiredArgsConstructor;

import javax.inject.Inject;
import java.util.*;

import static com.flipkart.dsp.utils.Constants.DUMMY_CSV_TABLE;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class DataframeOverrideAPI {
    private final HiveConfig hiveConfig;
    private final MetaStoreClient metaStoreClient;
    private final DataSourceActor dataSourceActor;
    private final GetDataFramesAPI getDataFramesAPI;
    private final UpdateDataFramesAPI updateDataFramesAPI;
    private final SignalDataTypeMapper signalDataTypeMapper;
    private final AutomateSGConfiguration automateSGConfiguration;

    // for each overridden dataframe, check if dataframe exists and validate, else create new
    public List<DataFrameEntity> createMissingDataFrames(CreateWorkflowRequest createWorkflowRequest) throws DataFrameCreationException {
        List<DataFrameEntity> dataFrameEntityList = new ArrayList<>();
        String hiveQueue = createWorkflowRequest.getHiveQueue();
        RequestOverride requestOverride = createWorkflowRequest.getRequestOverride();
        Map<SignalDataType, String> datatypeDefaults = createWorkflowRequest.getDatatypeDefaults();

        if (requestOverride == null) {
            return dataFrameEntityList;
        }
        try {
            for (String dataFrame : requestOverride.getDataframeOverrideMap().keySet()) {
                DataframeOverride dataFrameoverride = requestOverride.getDataframeOverrideMap().get(dataFrame);
                if ((dataFrameoverride instanceof CSVDataframeOverride)) {
                    final DataFrameEntity dataFrameEntity = handleCSVDataFrameOverride(dataFrame, ((CSVDataframeOverride) dataFrameoverride).getColumnMapping(), datatypeDefaults);
                    dataFrameEntityList.add(dataFrameEntity);
                } else if (dataFrameoverride instanceof FTPDataframeOverride) {
                    final DataFrameEntity dataFrameEntity = handleCSVDataFrameOverride(dataFrame, ((FTPDataframeOverride) dataFrameoverride).getColumnMapping(), datatypeDefaults);
                    dataFrameEntityList.add(dataFrameEntity);
                } else if (dataFrameoverride instanceof DefaultDataframeOverride || dataFrameoverride instanceof RunIdDataframeOverride) {
                    //todo: selecting dataframe randomly based on name.
                    final List<DataFrameEntity> sgDataFrameEntitiesByName = getDataFramesAPI.getDataFrameEntitiesByName(dataFrame);
                    if (sgDataFrameEntitiesByName.isEmpty()) {
                        throw new DataframeOverrideException("Failed to override DataFrame: " + dataFrame + ". DataFrame does not exist in platform!");
                    } else {
                        dataFrameEntityList.add(sgDataFrameEntitiesByName.get(0));
                    }

                }
            }
        } catch (DataframeOverrideException e) {
            throw new DataFrameCreationException("Error while creating DataFrames. Error: " + e.getMessage());
        }
        updateDataFramesAPI.updateDataframeIds(dataFrameEntityList, createWorkflowRequest);
        return dataFrameEntityList;
    }

    private DataFrameEntity handleCSVDataFrameOverride(String dataFrame, LinkedHashMap<String, SignalDataType> columnMapping,
                                                       Map<SignalDataType, String> datatypeDefaults) {
        final Optional<DataFrameEntity> similarDataframe = getDataFramesAPI.getSimilarDataframeEntity(dataFrame,
                columnMapping, datatypeDefaults);
        return similarDataframe.orElseGet(() -> createDataFrame(dataFrame, DUMMY_CSV_TABLE, "database",
                AutomateSGConfigurationDTO.SGType.CSV, datatypeDefaults, columnMapping));
    }


    private DataFrameEntity createDataFrame(String dataFrameName, String tableName, String dbName, AutomateSGConfigurationDTO.SGType sgType,
                                            Map<SignalDataType, String> datatypeDefaults, LinkedHashMap<String, SignalDataType> columnMapping) {
        List<AutomateSGConfigurationDTO.ColumnDetails> columnDetailsList = transformToColumnDetails(columnMapping);
        return automateSGConfiguration.createNewSGConfiguration(dataFrameName, tableName, columnDetailsList,
                dbName, sgType, datatypeDefaults);

    }

    private LinkedHashMap<String, SignalDataType> getColumnMapping(List<AutomateSGConfigurationDTO.ColumnDetails> columnDetails) {
        LinkedHashMap<String, SignalDataType> columnMapping = new LinkedHashMap<>();
        for (AutomateSGConfigurationDTO.ColumnDetails fdpColumn : columnDetails) {
            columnMapping.put(fdpColumn.getName(), signalDataTypeMapper.getSignalDataType(fdpColumn.getType()));
        }
        return columnMapping;
    }

    private List<AutomateSGConfigurationDTO.ColumnDetails> transformToColumnDetails(LinkedHashMap<String, SignalDataType> columnMapping) {
        List<AutomateSGConfigurationDTO.ColumnDetails> columnDetails = new ArrayList<>();
        for (String column : columnMapping.keySet()) {
            AutomateSGConfigurationDTO.ColumnDetails columnDetail = new AutomateSGConfigurationDTO.ColumnDetails();
            columnDetail.setName(column);
            columnDetail.setType(columnMapping.get(column).name());
            columnDetails.add(columnDetail);
        }
        return columnDetails;
    }
}
