
package com.flipkart.dsp.service;

import com.flipkart.dsp.api.dataFrame.CreateDataFramesAPI;
import com.flipkart.dsp.db.entities.DataFrameEntity;
import com.flipkart.dsp.dto.ConfigurableSG.AutomateSGConfigurationDTO;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.utils.SignalDataTypeMapper;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import java.util.*;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class AutomateSGConfiguration {
    private final CreateDataFramesAPI createDataFramesAPI;
    private final SignalDataTypeMapper signalDataTypeMapper;

    /**
     * This method will create a new table in fdp_fact db on hadoopcluster2
     * taking defination from hadoop fact.
     * It will also create all the sg related configuration in db using the new table
     *
     * @param dataFrameName
     * @param tableName
     */
    public DataFrameEntity createNewSGConfiguration(String dataFrameName, String tableName,
                                                    List<AutomateSGConfigurationDTO.ColumnDetails> columnDetailsList,
                                                    String DBName, AutomateSGConfigurationDTO.SGType sgType,
                                                    Map<SignalDataType, String> datatypeDefaults) {

        LinkedList<AutomateSGConfigurationDTO.SignalDetail> signalDetails = new LinkedList<>();
        for (AutomateSGConfigurationDTO.ColumnDetails fdpColumns : columnDetailsList) {
            Boolean isVisible = true;


            /**
             * Due to bug which does not take primary signal in table defination.Making all as false
             */
            Boolean isPrimary = false;

            if (fdpColumns.isPartition()) {
                isVisible = false;
            }
            if (fdpColumns.isPrimaryKey()) {
                isPrimary = false;
            }

            SignalDataType signalDataType;
            String signalId;
            String factNameOnHive;
            signalDataType = SignalDataType.valueOf(fdpColumns.getType());
            signalId = fdpColumns.getName();
            factNameOnHive = tableName;


            AutomateSGConfigurationDTO.SignalDetail signalDetail = AutomateSGConfigurationDTO.SignalDetail.builder()
                    .signalId(signalId)
                    .signalBaseEntity(fdpColumns.getName())
                    .isPrimary(isPrimary)
                    .isVisible(isVisible)
                    .signalDataType(signalDataType)
                    .dbName(DBName)
                    .tableName(factNameOnHive)
                    .build();

            signalDetails.add(signalDetail);
        }

        DataFrame sgDataFrame = createDefaultSGConfig(dataFrameName, signalDetails);
        ConfigurableSGDTO configurableSGDTO = new ConfigurableSGDTO();
        configurableSGDTO.setDataFrameList(Collections.singletonList(sgDataFrame));
        // signal defaults added
        datatypeDefaults = datatypeDefaults == null ? new HashMap<>() : datatypeDefaults;
        configurableSGDTO.setDatatypeDefaults(datatypeDefaults);
        return createDataFramesAPI.createDataFrames(configurableSGDTO).get(0);
    }

    /**
     * This method will help you create a Default DataFrame configuration.
     *
     * @param dataFrameName
     * @param signalDetails - contains signalId, signal base entity, isPrimary, isVisible, signalData Type, DataSource id, dataTable Name, default Value
     * @return
     */
    public DataFrame createDefaultSGConfig(String dataFrameName,
                                           LinkedList<AutomateSGConfigurationDTO.SignalDetail> signalDetails) {

        DataFrame sgDataFrame = new DataFrame();
        sgDataFrame.setName(dataFrameName);
        LinkedHashSet<Signal> visibleSignals = new LinkedHashSet<>();
        signalDetails.stream().forEach(s -> {
            Signal signal = Signal.builder().name(s.getSignalId())
                    .signalBaseEntity(s.getSignalBaseEntity())
                    .isPrimary(s.getIsPrimary())
                    .isVisible(s.getIsVisible())
                    .signalDataType(s.getSignalDataType())
                    .dataSourceID(s.getDbName())
                    .dataTableName(s.getTableName())
                    .build();
            visibleSignals.add(signal);
        });
        sgDataFrame.setDataFrameConfig(DataFrameConfig.builder()
                .visibleSignals(visibleSignals)
                .build());
        return sgDataFrame;
    }
}
