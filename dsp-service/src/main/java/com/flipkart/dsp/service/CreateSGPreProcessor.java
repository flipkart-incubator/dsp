package com.flipkart.dsp.service;

import com.flipkart.dsp.db.entities.DataFrameEntity;
import com.flipkart.dsp.db.entities.DataSourceEntity;
import com.flipkart.dsp.db.entities.DataTableEntity;
import com.flipkart.dsp.db.entities.SignalEntity;
import com.flipkart.dsp.dto.ConfigurableSG.ConfigurableSGPreProcessedDTO;
import com.flipkart.dsp.dto.ConfigurableSG.SGEntity;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.utils.SGDefaultValuePopulator;
import com.google.inject.Inject;
import javafx.util.Pair;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class CreateSGPreProcessor {
    private final SGDefaultValuePopulator SGDefaultValuePopulator;

    public ConfigurableSGPreProcessedDTO getSGEntities(ConfigurableSGDTO configurableSGDTO) {
        ConfigurableSGPreProcessedDTO configurableSgPreProcessedDTO = new ConfigurableSGPreProcessedDTO();
        List<String> errorList = new ArrayList<>();
        List<SGEntity> sgEntityList = new ArrayList<>();

        for(DataFrame sgDataFrame : configurableSGDTO.getDataFrameList()) {
            Map<DataTableEntity, List<Pair<SignalEntity,Boolean>>> dataTableToSignals = new HashMap<>();
            DataFrameEntity dataFrameEntity = new DataFrameEntity(null, sgDataFrame.getName(),
                    null, sgDataFrame.getDataFrameConfig());
            for(Signal signal : sgDataFrame.getDataFrameConfig().getVisibleSignals()) {

                DataTableEntity dataTableEntity = getDataTableObject(signal.getDataSourceID(), signal.getDataTableName());
                if(signal.getIsPrimary() == null) {
                    signal.setIsPrimary(false);
                }
                List<Pair<SignalEntity, Boolean>> signalEntitieslist = getSignalEntities(dataTableToSignals, dataTableEntity);
                String defaultValue = configurableSGDTO.getDatatypeDefaults() != null ?
                        configurableSGDTO.getDatatypeDefaults().getOrDefault(signal.getSignalDataType(),"NULL")
                        : null;
                SignalEntity signalEntity = getSignalEntity(signal, defaultValue);

                signalEntitieslist.add(new Pair<>(signalEntity, signal.getIsPrimary()));
                dataTableToSignals.put(dataTableEntity, signalEntitieslist);
            }
            SGEntity sgEntity = new SGEntity(dataTableToSignals, dataFrameEntity);
            checkForDuplicateSignal(sgEntity.getDataTableSignalListMap(), errorList);
            sgEntityList.add(sgEntity);
        }
        configurableSgPreProcessedDTO.setSgEntityList(sgEntityList);
        configurableSgPreProcessedDTO.setErrorOutputList(errorList);
        return configurableSgPreProcessedDTO;
    }

    private SignalEntity getSignalEntity(Signal signal, String defaultValue) {
        final SignalDefinition signalDefinitionNew = SGDefaultValuePopulator.getSignalDefinition(signal.getSignalDefinition(), defaultValue);
        return new SignalEntity(null, signal.getName(), signal.getSignalDataType(), signalDefinitionNew, signal.getSignalBaseEntity());
    }

    private List<Pair<SignalEntity,Boolean>> getSignalEntities(Map<DataTableEntity, List<Pair<SignalEntity,Boolean>>> dataTableToSignals, DataTableEntity dataTableEntity) {
        if(dataTableToSignals.containsKey(dataTableEntity)) {
            return dataTableToSignals.get(dataTableEntity);
        } else {
            return new ArrayList<>();
        }
    }

    private void checkForDuplicateSignal(Map<DataTableEntity, List<Pair<SignalEntity,Boolean>>> dataTableSignalListMap, List<String> errorList) {

        for(Map.Entry<DataTableEntity, List<Pair<SignalEntity,Boolean>>> entry : dataTableSignalListMap.entrySet()) {
            List<String> allSignal = entry.getValue().stream().map(s -> s.getKey().getName()).collect(Collectors.toList());
            List<String> uniqueSignal = new ArrayList<>();
            for(String signal : allSignal) {
                if(uniqueSignal.contains(signal)) {
                    errorList.add("Duplicate Signal " + signal + " for DataTable " + entry.getKey().getId());
                } else {
                    uniqueSignal.add(signal);
                }
            }
        }
    }


    private DataTableEntity getDataTableObject(String dataSourceId, String datatable) {
        DataSourceEntity dataSource = new DataSourceEntity(dataSourceId, null);
        return new DataTableEntity(datatable, "Created via API", dataSource);
    }
}
