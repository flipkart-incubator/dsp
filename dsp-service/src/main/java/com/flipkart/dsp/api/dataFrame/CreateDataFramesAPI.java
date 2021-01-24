package com.flipkart.dsp.api.dataFrame;

import com.flipkart.dsp.actors.*;
import com.flipkart.dsp.db.entities.*;
import com.flipkart.dsp.dto.ConfigurableSG.ConfigurableSGPreProcessedDTO;
import com.flipkart.dsp.dto.ConfigurableSG.SGEntity;
import com.flipkart.dsp.models.sg.ConfigurableSGDTO;
import com.flipkart.dsp.models.sg.Signal;
import com.flipkart.dsp.service.CreateSGPreProcessor;
import com.flipkart.dsp.exceptions.ConfigurableSGException;
import com.google.inject.Inject;
import javafx.util.Pair;
import lombok.RequiredArgsConstructor;

import java.util.*;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class CreateDataFramesAPI {
    private final SignalActor signalActor;
    private final DataTableActor dataTableActor;
    private final DataFrameActor dataFrameActor;
    private final DataSourceActor dataSourceActor;
    private final SignalGroupActor signalGroupActor;
    private final CreateSGPreProcessor createSGPreProcessor;
    private final SignalGroupToSignalActor signalGroupToSignalActor;
    private final static String SIGNAL_GROUP_NAME = "dspSignalGroup";

    public List<DataFrameEntity> createDataFrames(ConfigurableSGDTO configurableSGDTO) {
        ConfigurableSGPreProcessedDTO configurableSgPreProcessedDTO = createSGPreProcessor.getSGEntities(configurableSGDTO);
        validateSGConverter(configurableSgPreProcessedDTO);
        final List<DataFrameEntity> dataFrameEntities;
        if(configurableSgPreProcessedDTO.getErrorOutputList().size() == 0) {
            dataFrameEntities = persistEntity(configurableSgPreProcessedDTO);
        } else {
            throw new ConfigurableSGException(configurableSgPreProcessedDTO.getErrorOutputList().toString());
        }
        return dataFrameEntities;
    }

    private void validateSGConverter(ConfigurableSGPreProcessedDTO configurableSgPreProcessedDTO) {
        List<String> errorOutputList = configurableSgPreProcessedDTO.getErrorOutputList();
        for (SGEntity sgEntity : configurableSgPreProcessedDTO.getSgEntityList()) {
            for (Map.Entry<DataTableEntity, List<Pair<SignalEntity, Boolean>>> entry : sgEntity.getDataTableSignalListMap().entrySet()) {
                verifyDataSourceExistence(errorOutputList, entry.getKey().getDataSource().getId());
            }
        }
    }

    private void verifyDataSourceExistence(List<String> errorOutputList, String dataSourceId) {
        DataSourceEntity dataSourceEntity = dataSourceActor.getDataSourceEntity(dataSourceId);
        if(dataSourceEntity == null) {
            errorOutputList.add("Invalid dataSource Id " + dataSourceId + "." +
                    " Mail on dsp-oncall@flipkart.com to register your dataSource or use a different one");
        }
    }
    private List<DataFrameEntity> persistEntity(ConfigurableSGPreProcessedDTO configurableSgPreProcessedDTO) {
        List<DataFrameEntity> persistedDataframes = new ArrayList<>();
        for (SGEntity sgEntity : configurableSgPreProcessedDTO.getSgEntityList()) {
            long timeMilli = new Date().getTime();

            final DataFrameEntity sgDataFrame = sgEntity.getDataFrame();
            LinkedHashSet<Signal> visibleSignals = processDataFrameSignalList(sgDataFrame.getDataFrameConfig().getVisibleSignals());
            sgDataFrame.getDataFrameConfig().setVisibleSignals(visibleSignals);

            Map<String, SignalEntity> signalMap = new HashMap<>();
            Map<String, DataTableEntity> dataTableEntityMap = new HashMap<>();
            sgEntity.getDataTableSignalListMap().forEach((dataTableEntity, signalEntityList) -> {
                if (!dataTableEntityMap.containsKey(dataTableEntity.getId())) {
                    dataTableEntityMap.put(dataTableEntity.getId(), dataTableActor.persistIfNotExist(dataTableEntity));
                }
                signalEntityList.forEach(signalEntityPair -> {
                    final SignalEntity signalEntity = signalEntityPair.getKey();
                    if (!signalMap.containsKey(signalEntity.getName())) {
                        final SignalEntity newSignalEntity = signalActor.persistIfMissing(signalEntity);
                        signalMap.put(signalEntity.getName(), newSignalEntity);
                    }
                });
            });

            SignalGroupEntity signalGroupEntity = new SignalGroupEntity(String.valueOf(timeMilli), SIGNAL_GROUP_NAME, null);
            signalGroupEntity = signalGroupActor.save(signalGroupEntity);
            List<SignalGroupToSignalEntity> signalGroupToSignalEntities = new LinkedList<>();
            for (Map.Entry<DataTableEntity, List<Pair<SignalEntity, Boolean>>> entry : sgEntity.getDataTableSignalListMap().entrySet()) {
                final List<Pair<SignalEntity, Boolean>> signalEntityPairList = entry.getValue();
                final DataTableEntity dataTableEntity = entry.getKey();
                for (Pair<SignalEntity, Boolean> signalEntityPair : signalEntityPairList) {
                    final SignalEntity signalEntity = signalEntityPair.getKey();

                    SignalGroupToSignalEntity signalGroupToSignalEntity = new SignalGroupToSignalEntity(signalGroupEntity,
                            signalMap.get(signalEntity.getName()), signalEntityPair.getValue(),
                            dataTableEntityMap.get(dataTableEntity.getId()));

                    signalGroupToSignalEntities.add(signalGroupToSignalEntity);
                }
            }
            signalGroupToSignalActor.persist(signalGroupToSignalEntities);
            signalGroupEntity.setSignalGroupToSignalEntities(signalGroupToSignalEntities);
            sgDataFrame.setSignalGroupEntity(signalGroupEntity);
            persistedDataframes.add(dataFrameActor.persist(sgDataFrame));
        }
        return persistedDataframes;
    }

    private LinkedHashSet<Signal> processDataFrameSignalList(LinkedHashSet<Signal> signalSet) {

        LinkedHashSet<Signal> eligibleVisibleSignal = new LinkedHashSet<>();
        for(Signal signal : signalSet) {
            if(signal.getIsVisible()) {
                Signal newSignal = new Signal(signal.getName());
                eligibleVisibleSignal.add(newSignal);
            }
        }
        return eligibleVisibleSignal;
    }
}
