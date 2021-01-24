package com.flipkart.dsp.api.dataFrame;

import com.flipkart.dsp.actors.DataFrameActor;
import com.flipkart.dsp.dao.DataFrameDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.DataFrameEntity;
import com.flipkart.dsp.exception.CreateWorkflowException;
import com.flipkart.dsp.exceptions.ValidationException;
import com.flipkart.dsp.models.sg.*;
import com.flipkart.dsp.models.workflow.CreateWorkflowRequest;
import com.flipkart.dsp.validation.DataFrameValidator;
import com.flipkart.dsp.validation.SignalValidator;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.Validate;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.commons.collections.CollectionUtils.isEmpty;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class GetDataFramesAPI {
    private final DataFrameDAO dataFrameDAO;
    private final DataFrameActor dataFrameActor;
    private final SignalValidator signalValidator;
    private final TransactionLender transactionLender;
    private final DataFrameValidator dataFrameValidator;

    public Optional<DataFrameEntity> getSimilarDataframeEntity(String dataFrameName, Map<String, SignalDataType> columnNameDataTypeMap,
                                                               Map<SignalDataType, String> datatypeDefaults) {
        Map<SignalDataType, String> safeDatatypeDefaults = datatypeDefaults == null ? new HashMap<>() : datatypeDefaults;
        List<DataFrameEntity> dataFrameList = getDataFrameEntitiesByName(dataFrameName);
        final List<DataFrameEntity> matchingDataFrames = dataFrameList.stream().filter(sgDataFrameEntity -> {
            final DataFrame dataFrame = dataFrameActor.wrap(sgDataFrameEntity);
            return (signalValidator.verifyDataTableColumnDetails(dataFrame, columnNameDataTypeMap) &&
                    signalValidator.verifyDataTableColumnDefaults(dataFrame, safeDatatypeDefaults));
        }).collect(Collectors.toList());
        if (matchingDataFrames.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(matchingDataFrames.get(0));
        }
    }

    List<DataFrameEntity> getDataFrameEntitiesByName(String dataFrameName) {
        AtomicReference<List<DataFrameEntity>> dataFrameAtomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                dataFrameAtomicReference.set(dataFrameDAO.getDataFramesByName(dataFrameName));
            }
        });
        return dataFrameAtomicReference.get();
    }

    public ConfigurableSGDTO getDataFrames(List<Long> dataFrameIds) {
        List<DataFrame> sgDataFrames = dataFrameValidator.validateDataFrames(dataFrameIds);
        processConfiguration(sgDataFrames);
        ConfigurableSGDTO configurableSGDTO = new ConfigurableSGDTO();
        configurableSGDTO.setDataFrameList(sgDataFrames);
        return configurableSGDTO;
    }

    private DataFrame getDataframe(Long id) {
        AtomicReference<DataFrameEntity> dataFrameEntityAtomicReference = new AtomicReference<>();
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() throws Exception {
                final DataFrameEntity dataFrame = dataFrameDAO.getDataFrame(id);
                dataFrameEntityAtomicReference.set(dataFrame);
            }
        });

        return dataFrameActor.wrap(dataFrameEntityAtomicReference.get());
    }

    private void processConfiguration(List<DataFrame> sgDataFramesList) {
        for (DataFrame sgDataFrame : sgDataFramesList) {
            List<String> visibleSignalList = getVisibleSignals(sgDataFrame);
            LinkedHashSet<Signal> allSignals = getAllSignals(visibleSignalList, sgDataFrame.getSignalGroup());
            sgDataFrame.getDataFrameConfig().setVisibleSignals(allSignals);
        }
    }


    private LinkedHashSet<Signal> getAllSignals(List<String> visibleSignalList, SignalGroup signalGroup) {
        LinkedHashSet<Signal> allSignals = new LinkedHashSet<>();
        for (SignalGroup.SignalMeta signalMeta : signalGroup.getSignalMetas()) {
            Signal signal = signalMeta.getSignal();
            signal.setDataTableName(signalMeta.getDataTable().getId());
            signal.setIsPrimary(signalMeta.isPrimary());
            signal.setDataSourceID(signalMeta.getDataTable().getDataSource().getId());
            if (visibleSignalList.contains(signal.getName())) {
                signal.setIsVisible(true);
            } else {
                signal.setIsVisible(false);
            }
            allSignals.add(signal);
        }
        return allSignals;
    }

    private List<String> getVisibleSignals(DataFrame sgDataFrame) {
        List<String> visibleSignals = new ArrayList<>();
        for (Signal signal : sgDataFrame.getDataFrameConfig().getVisibleSignals()) {
            visibleSignals.add(signal.getName());
        }
        return visibleSignals;
    }


    public Set<DataFrame> convertDataFrames(List<CreateWorkflowRequest.Dataframe> dataFrames) throws ValidationException,CreateWorkflowException {
        if (isEmpty(dataFrames)) return new HashSet<>();

        Set<DataFrame> dataFrameSet = new HashSet<>();
        for (CreateWorkflowRequest.Dataframe dataframe : dataFrames) {
            if (Objects.isNull(dataframe.getId())) {
                log.error("Ha ha you screwed up!! Dataframe id cannot be null");
                throw new CreateWorkflowException("Dataframe id cannot be null");
            }
            dataFrameSet.add(dataFrameValidator.validateDataFrame(dataframe.getId()));
        }
        return dataFrameSet;
    }
}
