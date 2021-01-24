package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.DataFrameDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.DataFrameEntity;
import com.flipkart.dsp.models.sg.*;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

/**
 */

@RequiredArgsConstructor(onConstructor = @__(@Inject))
@Slf4j
public class DataFrameActor implements SGActor<DataFrameEntity, DataFrame> {
    private final SignalActor signalActor;
    private final DataFrameDAO dataFrameDAO;
    private final SignalGroupActor signalGroupActor;
    private final TransactionLender transactionLender;

    @Override
    public DataFrameEntity unWrap(DataFrame dto) {
        if (Objects.nonNull(dto))
            return DataFrameEntity.builder().id(dto.getId()).name(dto.getName()).dataFrameConfig(dto.getDataFrameConfig())
                    .signalGroupEntity(signalGroupActor.unWrap(dto.getSignalGroup())).build();
        return null;
    }

    @Override
    public DataFrame wrap(DataFrameEntity entity) {
        if (Objects.nonNull(entity)) {
            List<Signal> missingSignals = new ArrayList<>();
            LinkedHashSet<Signal> visibleSignals = new LinkedHashSet<>();
            Map<String, Signal> signalNameMap = new HashMap<>();
            entity.getSignalGroupEntity().getSignalGroupToSignalEntities().forEach(sm -> {
                Signal signal = signalActor.wrap(sm.getSignal());
                signal.setDataSourceID(sm.getDataTableEntity().getDataSource().getId());
                signal.setDataTableName(sm.getDataTableEntity().getId());
                if (signalNameMap.put(sm.getSignal().getName(), signal) != null) {
                    throw new IllegalStateException("Duplicate key");
                }
            });
            Set<DataFrameScope> dataFrameScopes = getDataFrameScopes(entity, signalNameMap);
            populateMissingAndVisibleSignals(entity, signalNameMap, missingSignals, visibleSignals);
            SignalGroup signalGroup = signalGroupActor.wrap(entity.getSignalGroupEntity());
            populateSignalMetas(signalGroup, visibleSignals);
            return DataFrame.builder().id(entity.getId()).name(entity.getName()).signalGroup(signalGroup)
                    .dataFrameConfig(new DataFrameConfig(dataFrameScopes, visibleSignals)).build();
        }
        return null;
    }

    private Set<DataFrameScope> getDataFrameScopes(DataFrameEntity dataFrameEntity, Map<String, Signal> signalNameMap) {
        Set<DataFrameScope> dataFrameScopes = new HashSet<>();
        if (dataFrameEntity.getDataFrameConfig().getDataFrameScopeSet() != null) {
            for (DataFrameScope dataFrameScope : dataFrameEntity.getDataFrameConfig().getDataFrameScopeSet()) {
                Signal signal = signalNameMap.get(dataFrameScope.getSignal().getName());
                dataFrameScopes.add(new DataFrameScope(signal, dataFrameScope.getAbstractPredicateClause()));
            }
        }
        return dataFrameScopes;
    }

    private void populateMissingAndVisibleSignals(DataFrameEntity dataFrameEntity, Map<String, Signal> signalNameMap,
                                                  List<Signal> missingSignals, LinkedHashSet<Signal> visibleSignals) {
        dataFrameEntity.getDataFrameConfig().getVisibleSignals().forEach(signal -> {
            Signal visibleSignal = signalNameMap.get(signal.getName());
            if (visibleSignal == null) missingSignals.add(signal);
            visibleSignals.add(visibleSignal);
        });

        if (!missingSignals.isEmpty()) {
            String missingVisibleSignals = missingSignals.stream().map(Signal::getName).collect(Collectors.joining(", "));
            log.error("Configuration Error!! Following visible signals named: {} are not part of the signal group!", missingVisibleSignals);
            throw new IllegalArgumentException("Configuration Error!! Following visible signals named: " + missingVisibleSignals + " are not part of the signal group!");
        }
    }

    private void populateSignalMetas(SignalGroup signalGroup, LinkedHashSet<Signal> visibleSignals) {
        List<SignalGroup.SignalMeta> orderedSignalMetaList = new ArrayList<>();
        Map<Long, SignalGroup.SignalMeta> idToSignalMeta = signalGroup.getSignalMetas().stream()
                .collect(Collectors.toMap(s -> s.getSignal().getId(), y -> y));

        visibleSignals.forEach(signal -> {
            orderedSignalMetaList.add(idToSignalMeta.get(signal.getId()));
            idToSignalMeta.remove(signal.getId());
        });
        if (idToSignalMeta.size() != 0) orderedSignalMetaList.addAll(new ArrayList<>(idToSignalMeta.values()));
        signalGroup.setSignalMetas(orderedSignalMetaList);
    }

    public DataFrameEntity persist(DataFrameEntity dataFrameEntity) {
        AtomicReference<DataFrameEntity> atomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(dataFrameDAO.persist(dataFrameEntity));
            }
        }, "Error while creating DataFrame.");
        return atomicReference.get();
    }

    public DataFrame getDataFrameById(Long id) {
        AtomicReference<DataFrameEntity> atomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(dataFrameDAO.get(id));
            }
        }, "Error while getting DataFrame for id: " + id);
        return wrap(atomicReference.get());
    }

    public Long getUsedDataFrameCount(String signalGroupId) {
        AtomicReference<Long> signalGroupCount = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                signalGroupCount.set(dataFrameDAO.getUsedDataFrameCount(signalGroupId));
            }
        }, "Error while getting dataFrames for signalGroupId: " + signalGroupId);
        return signalGroupCount.get();
    }

    public void deleteDataFrames(List<DataFrame> dataFrames) {
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                dataFrameDAO.deleteDataframe(dataFrames.stream().map(DataFrame::getId).collect(toList()));
            }
        }, "Error while Deleting  DataFrames for ids: " + dataFrames.stream().map(DataFrame::getId).collect(toList()));
    }

    public List<DataFrame> getAllDataFrames() {
        AtomicReference<List<DataFrameEntity>> dataFrameAtomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                dataFrameAtomicReference.set(dataFrameDAO.getAllDataFrames());
            }
        }, "Error while getting DataFrames");
        return dataFrameAtomicReference.get().stream().map(this::wrap).collect(toList());
    }
}
