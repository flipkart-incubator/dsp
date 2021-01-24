package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.SignalGroupDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.SignalGroupEntity;
import com.flipkart.dsp.db.entities.SignalGroupToSignalEntity;
import com.flipkart.dsp.models.sg.DataTable;
import com.flipkart.dsp.models.sg.Signal;
import com.flipkart.dsp.models.sg.SignalGroup;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 */

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class SignalGroupActor implements SGActor<SignalGroupEntity, SignalGroup> {

    private final SignalActor signalActor;
    private final SignalGroupDAO signalGroupDAO;
    private final DataTableActor dataTableActor;
    private final TransactionLender transactionLender;

    @Override
    public SignalGroupEntity unWrap(SignalGroup dto) {
        if (Objects.nonNull(dto)) {
            SignalGroupEntity signalGroupEntity = SignalGroupEntity.builder().id(dto.getId()).description(dto.getDescription()).build();
            List<SignalGroupToSignalEntity> signalGroupToSignalEntities = dto.getSignalMetas().stream()
                    .map(signalMeta -> SignalGroupToSignalEntity.builder().signal(signalActor.unWrap(signalMeta.getSignal()))
                            .signalGroup(signalGroupEntity).primary(signalMeta.isPrimary())
                            .dataTableEntity(dataTableActor.unWrap(signalMeta.getDataTable())).build()).collect(Collectors.toList());
            signalGroupEntity.setSignalGroupToSignalEntities(signalGroupToSignalEntities);
            return signalGroupEntity;
        }
        return null;
    }

    @Override
    public SignalGroup wrap(SignalGroupEntity entity) {
        if (Objects.nonNull(entity)) {
            List<SignalGroup.SignalMeta> signalMetas = entity.getSignalGroupToSignalEntities().stream().map(this::getSignalMeta).collect(Collectors.toList());
            return new SignalGroup(entity.getId(), entity.getDescription(), signalMetas);
        }
        return null;
    }

    public SignalGroup getSignalGroup(String signalGroupId) {
        AtomicReference<SignalGroupEntity> signalGroupEntityAtomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                signalGroupEntityAtomicReference.set(signalGroupDAO.get(signalGroupId));
            }
        });
        return wrap(signalGroupEntityAtomicReference.get());
    }

    public SignalGroupEntity save(SignalGroupEntity signalGroupEntity) {
        AtomicReference<SignalGroupEntity> atomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(signalGroupDAO.persist(signalGroupEntity));
            }
        });
        return atomicReference.get();
    }

    private SignalGroup.SignalMeta getSignalMeta(SignalGroupToSignalEntity signalGroupToSignalEntity) {
        Signal signal = signalActor.wrap(signalGroupToSignalEntity.getSignal());
        DataTable dataTable = dataTableActor.wrap(signalGroupToSignalEntity.getDataTableEntity());
        return new SignalGroup.SignalMeta(signal, signalGroupToSignalEntity.isPrimary(), dataTable);
    }

    public void deleteSignalGroups(List<String> signalGroupLists) {
        if (signalGroupLists.size() > 0) {
            signalGroupDAO.deleteSignalGroup(signalGroupLists);
        }
    }
}
