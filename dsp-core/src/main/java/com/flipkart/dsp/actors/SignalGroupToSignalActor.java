package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.SignalGroupToSignalDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.SignalGroupToSignalEntity;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class SignalGroupToSignalActor {
    private final TransactionLender transactionLender;
    private final SignalGroupToSignalDAO signalGroupToSignalDAO;

    public SignalGroupToSignalEntity persist(SignalGroupToSignalEntity signalGroupToSignalEntity) {
        AtomicReference<SignalGroupToSignalEntity> atomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(signalGroupToSignalDAO.persist(signalGroupToSignalEntity));
            }
        });
        return atomicReference.get();
    }

    public List<SignalGroupToSignalEntity> persist(List<SignalGroupToSignalEntity> signalGroupToSignalEntity) {
        AtomicReference<List<SignalGroupToSignalEntity>> listAtomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                listAtomicReference.set(signalGroupToSignalEntity.stream().map(signalGroupToSignalDAO::persist).collect(Collectors.toList()));
            }
        });
        return listAtomicReference.get();
    }

    public Long getSignalCount(String signal) {
        AtomicReference<Long> signalAtomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                signalAtomicReference.set(signalGroupToSignalDAO.getSignalCount(signal));
            }
        });
        return signalAtomicReference.get();
    }

    public Long getDataTableCount(String datatableName) {
        AtomicReference<Long> signalAtomicReference = new AtomicReference<>(null);
        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                signalAtomicReference.set(signalGroupToSignalDAO.getDataTableCount(datatableName));
            }
        });
        return signalAtomicReference.get();
    }

    public void deleteSignalGroup(List<String> signalGroupName) {
        if (signalGroupName.size() > 0) signalGroupToSignalDAO.deleteSignalGroup(signalGroupName);
    }

}
