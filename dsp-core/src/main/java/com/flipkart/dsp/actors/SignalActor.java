package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.SignalDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.SignalEntity;
import com.flipkart.dsp.exceptions.EntityNotFoundException;
import com.flipkart.dsp.models.sg.Signal;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Optional.of;

/**
 */

@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class SignalActor implements SGActor<SignalEntity, Signal> {

    private final SignalDAO signalDAO;
    private final TransactionLender transactionLender;

    @Override
    public SignalEntity unWrap(Signal dto) {
        if (Objects.nonNull(dto))
            return SignalEntity.builder().id(dto.getId()).name(dto.getName()).signalDataType(dto.getSignalDataType())
                    .signalDefinition(dto.getSignalDefinition()).baseEntity(dto.getSignalBaseEntity()).build();
        return null;
    }

    @Override
    public Signal wrap(SignalEntity entity) {
        if (Objects.nonNull(entity))
            return Signal.builder().id(entity.getId()).name(entity.getName()).signalDataType(entity.getSignalDataType())
                    .signalDefinition(entity.getSignalDefinition()).signalBaseEntity(entity.getBaseEntity()).build();
        return null;
    }


    public Signal getSignal(Long signalId) {
        Optional<Signal> signalOptional = findSignal(signalId);
        if (!signalOptional.isPresent()) {
            throw new EntityNotFoundException(Signal.class.getSimpleName(), "Invalid signal : " + signalId);
        }
        return signalOptional.get();
    }

    public SignalEntity persist(SignalEntity signalEntity) {
        return signalDAO.persist(signalEntity);
    }

    public SignalEntity persistIfMissing(SignalEntity signalEntity) {
        SignalEntity oldSignalEntity = signalDAO.getSignal(signalEntity.getName(), signalEntity.getSignalDataType(),
                signalEntity.getSignalDefinition(), signalEntity.getBaseEntity());
        if (oldSignalEntity != null && oldSignalEntity.getName().equals(signalEntity.getName())) {
            return oldSignalEntity;
        }
        return signalDAO.persist(signalEntity);
    }

    private Optional<Signal> findSignal(Long signalId) {
        SignalEntity signalEntity = getSignalEntity(signalId);
        if (signalEntity == null) {
            return Optional.empty();
        }
        return of(wrap(signalEntity));
    }

    public void deleteSignals(List<String> signalNames) {
        if (signalNames.size() > 0) {
            signalDAO.deleteSignals(signalNames);
        }
    }

    private SignalEntity getSignalEntity(Long signalId) {
        AtomicReference<SignalEntity> signalAtomicReference = new AtomicReference<>(null);

        transactionLender.executeReadOnly(new WorkUnit() {
            @Override
            public void actualWork() {
                signalAtomicReference.set(signalDAO.get(signalId));
            }
        });
        return signalAtomicReference.get();
    }

}
