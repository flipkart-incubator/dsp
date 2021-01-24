package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.EventAuditsDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.EventAuditEntity;
import com.flipkart.dsp.models.EventLevel;
import com.flipkart.dsp.models.EventType;
import com.flipkart.dsp.models.event_audits.EventAudit;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class EventAuditActor implements SGActor<EventAuditEntity, EventAudit> {
    private final EventAuditsDAO eventAuditsDAO;
    private final TransactionLender transactionLender;

    @Override
    public EventAuditEntity unWrap(EventAudit eventAudit) {
        if (Objects.nonNull(eventAudit)) {
            return EventAuditEntity.builder().eventLevel(eventAudit.getEventLevel())
                    .eventType(eventAudit.getEventType()).workflowId(eventAudit.getWorkflowId())
                    .requestId(eventAudit.getRequestId()).payload(eventAudit.getPayload()).build();
        }
        return null;
    }

    @Override
    public EventAudit wrap(EventAuditEntity eventAuditEntity) {
        if (Objects.nonNull(eventAuditEntity)) {
            return EventAudit.builder().id(eventAuditEntity.getId()).requestId(eventAuditEntity.getRequestId())
                    .workflowId(eventAuditEntity.getWorkflowId()).eventLevel(eventAuditEntity.getEventLevel())
                    .eventType(eventAuditEntity.getEventType()).payload(eventAuditEntity.getPayload()).build();
        }
        return null;
    }

    /**
     * @param requestId
     * @param id        - this behaves as offset and will return Events from that offset
     * @return List<EventAudit> : List can be empty if either requestId is wrong or when requestEntity has'nt started
     */
    public List<EventAudit> getEvents(Long id, Long requestId, List<EventLevel> eventLevels) {
        final AtomicReference<List<EventAuditEntity>> listAtomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                listAtomicReference.set(eventAuditsDAO.getEvents(id, requestId, null, eventLevels));
            }
        });
        return listAtomicReference.get().stream().map(this::wrap).collect(Collectors.toList());
    }

    public void saveEvent(EventAudit eventAudit) {
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                eventAuditsDAO.persist(unWrap(eventAudit));
            }
        });
    }

    public List<EventAudit> getEvents(Long requestId, EventLevel eventLevel, EventType eventType) {
        final AtomicReference<List<EventAuditEntity>> listAtomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                listAtomicReference.set(eventAuditsDAO.getEvents(requestId, eventLevel, eventType));
            }
        });
        return listAtomicReference.get().stream().map(this::wrap).collect(Collectors.toList());
    }

}
