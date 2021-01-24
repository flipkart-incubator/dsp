package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.NotificationPreferenceDAO;
import com.flipkart.dsp.dao.WorkflowDAO;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.NotificationPreferencesEntity;
import com.flipkart.dsp.db.entities.WorkflowEntity;
import com.flipkart.dsp.entities.misc.NotificationPreference;
import com.flipkart.dsp.entities.workflow.Workflow;
import com.flipkart.dsp.models.misc.EmailNotifications;
import com.google.inject.Inject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * +
 */
@Slf4j
@RequiredArgsConstructor(onConstructor = @__(@Inject))
public class NotificationPreferencesActor implements SGActor<NotificationPreferencesEntity, NotificationPreference> {
    private final TransactionLender transactionLender;
    private final NotificationPreferenceDAO notificationPreferenceDAO;

    @Override
    public NotificationPreferencesEntity unWrap(NotificationPreference notificationPreference) {
        return null;
    }

    @Override
    public NotificationPreference wrap(NotificationPreferencesEntity notificationPreferencesEntity) {
        if (Objects.nonNull(notificationPreferencesEntity)) {
            return NotificationPreference.builder().id(notificationPreferencesEntity.getId())
                    .workflowId(notificationPreferencesEntity.getWorkflowEntity().getId())
                    .emailNotificationPreferences(notificationPreferencesEntity.getEmailNotificationPreferences()).build();
        }
        return null;
    }

    public NotificationPreference getNotificationPreference(Long workflowId) {
        final AtomicReference<NotificationPreference> atomicReference = new AtomicReference<>(null);
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                atomicReference.set(wrap(notificationPreferenceDAO.getNotificationPreference(workflowId)));
            }
        });
        return atomicReference.get();
    }


    public void createNotificationPreferences(WorkflowEntity workflow, EmailNotifications emailNotifications) {
        if (Objects.nonNull(emailNotifications)) {
            AtomicReference<NotificationPreference> notificationPreferenceAtomicReference = new AtomicReference<>(null);
            transactionLender.execute(new WorkUnit() {
                @Override
                public void actualWork() {
                    NotificationPreferencesEntity notificationPreference = NotificationPreferencesEntity.builder()
                            .workflowEntity(workflow).emailNotificationPreferences(emailNotifications).build();
                    notificationPreferenceAtomicReference.set(wrap(notificationPreferenceDAO.persist(notificationPreference)));
                }
            });
            log.info("notificationPreference created with Id: {}, for workflowId: {}", notificationPreferenceAtomicReference.get().getId(), workflow.getId());
        }
    }
}
