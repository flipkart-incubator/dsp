package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.NotificationPreferencesEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;

public class NotificationPreferenceDAO extends AbstractDAO<NotificationPreferencesEntity> {

    @Inject
    public NotificationPreferenceDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public NotificationPreferencesEntity getNotificationPreference(Long workflowId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("workflowEntity.id").paramValues(workflowId).predicateType(EQUAL).build());
        return createQuery(NotificationPreferencesEntity.class, sqlParams).uniqueResult();
    }

}
