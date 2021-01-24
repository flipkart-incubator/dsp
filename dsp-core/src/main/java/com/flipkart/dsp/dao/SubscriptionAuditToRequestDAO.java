package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.SubscriptionAuditToRequestEntity;
import org.hibernate.SessionFactory;

import javax.inject.Inject;

public class SubscriptionAuditToRequestDAO extends AbstractDAO<SubscriptionAuditToRequestEntity> {

    @Inject
    public SubscriptionAuditToRequestDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }
}
