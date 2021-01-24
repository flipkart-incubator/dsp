package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.SubscriptionAuditEntity;
import org.hibernate.SessionFactory;

import javax.inject.Inject;

public class SubscriptionAuditDAO extends AbstractDAO<SubscriptionAuditEntity> {

    @Inject
    public SubscriptionAuditDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }
}
