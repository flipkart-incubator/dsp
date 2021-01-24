package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.ExternalHealthCheckAuditEntity;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

/**
 * +
 */
public class ExternalHealthCheckAuditDao extends AbstractDAO<ExternalHealthCheckAuditEntity> {
    @Inject
    public ExternalHealthCheckAuditDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }
}
