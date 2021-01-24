package com.flipkart.dsp.dao.core;


import com.flipkart.dsp.exceptions.DSPCoreException;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.*;
import org.hibernate.context.internal.ManagedSessionContext;

import static java.util.Objects.isNull;

/**
 * Subclass it and override the #actualWork method to do the required work within a transaction.
 * <p>
 * To change this template use File | Settings | File Templates.
 */
@Slf4j
public abstract class WorkUnit {

    final void doWork(SessionFactory sessionFactory, boolean readOnly, String errorMessage) throws DSPCoreException {
        Session session;
        if (hasOpenSession(sessionFactory)) {
            session = sessionFactory.getCurrentSession();
        } else {
            session = sessionFactory.openSession();
            ManagedSessionContext.bind(session);

            if (readOnly) {
                session.setDefaultReadOnly(true);
                session.setHibernateFlushMode(FlushMode.MANUAL);
            }

        }

        doActualWork(session, readOnly, errorMessage);
    }

    /**
     * Note: we support only ManagedSessionContext. So we need to take care of opening a session if not already opened
     */
    private boolean hasOpenSession(SessionFactory sessionFactory) {
        try {
            return !isNull(sessionFactory.getCurrentSession());
        } catch (HibernateException exception) {
            return false;
        }
    }

    private void doActualWork(Session session, boolean readOnlyFlag, String errorMessage) {
        Transaction txn = session.getTransaction();
        boolean amSessionInitiator = !txn.isActive();
        if (amSessionInitiator) {
            txn.begin();
        }

        try {
            actualWork();
            if (amSessionInitiator && !readOnlyFlag)
                session.getTransaction().commit();
        } catch (Exception e) {
            e.printStackTrace();
            session.getTransaction().rollback();
            log.error("Transaction failed with following exception: {}", e.getMessage());
            throw new DSPCoreException(errorMessage + ". Transaction failed with following exception: " + e.getMessage(), e);
        } finally {
            if (amSessionInitiator) {
                session.close();
                ManagedSessionContext.unbind(session.getSessionFactory());
            }

        }
    }

    public abstract void actualWork() throws Exception;
}
