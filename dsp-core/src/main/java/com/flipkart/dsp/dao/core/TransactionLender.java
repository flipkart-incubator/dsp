package com.flipkart.dsp.dao.core;

import com.flipkart.dsp.exceptions.DSPCoreException;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.hibernate.SessionFactory;

/**
 * Use this class to create transactions.
 * This, along with WorkUnit is basically a helper
 * that initializes, closes / rollbacks the session after a work unit.
 *
 * Typical expectation of use would be to create an Anonymous class
 * subclassing WorkUnit and do work that need to be in a transaction there.
 */
@Singleton
public class TransactionLender {

    private final SessionFactory factory;

    @Inject
    public TransactionLender(SessionFactory factory) {
        this.factory = factory;
    }

    public void execute(WorkUnit unit) {
        unit.doWork(this.factory, false, "");
    }

    public void execute(WorkUnit unit, String errorMessage) throws DSPCoreException {
        unit.doWork(this.factory, false, errorMessage);
    }

    public void executeReadOnly(WorkUnit unit, String errorMessage) {
        unit.doWork(this.factory, true, errorMessage);
    }

    public void executeReadOnly(WorkUnit unit) {
        unit.doWork(this.factory, true, "");
    }

}
