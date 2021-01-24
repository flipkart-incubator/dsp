package com.flipkart.dsp.actors;

import com.flipkart.dsp.dao.ExternalHealthCheckAuditDao;
import com.flipkart.dsp.dao.core.TransactionLender;
import com.flipkart.dsp.dao.core.WorkUnit;
import com.flipkart.dsp.db.entities.ExternalHealthCheckAuditEntity;
import com.flipkart.dsp.entities.enums.ExternalHealthCheckStatus;
import com.flipkart.dsp.models.ExternalClient;

import javax.inject.Inject;
import java.sql.Timestamp;

/**
 * +
 */
public class ExternalHealthCheckAuditActor {
    private TransactionLender transactionLender;
    private ExternalHealthCheckAuditDao externalHealthCheckAuditDao;

    @Inject
    public ExternalHealthCheckAuditActor(TransactionLender transactionLender,
                                         ExternalHealthCheckAuditDao externalHealthCheckAuditDao) {
        this.transactionLender = transactionLender;
        this.externalHealthCheckAuditDao = externalHealthCheckAuditDao;
    }

    public void createExternalHealthCheckAudit(ExternalHealthCheckStatus status, ExternalClient externalClient) {
        transactionLender.execute(new WorkUnit() {
            @Override
            public void actualWork() {
                ExternalHealthCheckAuditEntity externalHealthCheckAuditEntity = ExternalHealthCheckAuditEntity.builder()
                        .status(status).externalClient(externalClient)
                        .createdAt(new Timestamp(System.currentTimeMillis())).build();
                externalHealthCheckAuditDao.persist(externalHealthCheckAuditEntity);
            }
        });
    }


}
