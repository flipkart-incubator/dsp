package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.PipelineStepSGAuditEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;

public class PipelineStepSGAuditDAO extends AbstractDAO<PipelineStepSGAuditEntity> {

    @Inject
    public PipelineStepSGAuditDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public PipelineStepSGAuditEntity saveOrUpdate(PipelineStepSGAuditEntity pipelineStepSGAuditEntity) {
        PipelineStepSGAuditEntity pipelineStepSGAuditEntity1 = getPipelineStepSGAuditEntity(
                pipelineStepSGAuditEntity.getPipelineExecutionId(), pipelineStepSGAuditEntity.getPipelineStep(),
                pipelineStepSGAuditEntity.getRefreshId());

        if (pipelineStepSGAuditEntity1 != null) {
            pipelineStepSGAuditEntity1.setUpdatedAt(new Timestamp(System.currentTimeMillis()));
            pipelineStepSGAuditEntity1.setStatus(pipelineStepSGAuditEntity.getStatus());
            currentSession().clear();
            return persist(pipelineStepSGAuditEntity1);
        } else {
            return persist(pipelineStepSGAuditEntity);
        }
    }

    private PipelineStepSGAuditEntity getPipelineStepSGAuditEntity(String pipelineExecutionId, long pipelineStep, long refreshId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("refreshId").paramValues(refreshId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("pipelineStep").paramValues(pipelineStep).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("pipelineExecutionId").paramValues(pipelineExecutionId).predicateType(EQUAL).build());
        return createQuery(PipelineStepSGAuditEntity.class, sqlParams).uniqueResult();
    }

    public List<PipelineStepSGAuditEntity> getPipelineStepSGAudits(String workflowExecutionId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("workflowExecutionId").paramValues(workflowExecutionId).predicateType(EQUAL).build());
        return createQuery(PipelineStepSGAuditEntity.class, sqlParams).list();
    }

}
