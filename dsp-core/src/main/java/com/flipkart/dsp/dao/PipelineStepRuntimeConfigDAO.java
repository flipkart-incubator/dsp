package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.PipelineStepRuntimeConfigEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;

/**
 */
@Singleton
public class PipelineStepRuntimeConfigDAO extends AbstractDAO<PipelineStepRuntimeConfigEntity> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    @Inject
    public PipelineStepRuntimeConfigDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public PipelineStepRuntimeConfigEntity get(String pipelineExecutionId, Long pipelineStepId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("pipelineStepEntity.id").paramValues(pipelineStepId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("pipelineExecutionId").paramValues(pipelineExecutionId).predicateType(EQUAL).build());
        return createQuery(PipelineStepRuntimeConfigEntity.class, sqlParams).uniqueResult();
    }

    public List<PipelineStepRuntimeConfigEntity> getByWorkflowExecutionId(String workflowExecutionId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("workflowExecutionId").paramValues(workflowExecutionId).predicateType(EQUAL).build());
        return createQuery(PipelineStepRuntimeConfigEntity.class, sqlParams).list();
    }

    public List<PipelineStepRuntimeConfigEntity> getByWorkflowExecutionIdAndScope(String workflowExecutionId, String scope) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("workflowExecutionId").paramValues(workflowExecutionId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("scope").paramValues(scope).predicateType(EQUAL).build());
        return createQuery(PipelineStepRuntimeConfigEntity.class, sqlParams).list();
    }
}
