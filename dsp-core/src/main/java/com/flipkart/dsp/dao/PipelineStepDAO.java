package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.PipelineStepEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;
import static com.flipkart.dsp.models.sg.PredicateType.IN;

/**
 */
@Singleton
public class PipelineStepDAO extends AbstractDAO<PipelineStepEntity> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    @Inject
    public PipelineStepDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }


    public List<PipelineStepEntity> getPipelineStepsByWorkflowId(Long workflowId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("workflowId").paramValues(workflowId).predicateType(EQUAL).build());
        return createQuery(PipelineStepEntity.class, sqlParams).list();
    }

    public PipelineStepEntity getPipelineStepById(Long id) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("id").paramValues(id).predicateType(EQUAL).build());
        return createQuery(PipelineStepEntity.class, sqlParams).uniqueResult();
    }

    public List<PipelineStepEntity> getPipelineSteps(List<Long> pipelineStepIds) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("id").paramValues(pipelineStepIds).predicateType(IN).build());
        return createQuery(PipelineStepEntity.class, sqlParams).list();
    }
}
