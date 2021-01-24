package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.ExecutionEnvironmentEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;

/**
 */
public class ExecutionEnvironmentDAO extends AbstractDAO<ExecutionEnvironmentEntity> {

    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    @Inject
    public ExecutionEnvironmentDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public ExecutionEnvironmentEntity getEnvironmentEnvironment(String executionEnvironment) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("executionEnvironment").paramValues(executionEnvironment).predicateType(EQUAL).build());
        return createQuery(ExecutionEnvironmentEntity.class, sqlParams).uniqueResult();
    }

    public List<ExecutionEnvironmentEntity> getAllEnvironments() {
        return createQuery(ExecutionEnvironmentEntity.class, new ArrayList<>()).list();
    }
}
