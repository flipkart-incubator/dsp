package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.ExecutionEnvironmentSnapshotEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;

/**
 */
public class ExecutionEnvironmentSnapshotDAO extends AbstractDAO<ExecutionEnvironmentSnapshotEntity> {

    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    @Inject
    public ExecutionEnvironmentSnapshotDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public List<ExecutionEnvironmentSnapshotEntity> getExecutionEnvironmentSnapshots(Long executionEnvironmentId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("executionEnvironmentId").paramValues(executionEnvironmentId).predicateType(EQUAL).build());
        return createQuery(ExecutionEnvironmentSnapshotEntity.class, sqlParams).list();
    }
}
