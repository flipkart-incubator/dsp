package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.RequestStepAuditEntity;
import com.flipkart.dsp.db.entities.RequestStepEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;

/**
 */
public class RequestStepDAO extends AbstractDAO<RequestStepEntity> {
    private static int INDEX_ZERO = 0;

    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    @Inject
    public RequestStepDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public List<RequestStepEntity> getAllStepIdsForRequest(Long requestId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("requestEntity.id").paramValues(requestId).predicateType(EQUAL).build());
        return createQuery(RequestStepEntity.class, sqlParams).list();
    }

    public RequestStepEntity getLatestRequestStepByJobName(Long requestId, String jobName) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("requestEntity.id").paramValues(requestId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("jobName").paramValues(jobName).predicateType(EQUAL).build());
        Map<String, String> orderByMap = new HashMap<>();
        orderByMap.put("id", "desc");
        return createQuery(RequestStepEntity.class, sqlParams, orderByMap).setMaxResults(1).list().stream().findFirst().orElse(null);
    }

}
