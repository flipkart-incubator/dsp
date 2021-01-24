package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.RequestStepAuditEntity;
import com.flipkart.dsp.entities.enums.RequestStepAuditStatus;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;
import static com.flipkart.dsp.models.sg.PredicateType.IN;

/**
 */
public class RequestStepAuditDAO extends AbstractDAO<RequestStepAuditEntity> {
    private static int INDEX_ZERO = 0;

    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    @Inject
    public RequestStepAuditDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public List<RequestStepAuditEntity> getAllAuditsForRequestStep(Long requestStepId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("requestStepEntity.id").paramValues(requestStepId).predicateType(EQUAL).build());
        Map<String, String> orderByMap = new HashMap<>();
        orderByMap.put("updatedAt", "desc");
        return createQuery(RequestStepAuditEntity.class, sqlParams, orderByMap).list();
    }

    public RequestStepAuditEntity getLatestSuccessfulAuditForRequestStep(long requestStepId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("requestStepEntity.id").paramValues(requestStepId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("status").paramValues(RequestStepAuditStatus.SUCCESSFUL).predicateType(EQUAL).build());
        Map<String, String> orderByMap = new HashMap<>();
        orderByMap.put("id", "desc");
        return createQuery(RequestStepAuditEntity.class, sqlParams, orderByMap).setMaxResults(1).list().stream().findFirst().orElse(null);
    }

    public List<RequestStepAuditEntity> getAllAuditsForRequest(List<Long> requestStepIds) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("requestStepEntity.id").paramValues(requestStepIds).predicateType(IN).build());
        return createQuery(RequestStepAuditEntity.class, sqlParams).list();
    }
}
