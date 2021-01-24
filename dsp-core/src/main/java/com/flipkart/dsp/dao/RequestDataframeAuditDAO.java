package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.DataFrameAuditEntity;
import com.flipkart.dsp.db.entities.RequestDataframeAuditEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import org.hibernate.SessionFactory;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;

public class RequestDataframeAuditDAO extends AbstractDAO<RequestDataframeAuditEntity> {

    @Inject
    public RequestDataframeAuditDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public List<DataFrameAuditEntity> getDataFrameAudit(Long requestId, Long workflowId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("requestEntity.id").paramValues(requestId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("workflowEntity.id").paramValues(workflowId).predicateType(EQUAL).build());
        return createQuery(RequestDataframeAuditEntity.class, sqlParams).list().stream()
                .map(RequestDataframeAuditEntity::getDataFrameAuditEntity).collect(Collectors.toList());
    }
}
