package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.DataFrameOverrideAuditEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideState;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideType;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.flipkart.dsp.models.sg.PredicateType.*;

/**
 */
public class DataFrameOverrideAuditDAO extends AbstractDAO<DataFrameOverrideAuditEntity> {

    @Inject
    public DataFrameOverrideAuditDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public DataFrameOverrideAuditEntity getDataFrameOverrideAudit(Long requestId, String inputDataId, Long dataframeId, DataFrameOverrideType dataFrameOverrideType) {
        List<DataFrameOverrideState> dataFrameOverrideStates = new ArrayList<>();
        dataFrameOverrideStates.add(DataFrameOverrideState.STARTED);
        dataFrameOverrideStates.add(DataFrameOverrideState.SUCCEDED);
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("isDeleted").paramValues(false).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("expiresAt").paramValues(LocalDateTime.now()).predicateType(LOCAL_DATE_TIME_GREATER_THAN).build());
        sqlParams.add(SqlParam.builder().paramName("requestId").paramValues(requestId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("inputDataId").paramValues(inputDataId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("dataframeId").paramValues(dataframeId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("state").paramValues(dataFrameOverrideStates).predicateType(IN).build());
        sqlParams.add(SqlParam.builder().paramName("dataFrameOverrideType").paramValues(dataFrameOverrideType).predicateType(EQUAL).build());

        Map<String, String> orderByMap = new HashMap<>();
        orderByMap.put("createdAt", "desc");
        orderByMap.put("state", "desc");
        return createQuery(DataFrameOverrideAuditEntity.class, sqlParams, orderByMap).setMaxResults(1).uniqueResult();
    }

    public List<DataFrameOverrideAuditEntity> getByRequestId(Long requestId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("requestId").paramValues(requestId).predicateType(EQUAL).build());
        return createQuery(DataFrameOverrideAuditEntity.class, sqlParams).list();
    }

    public List<DataFrameOverrideAuditEntity> getStartedByRequestId(Long requestId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("requestId").paramValues(requestId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("isDeleted").paramValues(false).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("expiresAt").paramValues(LocalDateTime.now()).predicateType(LOCAL_DATE_TIME_GREATER_THAN).build());
        sqlParams.add(SqlParam.builder().paramName("state").paramValues(DataFrameOverrideState.STARTED).predicateType(EQUAL).build());
        return createQuery(DataFrameOverrideAuditEntity.class, sqlParams).list();
    }

    public DataFrameOverrideAuditEntity get(Long requestId, Long dataframeId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("requestId").paramValues(requestId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("dataframeId").paramValues(dataframeId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("state").paramValues(DataFrameOverrideState.SUCCEDED).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("isDeleted").paramValues(false).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("expiresAt").paramValues(LocalDateTime.now()).predicateType(LOCAL_DATE_TIME_GREATER_THAN).build());
        Map<String, String> orderByMap = new HashMap<>();
        orderByMap.put("createdAt", "desc");
        return createQuery(DataFrameOverrideAuditEntity.class, sqlParams, orderByMap).setMaxResults(1).getSingleResult();
    }
}
