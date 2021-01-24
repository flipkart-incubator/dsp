package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.DataFrameAuditEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.flipkart.dsp.entities.sg.core.DataFrameAuditStatus;
import com.google.inject.Inject;
import org.apache.hadoop.util.Time;
import org.hibernate.SessionFactory;

import java.time.LocalDateTime;
import java.util.*;

import static com.flipkart.dsp.models.sg.PredicateType.*;
import static java.util.Optional.ofNullable;

/**
 */

public class DataFrameAuditDAO extends AbstractDAO<DataFrameAuditEntity> {

    @Inject
    public DataFrameAuditDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }


    public List<DataFrameAuditEntity> getDataFrameAudits(List<Long> dataFrameIds, DataFrameAuditStatus status) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("dataFrameEntity.id").paramValues(dataFrameIds).predicateType(IN).build());
        sqlParams.add(SqlParam.builder().paramName("status").paramValues(status).predicateType(EQUAL).build());
        return createQuery(DataFrameAuditEntity.class, sqlParams).getResultList();
    }

    public DataFrameAuditEntity getDataFrameAudit(Long overrideAuditId, Long dataframeId, String partitions) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("partitions").paramValues(partitions).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("dataFrameEntity.id").paramValues(dataframeId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("overrideAuditId").paramValues(overrideAuditId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("status").paramValues(DataFrameAuditStatus.COMPLETED).predicateType(EQUAL).build());
        Map<String, String> orderByMap = new HashMap<>();
        orderByMap.put("updatedAt", "desc");
        List<DataFrameAuditEntity> dataFrameAuditEntities =  createQuery(DataFrameAuditEntity.class, sqlParams, orderByMap).setMaxResults(1).list();
        return dataFrameAuditEntities.size() == 0 ? null : dataFrameAuditEntities.get(0);
    }

    public void updateLogId(List<DataFrameAuditEntity> dataFrameAuditEntities, Long logAuditId) {
        dataFrameAuditEntities.forEach(entity -> entity.setLogAuditId(logAuditId));
        persistCollection(dataFrameAuditEntities);
    }

    public Optional<DataFrameAuditEntity> getLatestDataFrameAudit(Long dataFrameId, DataFrameAuditStatus status) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("dataFrameEntity.id").paramValues(dataFrameId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("status").paramValues(status).predicateType(EQUAL).build());
        Map<String, String> orderMap = new HashMap<>();
        orderMap.put("runId", "desc");
        return ofNullable(createQuery(DataFrameAuditEntity.class, sqlParams, orderMap).setMaxResults(1).uniqueResult());
    }

    public List<DataFrameAuditEntity> getLatestSuccessfulDataFrameAudits(String dataFrameId, Integer noOfRuns) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("dataFrameEntity.id").paramValues(dataFrameId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("status").paramValues(DataFrameAuditStatus.COMPLETED).predicateType(EQUAL).build());
        Map<String, String> orderMap = new HashMap<>();
        orderMap.put("runId", "desc");
        return createQuery(DataFrameAuditEntity.class, sqlParams, orderMap).setMaxResults(noOfRuns).list();
    }
}
