package com.flipkart.dsp.dao;


import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.DataFrameEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;
import static com.flipkart.dsp.models.sg.PredicateType.IN;

/**
 */
public class DataFrameDAO extends AbstractDAO<DataFrameEntity> {

    @Inject
    public DataFrameDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public List<DataFrameEntity> getDataFramesByName(String dataframeName) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("name").paramValues(dataframeName).predicateType(EQUAL).build());
        return createQuery(DataFrameEntity.class, sqlParams).list();
    }

    public DataFrameEntity getDataFrame(Long id) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("id").paramValues(id).predicateType(EQUAL).build());
        return createQuery(DataFrameEntity.class, sqlParams).uniqueResult();
    }

    public Long getUsedDataFrameCount(String signalGroupId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("signalGroupEntity.id").paramValues(signalGroupId).predicateType(EQUAL).build());
        return getCount(DataFrameEntity.class, sqlParams);
    }

    public void deleteDataframe(List<Long> dataFrameIds) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("id").paramValues(dataFrameIds).predicateType(IN).build());
        delete(DataFrameEntity.class, sqlParams);
    }

    public List<DataFrameEntity> getAllDataFrames() {
        return createQuery(DataFrameEntity.class, new ArrayList<>()).list();
    }
}
