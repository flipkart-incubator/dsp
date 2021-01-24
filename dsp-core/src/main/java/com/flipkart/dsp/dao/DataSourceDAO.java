package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.DataSourceEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.flipkart.dsp.models.sg.PredicateType;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class DataSourceDAO extends AbstractDAO<DataSourceEntity> {

    @Inject
    public DataSourceDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public DataSourceEntity getDataSource(String dataSourceId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("id").paramValues(dataSourceId).predicateType(PredicateType.EQUAL).build());
        return createQuery(DataSourceEntity.class, sqlParams).uniqueResult();
    }

    public void flushSession() {
        getSessionFactory().getCurrentSession().flush();
    }

    public void clearSession() {
        getSessionFactory().getCurrentSession().clear();
    }


}
