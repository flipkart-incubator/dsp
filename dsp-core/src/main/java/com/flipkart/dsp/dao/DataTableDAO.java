package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.DataTableEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;
import static com.flipkart.dsp.models.sg.PredicateType.IN;

/**
 */
public class DataTableDAO extends AbstractDAO<DataTableEntity> {

    @Inject
    public DataTableDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public DataTableEntity getTable(String tableId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("id").paramValues(tableId).predicateType(EQUAL).build());
        return createQuery(DataTableEntity.class, sqlParams).uniqueResult();
    }

    public void deleteDataTable(List<String> dataTableIds) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("id").paramValues(dataTableIds).predicateType(IN).build());
        delete(DataTableEntity.class, sqlParams);
    }

}
