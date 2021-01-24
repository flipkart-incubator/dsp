package com.flipkart.dsp.dao;


import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.SignalGroupToSignalEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.flipkart.dsp.models.sg.PredicateType;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.List;

public class SignalGroupToSignalDAO extends AbstractDAO<SignalGroupToSignalEntity> {
    @Inject
    public SignalGroupToSignalDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public List<SignalGroupToSignalEntity> getAll() {
        return createQuery(SignalGroupToSignalEntity.class, new ArrayList<>()).list();
    }

    public Long getSignalCount(String signalName) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("signal.name").paramValues(signalName).predicateType(PredicateType.EQUAL).build());
        return getCount(SignalGroupToSignalEntity.class, sqlParams);
    }

    public Long getDataTableCount(String datatableName) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("dataTableEntity.id").paramValues(datatableName).predicateType(PredicateType.EQUAL).build());
        return getCount(SignalGroupToSignalEntity.class, sqlParams);
    }

    public void deleteSignalGroup(List<String> signalGroupList) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("signalGroup.id").paramValues(signalGroupList).predicateType(PredicateType.IN).build());
        delete(SignalGroupToSignalEntity.class, sqlParams);
    }

}
