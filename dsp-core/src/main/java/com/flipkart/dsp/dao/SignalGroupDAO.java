package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.SignalGroupEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.flipkart.dsp.models.sg.PredicateType;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class SignalGroupDAO extends AbstractDAO<SignalGroupEntity> {

    @Inject
    public SignalGroupDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public void deleteSignalGroup(List<String> signalGroupLists) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("id").paramValues(signalGroupLists).predicateType(PredicateType.IN).build());
        delete(SignalGroupEntity.class, sqlParams);
    }
}
