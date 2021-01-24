package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.SignalEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.flipkart.dsp.models.sg.PredicateType;
import com.flipkart.dsp.models.sg.SignalDataType;
import com.flipkart.dsp.models.sg.SignalDefinition;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.hibernate.Criteria;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;

import javax.persistence.EntityNotFoundException;
import java.util.*;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;
import static java.util.stream.Collectors.toSet;

/**
 */

public class SignalDAO extends AbstractDAO<SignalEntity> {

    @Inject
    public SignalDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public SignalEntity getSignal(String signalName, SignalDataType signalDataType, SignalDefinition signalDefinition, String baseEntity) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("name").paramValues(signalName).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("baseEntity").paramValues(baseEntity).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("signalDataType").paramValues(signalDataType).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("signalDefinition").paramValues(signalDefinition).predicateType(EQUAL).build());
        return createQuery(SignalEntity.class, sqlParams).setMaxResults(1).list().stream().findFirst().orElse(null);
    }

    public void deleteSignals(List<String> signalNames) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("name").paramValues(signalNames).predicateType(PredicateType.IN).build());
        delete(SignalEntity.class, sqlParams);
    }
}
