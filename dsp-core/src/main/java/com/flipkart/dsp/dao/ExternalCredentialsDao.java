package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.ExternalCredentialsEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;

/**
 * +
 */
public class ExternalCredentialsDao extends AbstractDAO<ExternalCredentialsEntity> {

    @Inject
    public ExternalCredentialsDao(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public List<ExternalCredentialsEntity> getCredentialsByClientAlias(String clientAlias) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("clientAlias").paramValues(clientAlias).predicateType(EQUAL).build());
        return createQuery(ExternalCredentialsEntity.class, sqlParams).list();
    }
}
