package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.UserEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.flipkart.dsp.models.sg.PredicateType;
import com.google.inject.Inject;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.List;

public class UserDAO extends AbstractDAO<UserEntity> {

    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    @Inject
    public UserDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public UserEntity getUserByName(String userName) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("userId").paramValues(userName).predicateType(PredicateType.EQUAL).build());
        return createQuery(UserEntity.class, sqlParams).uniqueResult();
    }
}
