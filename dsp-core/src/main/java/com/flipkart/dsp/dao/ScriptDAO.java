package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.ScriptEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.hibernate.SessionFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;

/**
 */
@Singleton
public class ScriptDAO extends AbstractDAO<ScriptEntity> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    @Inject
    public ScriptDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public ScriptEntity getLatestScriptByGitDetails(String gitRepo, String gitFolder, String gitFilePath, String gitCommitId, boolean isProd) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("isDraft").paramValues(isProd).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("gitRepo").paramValues(gitRepo).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("gitFolder").paramValues(gitFolder).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("gitFilePath").paramValues(gitFilePath).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("gitCommitId").paramValues(gitCommitId).predicateType(EQUAL).build());
        Map<String, String> orderMap = new HashMap<>();
        orderMap.put("version", "desc");
        return createQuery(ScriptEntity.class, sqlParams, orderMap).setMaxResults(1).list().stream().findFirst().orElse(null);
    }

}
