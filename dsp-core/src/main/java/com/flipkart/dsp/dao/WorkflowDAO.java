package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.DataFrameEntity;
import com.flipkart.dsp.db.entities.WorkflowEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.hibernate.SessionFactory;

import javax.persistence.criteria.*;
import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;

/**
 */
@Singleton
public class WorkflowDAO extends AbstractDAO<WorkflowEntity> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    @Inject
    public WorkflowDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public WorkflowEntity save(WorkflowEntity workflowEntity) {
        return persist(workflowEntity);
    }

    public List<WorkflowEntity> getWorkflow(String workflowName, String workflowGroupName, boolean isProd, String version) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("isProd").paramValues(isProd).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("version").paramValues(version).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("name").paramValues(workflowName).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("workflowGroupName").paramValues(workflowGroupName).predicateType(EQUAL).build());
        return createQuery(WorkflowEntity.class, sqlParams).list();
    }

    public List<WorkflowEntity> getWorkFlowsBySubscriptionId(String subscriptionId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("subscriptionId").paramValues(subscriptionId).predicateType(EQUAL).build());
        return createQuery(WorkflowEntity.class, sqlParams).list();
    }

    /**
     * @param dataFrameId : dataFrameId
     * @return Long:  number of workflowEntity attached to a particular dataframe
     */
    public Long getWorkFlowCount(Long dataFrameId) {
        CriteriaBuilder criteriaBuilder = getSessionFactory().getCriteriaBuilder();
        CriteriaQuery<Long> criteriaQuery = criteriaBuilder.createQuery(Long.class);
        Root<WorkflowEntity> requestStepRoot = criteriaQuery.from(WorkflowEntity.class);
        Join<WorkflowEntity, DataFrameEntity> join = requestStepRoot.join("dataFrames", JoinType.INNER);
        Expression<Long> count = criteriaBuilder.count(requestStepRoot);
        criteriaQuery.select(count);
        criteriaQuery.where(join.get("id").in(dataFrameId));
        return getSessionFactory().getCurrentSession().createQuery(criteriaQuery).getSingleResult();
    }

    public List<String> getAllDistinctWorkFlowNames() {
        CriteriaBuilder criteriaBuilder = getSessionFactory().getCriteriaBuilder();
        CriteriaQuery<String> query = criteriaBuilder.createQuery(String.class);
        Root<WorkflowEntity> root = query.from(WorkflowEntity.class);
        query.select(root.get("name")).distinct(true);
        return getSessionFactory().getCurrentSession().createQuery(query).list();
    }
}
