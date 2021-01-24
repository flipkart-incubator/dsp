package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.WorkflowAuditEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.flipkart.dsp.models.WorkflowStatus;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.hibernate.SessionFactory;

import java.util.*;

import static com.flipkart.dsp.models.WorkflowStatus.FAILED;
import static com.flipkart.dsp.models.WorkflowStatus.SUCCESS;
import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;
import static com.flipkart.dsp.models.sg.PredicateType.NOT_IN;

/**
 */
@Singleton
public class WorkflowAuditDAO extends AbstractDAO<WorkflowAuditEntity> {
    /**
     * Creates a new DAO with a given session provider.
     *
     * @param sessionFactory a session provider
     */
    @Inject

    public WorkflowAuditDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public long getNumOfRunningWorkflowAudits(String workflowExecutionId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("workflowExecutionId").paramValues(workflowExecutionId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("workflowStatus").paramValues(Arrays.asList(FAILED, SUCCESS)).predicateType(NOT_IN).build());
        return getCount(WorkflowAuditEntity.class, sqlParams);
    }

    public long getNumOfRunningWorkflowAudits(Long refreshId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("refreshId").paramValues(refreshId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("workflowStatus").paramValues(Arrays.asList(FAILED, SUCCESS)).predicateType(NOT_IN).build());
        return getCount(WorkflowAuditEntity.class, sqlParams);
    }

    private List<WorkflowAuditEntity> getOrderedWorkflowAudits(Long refreshId, Long workflowId, String workflowExecutionId) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("refreshId").paramValues(refreshId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("workflowEntity.id").paramValues(workflowId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("workflowExecutionId").paramValues(workflowExecutionId).predicateType(EQUAL).build());
        Map<String, String> orderByMap = new HashMap<>();
        orderByMap.put("id", "desc");
        return createQuery(WorkflowAuditEntity.class, sqlParams, orderByMap).list();
    }


    public List<WorkflowAuditEntity> getWorkflowAuditsByWorkflowExecId(String workflowExecutionId) {
        return getOrderedWorkflowAudits(null, null, workflowExecutionId);
    }

    public List<WorkflowAuditEntity> getWorkflowAuditsByRequestId(Long refreshId) {
        return getOrderedWorkflowAudits(refreshId, null, null);
    }


    public WorkflowAuditEntity getWorkflowAudit(Long refreshId, Long workflowId, String workflowExecutionId) {
        return getOrderedWorkflowAudits(refreshId, workflowId, workflowExecutionId).stream().findFirst().orElse(null);
    }

    public WorkflowAuditEntity getWorkflowAuditByWorkflowExecutionId(String workflowExecutionId) {
        return getWorkflowAudit(null, null, workflowExecutionId);
    }

    public WorkflowAuditEntity getLatestWorkflowAudit(Long refreshId, Long workflowId) {
        return getWorkflowAudit(refreshId, workflowId, null);
    }

    public String getLatestWorkflowExecutionId(Long workflowId, Long refreshId) {
        List<WorkflowAuditEntity> entities = getOrderedWorkflowAudits(refreshId, workflowId, null);
        return entities.size() > 0 ? entities.get(0).getWorkflowExecutionId() : null;
    }

}
