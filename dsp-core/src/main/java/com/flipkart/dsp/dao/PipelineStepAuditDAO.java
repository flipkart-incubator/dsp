package com.flipkart.dsp.dao;

import com.flipkart.dsp.dao.core.AbstractDAO;
import com.flipkart.dsp.db.entities.PipelineStepAuditEntity;
import com.flipkart.dsp.entities.misc.SqlParam;
import com.flipkart.dsp.models.PipelineStepStatus;
import com.flipkart.dsp.utils.JsonUtils;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.SessionFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static com.flipkart.dsp.models.sg.PredicateType.EQUAL;
import static com.flipkart.dsp.models.sg.PredicateType.IN;

/**
 */
public class PipelineStepAuditDAO extends AbstractDAO<PipelineStepAuditEntity> {

    @Inject
    public PipelineStepAuditDAO(SessionFactory sessionFactory) {
        super(sessionFactory);
    }

    public List<PipelineStepAuditEntity> getPipelineStepAuditsByWorkflowExecutionId(String workflowExecutionId) {
        return getPipelineStepAudits(null, null, null, workflowExecutionId, null, new ArrayList<>());
    }

    public List<PipelineStepAuditEntity> getPipelineStepAudits(Integer attempt, Long refreshId, Long pipelineStepId,
                                                               String pipelineExecutionId, String workflowExecutionId) {
        return getPipelineStepAudits(attempt, refreshId, pipelineStepId, workflowExecutionId, pipelineExecutionId, new ArrayList<>());
    }

    private List<PipelineStepAuditEntity> getPipelineStepAudits(Integer attempt, Long refreshId, Long pipelineStepId, String workflowExecutionId,
                                                                String pipelineExecutionId, List<String> statusList) {
        List<SqlParam> sqlParams = new ArrayList<>();
        sqlParams.add(SqlParam.builder().paramName("pipelineStepStatus").paramValues(statusList).predicateType(IN).build());
        sqlParams.add(SqlParam.builder().paramName("attempt").paramValues(attempt).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("refreshId").paramValues(refreshId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("pipelineStepId").paramValues(pipelineStepId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("pipelineExecutionId").paramValues(pipelineExecutionId).predicateType(EQUAL).build());
        sqlParams.add(SqlParam.builder().paramName("workflowExecutionId").paramValues(workflowExecutionId).predicateType(EQUAL).build());
        return createQuery(PipelineStepAuditEntity.class, sqlParams).list();
    }

    public PipelineStepAuditEntity save(PipelineStepAuditEntity pipelineStepAuditEntity) {
        List<PipelineStepAuditEntity> pipelineStepAudits = getPipelineStepAudits(pipelineStepAuditEntity.getAttempt(),
                pipelineStepAuditEntity.getRefreshId(), pipelineStepAuditEntity.getPipelineStepId(), pipelineStepAuditEntity.getWorkflowExecutionId(),
                pipelineStepAuditEntity.getPipelineExecutionId(), new ArrayList<>());
        if (pipelineStepAudits.size() != 0) {
            PipelineStepAuditEntity pipelineStepAuditEntityInternal = pipelineStepAudits.get(0);
            if (PipelineStepStatus.SUCCESS == pipelineStepAuditEntityInternal.getPipelineStepStatus())
                return pipelineStepAuditEntityInternal;
            else {
                pipelineStepAuditEntityInternal.setPipelineStepStatus(pipelineStepAuditEntity.getPipelineStepStatus());
                if (pipelineStepAuditEntity.getResources() != null && !pipelineStepAuditEntity.getResources().isEmpty())
                    pipelineStepAuditEntityInternal.setResources(pipelineStepAuditEntity.getResources());
                if (pipelineStepAuditEntity.getLogs() != null && !pipelineStepAuditEntity.getLogs().isEmpty())
                    pipelineStepAuditEntityInternal.setLogs(pipelineStepAuditEntity.getLogs());
                pipelineStepAuditEntityInternal.setUpdatedAt(new Timestamp(System.currentTimeMillis()));
                return persist(pipelineStepAuditEntityInternal);
            }
        } else {
            return persist(pipelineStepAuditEntity);
        }
    }

}
