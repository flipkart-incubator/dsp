package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import com.flipkart.dsp.models.PipelineStepStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;

/**
 */
@Data
@Entity
@Builder
@DynamicUpdate
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "pipeline_step_audits", uniqueConstraints = @UniqueConstraint(columnNames = {"pipeline_step_id", "attempt", "pipeline_execution_id", "workflow_execution_id"}))
public class PipelineStepAuditEntity extends BaseEntity implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "workflow_execution_id", nullable = false)
    private String workflowExecutionId;

    @Column(name = "workflow_id", nullable = false)
    private Long workflowId;

    @Column(name = "pipeline_execution_id", nullable = false)
    private String pipelineExecutionId;

    @Column(name = "pipeline_step_id", nullable = false)
    private Long pipelineStepId;

    @Column(name = "attempt", nullable = false)
    private Integer attempt;

    @Column(name = "status")
    @Enumerated(EnumType.STRING)
    private PipelineStepStatus pipelineStepStatus;

    @Column(name = "refresh_id", nullable = false)
    private Long refreshId;

    @Column(name = "resources")
    private String resources;

    @Column(name = "logs")
    private String logs;

    @Column(name = "scope")
    @Type(type = "text")
    private String scope;
}
