package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;

@Data
@Entity
@Builder
@DynamicUpdate
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "pipeline_step_sg_audits", uniqueConstraints = @UniqueConstraint(columnNames = {"pipeline_execution_id", "pipeline_step_id"}))
public class PipelineStepSGAuditEntity extends BaseEntity implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "workflow_execution_id", nullable = false)
    private String workflowExecutionId;

    @Column(name = "pipeline_execution_id", nullable = false)
    private String pipelineExecutionId;

    @Column(name = "pipeline_step_id", nullable = false)
    private Long pipelineStep;

    @Column(name = "status")
    private String status;

    @Column(name = "refresh_id", nullable = false)
    private Long refreshId;

    @Column(name = "logs")
    private String logs;
}
