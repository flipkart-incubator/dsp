package com.flipkart.dsp.db.entities;

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
@NoArgsConstructor
@Entity
@Builder
@DynamicUpdate
@AllArgsConstructor
@Table(name = "pipeline_step_runtime_config", uniqueConstraints = @UniqueConstraint(columnNames = {"pipeline_execution_id","pipeline_step_id"}))
public class PipelineStepRuntimeConfigEntity implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "workflow_execution_id", nullable = false)
    private String workflowExecutionId;

    @Column(name = "pipeline_execution_id", nullable = false)
    private String pipelineExecutionId;

    @ManyToOne
    @JoinColumn(name = "pipeline_step_id", nullable = false)
    private PipelineStepEntity pipelineStepEntity;

    @Column(name = "scope", nullable = false)
    @Type(type = "text")
    private String scope;

    @Column(name = "run_config")
    @Type(type = "text")
    private String runConfig;

    @Column(name = "ts")
    private Timestamp ts;
}
