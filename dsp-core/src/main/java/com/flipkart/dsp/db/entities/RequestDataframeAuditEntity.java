package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import com.sun.javafx.beans.IDProperty;
import lombok.*;
import org.hibernate.annotations.DynamicUpdate;

import javax.persistence.*;
import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
@DynamicUpdate
@Table(name = "request_to_dataframe_audits")
@IdClass(RequestDataframeAuditID.class)
public class RequestDataframeAuditEntity extends BaseEntity implements Serializable {

    @Id
    @ManyToOne
    @JoinColumn(name = "request_id", referencedColumnName = "id")
    private RequestEntity requestEntity;

    @Id
    @ManyToOne
    @JoinColumn(name = "dataframe_audit_id", referencedColumnName = "run_id")
    private DataFrameAuditEntity dataFrameAuditEntity;

    @ManyToOne
    @JoinColumn(name = "workflow_id", referencedColumnName = "id")
    private WorkflowEntity workflowEntity;

    @Id
    @ManyToOne
    @JoinColumn(name = "pipeline_step_id", referencedColumnName = "id")
    private PipelineStepEntity pipelineStepEntity;
}

@Data
class RequestDataframeAuditID implements Serializable {
    private Long requestEntity;
    private Long dataFrameAuditEntity;
    private Long pipelineStepEntity;
}
