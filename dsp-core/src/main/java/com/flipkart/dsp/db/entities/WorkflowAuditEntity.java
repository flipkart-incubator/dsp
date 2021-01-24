package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import com.flipkart.dsp.models.WorkflowStatus;
import lombok.*;
import org.hibernate.annotations.DynamicUpdate;

import javax.persistence.*;
import java.io.Serializable;

/**
 */
@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@DynamicUpdate
@EqualsAndHashCode(callSuper = true)
@Table(name = "workflow_audits", uniqueConstraints = @UniqueConstraint(columnNames = {"refresh_id", "workflow_id", "workflow_execution_id"}))
public class WorkflowAuditEntity extends BaseEntity implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "refresh_id", nullable = false)
    private Long refreshId;

    @ManyToOne
    @JoinColumn(name = "workflow_id", nullable = false)
    private WorkflowEntity workflowEntity;

    @Column(name = "workflow_execution_id", nullable = false)
    private String workflowExecutionId;

    @Column(name = "status")
    @Enumerated(EnumType.STRING)
    private WorkflowStatus workflowStatus;

}
