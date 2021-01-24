package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.enums.RequestStepAuditStatus;
import com.flipkart.dsp.entities.sg.core.BaseEntity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.io.Serializable;

/**
 */
@Data
@DynamicUpdate
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "request_step_audit")
@Builder
public class RequestStepAuditEntity extends BaseEntity implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name="status")
    @Enumerated(EnumType.STRING)
    private RequestStepAuditStatus status;

    @ManyToOne
    @JoinColumn(name = "request_step_id", referencedColumnName = "id", nullable = false)
    private RequestStepEntity requestStepEntity;

    @Column(name = "meta_data")
    @Type(type = "text")
    private String metaData;

}
