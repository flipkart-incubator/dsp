package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.enums.RequestStepType;
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
@Builder
@DynamicUpdate
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "request_step")
public class RequestStepEntity extends BaseEntity implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Enumerated(EnumType.STRING)
    @Column(name = "step_type", nullable = false)
    private RequestStepType requestStepType;

    @ManyToOne
    @JoinColumn(name = "request_id", referencedColumnName = "id", nullable = false)
    private RequestEntity requestEntity;

    @Column(name = "job_name")
    @Type(type = "text")
    private String jobName;

}
