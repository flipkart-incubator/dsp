package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import com.flipkart.dsp.entities.sg.core.DataFrameAuditStatus;
import com.flipkart.dsp.entities.sg.dto.SGUseCasePayload;
import com.flipkart.dsp.models.sg.DataFrameConfig;
import lombok.*;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.io.Serializable;
import java.time.LocalDateTime;

/**
 */

@Data
@Builder
@Entity
@DynamicUpdate
@AllArgsConstructor
@NoArgsConstructor
@Table(name = "dataframe_audit")
@EqualsAndHashCode(callSuper = true)
public class DataFrameAuditEntity extends BaseEntity implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "run_id")
    private Long runId;

    @ManyToOne
    @JoinColumn(name = "dataframe_id", nullable = false)
    private DataFrameEntity dataFrameEntity;

    @Column(name = "status", nullable = false)
    @Enumerated(EnumType.STRING)
    private DataFrameAuditStatus status;

    @Type(type = "com.flipkart.dsp.utils.JsonAbstractDataType",
            parameters = {@org.hibernate.annotations.Parameter(name = "classType",
                    value = "com.flipkart.dsp.entities.sg.dto.SGUseCasePayload")})
    @Column(name = "payload")
    private SGUseCasePayload payload;

    @Type(type = "com.flipkart.dsp.utils.JsonAbstractDataType",
            parameters = {@org.hibernate.annotations.Parameter(name = "classType",
                    value = "com.flipkart.dsp.models.sg.DataFrameConfig")})
    @Column(name = "config")
    private DataFrameConfig dataFrameConfig;

    @Column(name = "dataframe_size")
    private Long dataframeSize;

    @Column(name = "override_audit_id")
    private Long overrideAuditId;

    @Column(name = "log_audit_id")
    private Long logAuditId;

    @Column(name = "partitions")
    private String partitions;
}
