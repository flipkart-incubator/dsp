package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideState;
import com.flipkart.dsp.entities.sg.core.DataFrameOverrideType;
import lombok.*;
import org.hibernate.annotations.DynamicUpdate;

import javax.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Date;

@NoArgsConstructor
@Data
@Builder
@DynamicUpdate
@AllArgsConstructor
@Entity(name = "dataframe_override_audits")
public class DataFrameOverrideAuditEntity extends BaseEntity  implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "request_id")
    private Long requestId;

    @Column(name = "workflow_id")
    private Long workflowId;

    @Column(name = "dataframe_id")
    private Long dataframeId;

    @Column(name = "is_deleted")
    private Boolean isDeleted;

    @Column(name = "input_data_id")
    private String inputDataId;

    @Column(name = "input_metadata")
    private String inputMetadata;

    @Setter
    @Column(name = "output_metadata")
    private String outputMetadata;

    @Setter
    @Column(name = "state", nullable = false)
    @Enumerated(EnumType.STRING)
    private DataFrameOverrideState state;

    @Setter
    @Column(name = "type", nullable = false)
    @Enumerated(EnumType.STRING)
    private DataFrameOverrideType dataFrameOverrideType;

    /*TODO: better way to do this, setting a value for expiry of tables/partitons not owned by DSP like dcp_fact */
    @Column(name = "expires_at")
    private LocalDateTime expiresAt = LocalDateTime.now().plusDays(30);

    @Column(name = "purge_policy_id")
    private Long purgePolicyId;
}
