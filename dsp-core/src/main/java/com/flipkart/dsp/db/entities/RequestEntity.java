package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import com.flipkart.dsp.models.RequestStatus;
import com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest;
import lombok.*;
import org.hibernate.annotations.*;

import javax.persistence.*;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 */
@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@DynamicUpdate
@Table(name = "request")
@EqualsAndHashCode(callSuper = true)
public class RequestEntity extends BaseEntity implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "request_id")
    private Long requestId;

    @Column(name = "workflow_id", nullable = false)
    private Long workflowId;

    @Column(name = "data", nullable = false)
    @Type(type = "com.flipkart.dsp.utils.JsonAbstractDataType",
            parameters = {@org.hibernate.annotations.Parameter(name = "classType",
                    value = "com.flipkart.dsp.models.workflow.ExecuteWorkflowRequest")})
    private ExecuteWorkflowRequest data;

    @Column(name = "azkaban_exec_id")
    private Long azkabanExecId;

    @Enumerated(EnumType.STRING)
    @Column(name = "request_status")
    private RequestStatus requestStatus;

    @Type(type = "text")
    @Column(name = "callback_url")
    private String callbackUrl;

    @Column(name = "workflow_details_snapshot")
    private String workflowDetailsSnapshot;

    @Column(name = "is_notified")
    private Boolean isNotified;

    @Column(name = "response")
    private String response;

    @Column(name = "varadhi_response")
    private String varadhiResponse;

    @OneToMany(mappedBy = "requestEntity", cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @Fetch(FetchMode.JOIN)
    private List<RequestDataframeAuditEntity> requestDataframeAudits = new ArrayList<>();

    @NonNull
    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @Fetch(FetchMode.JOIN)
    @JoinColumn(name = "triggered_by", referencedColumnName = "user_id")
    private UserEntity userEntity;
}
