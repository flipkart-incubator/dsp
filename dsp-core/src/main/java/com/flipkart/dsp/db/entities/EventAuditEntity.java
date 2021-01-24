package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import com.flipkart.dsp.models.EventLevel;
import com.flipkart.dsp.models.EventType;
import com.flipkart.dsp.models.event_audits.Events;
import lombok.*;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.io.Serializable;

@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@DynamicUpdate
@Table(name = "event_audits", indexes = {@Index(name = "request_id_index", columnList = "request_id")})
public class EventAuditEntity extends BaseEntity implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "request_id", nullable = false)
    private Long requestId;

    @Column(name = "workflow_id", nullable = false)
    private Long workflowId;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_level")
    private EventLevel eventLevel;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type")
    private EventType eventType;

    @Column(name = "data", columnDefinition = "TEXT")
    @Type(type = "com.flipkart.dsp.utils.JsonAbstractDataType",
            parameters = {@org.hibernate.annotations.Parameter(name = "classType",
                    value = "com.flipkart.dsp.models.event_audits.Events")})
    private Events payload;

}
