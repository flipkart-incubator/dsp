package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.enums.ExternalHealthCheckStatus;
import com.flipkart.dsp.models.ExternalClient;
import lombok.Builder;
import lombok.Data;
import org.hibernate.annotations.DynamicUpdate;

import javax.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;

@Data
@Builder
@Entity
@DynamicUpdate
@Table(name = "external_health_check_audits")
public class ExternalHealthCheckAuditEntity implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Enumerated(EnumType.STRING)
    @Column(name = "external_client", nullable = false)
    private ExternalClient externalClient;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private ExternalHealthCheckStatus status;

    @Column(name = "created_at", nullable = false)
    private Timestamp createdAt;
}
