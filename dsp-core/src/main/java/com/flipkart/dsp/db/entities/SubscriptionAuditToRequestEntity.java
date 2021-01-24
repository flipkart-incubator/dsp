package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.DynamicUpdate;

import javax.persistence.*;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Entity
@DynamicUpdate
@Table(name = "subscription_audit_to_requests")
public class SubscriptionAuditToRequestEntity extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @ManyToOne
    @JoinColumn(name = "subscription_audit_id")
    private SubscriptionAuditEntity subscriptionAuditEntity;

    @ManyToOne
    @JoinColumn(name = "request_id")
    private RequestEntity requestEntity;
}
