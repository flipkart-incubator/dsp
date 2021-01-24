package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.models.misc.EmailNotifications;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import java.io.Serializable;

@Data
@Entity
@Builder
@DynamicUpdate
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "notification_preferences")
public class NotificationPreferencesEntity implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "email_notifications", nullable = false)
    @Type(type = "com.flipkart.dsp.utils.JsonAbstractDataType",
            parameters = {@org.hibernate.annotations.Parameter(name = "classType",
                    value = "com.flipkart.dsp.models.misc.EmailNotifications")})
    private EmailNotifications emailNotificationPreferences;
    
    @ManyToOne(fetch = FetchType.LAZY)
    @Fetch(FetchMode.JOIN)
    @JoinColumn(name = "workflow_id", referencedColumnName = "id", nullable = false)
    private WorkflowEntity workflowEntity;
}
