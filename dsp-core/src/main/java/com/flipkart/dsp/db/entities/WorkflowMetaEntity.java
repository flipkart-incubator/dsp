package com.flipkart.dsp.db.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.Type;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.List;

/**
 */
@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@DynamicUpdate
@Table(name = "workflow_meta")
public class WorkflowMetaEntity implements Serializable {
    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name="callback_entities")
    @Type(type = "text")
    private String callbackEntities;

    @Column(name="callback_url")
    @Type(type = "text")
    private String callbackUrl;

    @Column(name="warning_time_for_notification")
    private Long warningTimeForNotification;

    @Column(name="kill_time_for_notification")
    private Long killTimeForNotification;

    @NotNull
    @Column(name="hive_queue")
    private String hiveQueue;

    @NotNull
    @Column(name="mesos_queue")
    private String mesosQueue;
}

