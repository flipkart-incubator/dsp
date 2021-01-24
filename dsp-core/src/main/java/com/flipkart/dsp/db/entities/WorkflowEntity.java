package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import jdk.nashorn.internal.ir.annotations.Reference;
import lombok.*;
import org.hibernate.annotations.*;

import javax.persistence.*;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.Set;

/**
 */
@Data
@NoArgsConstructor
@Entity
@Builder
@AllArgsConstructor
@DynamicUpdate
@PrimaryKeyJoinColumn(name="id")
@EqualsAndHashCode(callSuper = true)
@Table(name = "workflow", uniqueConstraints = @UniqueConstraint(columnNames = {"name", "version", "is_prod"}))
public class WorkflowEntity extends BaseEntity implements Serializable{
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "name", nullable = false)
    private String name;

    @Column(name = "description")
    private String description;

    @NotFound(action = NotFoundAction.IGNORE)
    @ManyToOne
    @JoinColumn(name = "parent_workflow_id")
    private WorkflowEntity parentWorkFlow;

    @ManyToMany(fetch = FetchType.LAZY)
    @Fetch(FetchMode.JOIN)
    @JoinTable(name = "workflow_to_dataframes",
            joinColumns = {@JoinColumn(name = "workflow_id")},
            inverseJoinColumns = {@JoinColumn(name = "dataframe_id")})
    private Set<DataFrameEntity> dataFrames;

    @Column(name = "retries", nullable = false, columnDefinition = "int default 3")
    private int retries;

    @Column(name = "default_overrides")
    private String defaultOverrides;

    @NonNull
    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @Fetch(FetchMode.JOIN)
    @JoinColumn(name="wf_meta_id")
    private WorkflowMetaEntity workflowMetaEntity;


    @Column(name = "subscription_id")
    private String subscriptionId;

    @Setter
    @Column
    private String version = "1.0.0";

    @Setter
    @Column(name = "is_prod")
    private boolean isProd = false;

    @Column(name = "workflow_group_name")
    private String workflowGroupName;

    @NonNull
    @ManyToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @Fetch(FetchMode.JOIN)
    @JoinColumn(name = "created_by", referencedColumnName = "user_id")
    private UserEntity createdBy;
}
