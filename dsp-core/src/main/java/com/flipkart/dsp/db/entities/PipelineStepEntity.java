package com.flipkart.dsp.db.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;
import org.hibernate.annotations.*;

import javax.persistence.*;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.io.Serializable;
import java.util.List;

/**
 */
@Data
@DynamicUpdate
@NoArgsConstructor
@Entity
@Table(name = "pipeline_step")
@PrimaryKeyJoinColumn(name = "id")
public class PipelineStepEntity implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "name")
    private String name;

    @Column(name = "workflow_id")
    private Long workflowId;

    @NotFound(action = NotFoundAction.IGNORE)
    @JsonIgnore
    @ManyToOne
    @JoinColumn(name = "parent_pipeline_step_id")
    private PipelineStepEntity parentPipelineStepEntity;

    @ManyToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @Fetch(FetchMode.JOIN)
    @JoinColumn(name = "script_id")
    private ScriptEntity scriptEntity;

    @Column(name = "pipeline_step_resources")
    private String pipelineStepResources;

    @OneToMany(mappedBy = "pipelineStepEntity", cascade = CascadeType.ALL)
    private List<PipelineStepPartitionEntity> pipelineStepPartitionEntities;
}
