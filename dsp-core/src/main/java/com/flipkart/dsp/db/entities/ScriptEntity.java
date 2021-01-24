package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import lombok.*;
import org.hibernate.annotations.DynamicUpdate;

import javax.persistence.*;
import java.io.Serializable;

/**
 */
@Data
@Builder
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@Entity
@DynamicUpdate
@Table(name = "script")
@PrimaryKeyJoinColumn(name="id")
public class ScriptEntity extends BaseEntity implements Serializable {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "git_file_path", nullable = false)
    private String gitFilePath;

    @Column(name = "git_commit_id", nullable = false)
    private String gitCommitId;

    @Column(name = "git_repo", nullable = false)
    private String gitRepo;

    @Column(name = "git_folder", nullable = false)
    private String gitFolder;

    @Setter
    @Column
    private Double version = 1.0;

    @Setter
    @Column(name = "is_draft")
    private Boolean isDraft = true;

    @ManyToOne
    @JoinColumn(name = "execution_env", referencedColumnName = "execution_env")
    @Enumerated(EnumType.STRING)
    private ExecutionEnvironmentEntity executionEnvironmentEntity;

    @Column(name = "input_variables", columnDefinition = "TEXT")
    private String inputVariables;

    @Column(name = "output_variables", columnDefinition = "TEXT")
    private String outputVariables;
}
