package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.models.ImageLanguageEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

import javax.persistence.*;
import java.io.Serializable;
import java.util.List;

/**
 */
@Data
@Builder
@Entity
@DynamicUpdate
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "execution_environment", uniqueConstraints = @UniqueConstraint(columnNames = {"execution_env"}))
public class ExecutionEnvironmentEntity implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id")
    private Long id;

    @Column(name = "execution_env", nullable = false)
    private String executionEnvironment;

    @Column(name = "docker_hub")
    private String dockerHub;

    @Column(name = "image_identifier")
    private String imageIdentifier;

    @Column(name = "image_version")
    private String imageVersion;

    @Column(name = "startup_script_path", nullable = false)
    private String startUpScriptPath;

    @Column(name = "image_language", nullable = false)
    @Enumerated(EnumType.STRING)
    private ImageLanguageEnum imageLanguage;

    @OneToMany
    @JoinColumn(name = "execution_environment_id")
    private List<ExecutionEnvironmentSnapshotEntity> executionEnvironmentSnapshotEntities;
}
