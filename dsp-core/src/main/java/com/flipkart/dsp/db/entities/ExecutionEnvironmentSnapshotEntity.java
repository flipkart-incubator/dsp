package com.flipkart.dsp.db.entities;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.DynamicUpdate;

import javax.persistence.*;
import java.io.Serializable;
import java.sql.Timestamp;

/**
 */
@Data
@DynamicUpdate
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Builder
@Table(name = "execution_environment_snapshots", uniqueConstraints = @UniqueConstraint(columnNames = {"execution_environment_id", "version"}))
public class ExecutionEnvironmentSnapshotEntity implements Serializable {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "execution_environment_id")
    private Long executionEnvironmentId;

    @Column(name = "library_set")
    private String librarySet;

    @Column(name = "version", nullable = false)
    private long version;

    @Column(name = "image_latest_digest", nullable = false)
    private String imageLatestDigest;

    @Column(name = "created_at", nullable = false)
    private Timestamp createdAt;

    @Column(name = "native_library_set")
    private String nativeLibrarySet;

    @Column(name = "os")
    private String os;

    @Column(name = "os_version")
    private String osVersion;

    @Column(name = "language_version")
    private String languageVersion;

}
