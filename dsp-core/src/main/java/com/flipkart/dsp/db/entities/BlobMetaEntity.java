package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.enums.BlobStatus;
import com.flipkart.dsp.entities.enums.BlobType;
import lombok.*;
import org.hibernate.annotations.DynamicUpdate;

import javax.persistence.*;
import java.sql.Timestamp;

@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@DynamicUpdate
@Table(
        name = "blob_meta",
        uniqueConstraints = {@UniqueConstraint(columnNames = {"request_id", "type"})}
)
public class BlobMetaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "request_id", nullable = false)
    private String requestId;

    @Column(name = "location", nullable = false)
    private String location;

    @Enumerated(EnumType.STRING)
    @Column(name = "type", nullable = false)
    private BlobType type;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private BlobStatus status;

    @Column(name = "created_at", nullable = false)
    private Timestamp createdAt;

    @Column(name = "updated_at", nullable = false)
    private Timestamp updatedAt;
}
