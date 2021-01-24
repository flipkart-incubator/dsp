package com.flipkart.dsp.entities.sg.core;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Date;

/**
 */

@MappedSuperclass
@Data
public abstract class BaseEntity implements Serializable {

    @Column(name = "created_at", updatable = false)
    @CreationTimestamp
    protected Timestamp createdAt;

    @Column(name = "updated_at")
    @UpdateTimestamp
    protected Timestamp updatedAt;

}
