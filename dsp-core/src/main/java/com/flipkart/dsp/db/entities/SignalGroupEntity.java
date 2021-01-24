package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.DynamicUpdate;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

import javax.persistence.*;
import java.util.List;

/**
 */

@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@DynamicUpdate
@Table(name = "signal_groups")
public class SignalGroupEntity extends BaseEntity {
    @Id
    @Column(length = 50)
    private String id;

    @Column
    private String description;

    @OneToMany(mappedBy = "signalGroup", fetch = FetchType.LAZY)
    @Fetch(FetchMode.JOIN)
    private List<SignalGroupToSignalEntity> signalGroupToSignalEntities;
}

