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

@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@DynamicUpdate
@Table(name = "signal_groups_to_signals", uniqueConstraints = @UniqueConstraint(columnNames = {"signal_id", "signal_group_id", "data_table_id"}))
public class SignalGroupToSignalEntity extends BaseEntity {

    @Id
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "signal_group_id", referencedColumnName = "id")
    private SignalGroupEntity signalGroup;

    @Id
    @ManyToOne
    @JoinColumn(name = "signal_id", referencedColumnName = "id")
    private SignalEntity signal;

    @Column(name = "is_primary")
    private boolean primary;

    @ManyToOne(cascade = {CascadeType.ALL})
    @JoinColumn(name = "data_table_id", referencedColumnName = "id")
    private DataTableEntity dataTableEntity;

}
