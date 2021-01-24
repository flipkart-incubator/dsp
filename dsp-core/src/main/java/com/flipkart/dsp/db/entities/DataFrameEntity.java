package com.flipkart.dsp.db.entities;

import com.flipkart.dsp.entities.sg.core.BaseEntity;
import com.flipkart.dsp.models.sg.DataFrameConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.*;
import org.hibernate.annotations.Parameter;

import javax.persistence.*;
import javax.persistence.CascadeType;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.util.Set;
import java.util.stream.Collectors;
import java.sql.Timestamp;


@Data
@Entity
@Builder
@DynamicUpdate
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "dataframes")
public class DataFrameEntity extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(length = 50)
    private String name;

    @OneToOne(cascade = CascadeType.ALL, fetch = FetchType.LAZY)
    @Fetch(FetchMode.JOIN)
    @JoinColumn(name = "signal_group_id", referencedColumnName = "id", nullable = false)
    private SignalGroupEntity signalGroupEntity;

    @Type(type = "com.flipkart.dsp.utils.JsonAbstractDataType",
            parameters = {@Parameter(name = "classType",
                    value = "com.flipkart.dsp.models.sg.DataFrameConfig")})
    @Column(name = "config", nullable = false)
    private DataFrameConfig dataFrameConfig;

    @Transient
    public Set<String> getDataTables() {
        return signalGroupEntity.getSignalGroupToSignalEntities().stream()
                .map(s -> s.getDataTableEntity().getId())
                .collect(Collectors.toSet());
    }
}

